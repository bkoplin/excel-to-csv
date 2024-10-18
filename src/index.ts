import type {
  ConditionalPick,
  Primitive,
  Simplify,
  StringKeyOf,
} from 'type-fest'
import {
  createReadStream,
  ReadStream,
} from 'node:fs'
import {
  basename,
  type ParsedPath,
} from 'node:path'
import { createInterface } from 'node:readline'
import { PassThrough } from 'node:stream'
import { Command } from '@commander-js/extra-typings'
import * as Prompts from '@inquirer/prompts'
import chalk from 'chalk'
import fs from 'fs-extra'
import { isUndefined } from 'lodash-es'
import ora from 'ora'
import Papa from 'papaparse'
import {
  join,
  parse,
} from 'pathe'
import { filename } from 'pathe/utils'
import { tryit } from 'radash'
import yaml from 'yaml'
import pkg from '../package.json'
import {
  categoryOption,
  filterTypeOption,
  filterValuesOption,
  includesHeaderOption,
  makeFilePathOption,
  maxFileSizeOption,
  rowCountOption,
  sheetNameOption,
  sheetRangeOption,
  skipLinesOption,
  writeHeaderOption,
} from './arguments'
import {
  extractDataFromWorksheet,
  extractRangeInfo,
  getWorkbook,
  isOverlappingRange,
  setRange,
  setRangeIncludesHeader,
  setSheetName,
} from './excel'
import {
  checkAndResolveFilePath,
  generateCommandLineString,
  generateParsedCsvFilePath,
} from './helpers'
import writeCsv from './writeCsv'

type PromptsType = ConditionalPick<typeof Prompts, (...args: any[]) => any>

type PromptKeys = StringKeyOf<PromptsType>

const spinner = ora({
  hideCursor: false,
  discardStdin: false,
})

function tryPrompts<Value, TypeName extends PromptKeys>(type: TypeName) {
  return tryit((opts: Parameters<PromptsType[TypeName]>[0]) => Prompts[type]<Value>(opts, { signal: AbortSignal.timeout(5000) }))
}

const program = new Command(pkg.name).version(pkg.version)
.description('A CLI tool to parse and split Excel Files and split CSV files, includes the ability to filter and group into smaller files based on a column value and/or file size')
.showSuggestionAfterError(true)
.configureHelp({
  sortOptions: true,
  sortSubcommands: true,
  showGlobalOptions: true,
})
.addOption(filterValuesOption)
.addOption(categoryOption)
.addOption(filterTypeOption)
.addOption(maxFileSizeOption)
.addOption(writeHeaderOption)

const _excelCommands = program.command('excel')
  .description('Parse an Excel file')
  .addOption(makeFilePathOption('Excel'))
  .addOption(sheetNameOption)
  .addOption(sheetRangeOption)
  .addOption(includesHeaderOption)
  .action(async (options, command) => {
    const globalOptions = command.optsWithGlobals < ExcelOptionsWithGlobals>()

    globalOptions.command = 'Excel'
    globalOptions.filePath = await checkAndResolveFilePath({
      fileType: 'Excel',
      argFilePath: globalOptions.filePath,
    })

    if (command.getOptionValue('filePath') !== globalOptions.filePath)
      command.setOptionValueWithSource('filePath', globalOptions.filePath, 'env')

    spinner.start(`Reading ${filename(globalOptions.filePath)}`)

    const {
      wb,
      bytesRead,
    } = await getWorkbook(globalOptions.filePath)

    spinner.succeed(chalk.greenBright(`Read ${filename(globalOptions.filePath)}`))
    if (isUndefined(globalOptions.sheet) || typeof globalOptions.sheet !== 'string' || !wb.SheetNames.includes(globalOptions.sheet)) {
      globalOptions.sheet = await setSheetName(wb)
      command.setOptionValueWithSource('sheet', globalOptions.sheet, 'env')
    }

    const parsedOutputFile = generateParsedCsvFilePath({
      parsedInputFile: parse(globalOptions.filePath),
      filters: globalOptions.rowFilters,
      sheetName: globalOptions.sheet,
    })

    const ws = wb.Sheets[globalOptions.sheet!]

    parsedOutputFile.name = `${parsedOutputFile.name} ${globalOptions.sheet}`
    if (typeof ws === 'undefined') {
      spinner.fail(`The worksheet "${globalOptions.sheet}" does not exist in the Excel file ${filename(globalOptions.filePath)}`)
      process.exit(1)
    }
    if (!isOverlappingRange(ws, globalOptions.range)) {
      globalOptions.range = await setRange(wb, globalOptions.sheet)
      command.setOptionValueWithSource('range', globalOptions.range, 'env')
      isOverlappingRange(ws, globalOptions.range)
    }
    if (isUndefined(globalOptions.rangeIncludesHeader)) {
      globalOptions.rangeIncludesHeader = await setRangeIncludesHeader(globalOptions.range, globalOptions.rangeIncludesHeader)
      command.setOptionValueWithSource('rangeIncludesHeader', globalOptions.rangeIncludesHeader, 'env')
    }
    if (globalOptions.rangeIncludesHeader === false && globalOptions.header === true) {
      globalOptions.header = false
      command.parent?.setOptionValueWithSource('header', false, 'env')
    }
    // await updateCommandOptions(command, globalOptions)

    const { parsedRange } = extractRangeInfo(ws, globalOptions.range)

    const data: (Primitive | Date)[][] = extractDataFromWorksheet(parsedRange, ws)

    if (globalOptions.rangeIncludesHeader === true && !globalOptions.categoryField) {
      try {
        const confirmCategory = await Prompts.confirm({
          message: 'Would you like to select a field to split the file into separate files?',
          default: false,
        }, { signal: AbortSignal.timeout(5000) })

        if (confirmCategory === true) {
          const selectedCategory = await Prompts.select<string>({
            message: 'Select a column to group by…',
            choices: [...data[0], new Prompts.Separator()],
            loop: true,
            pageSize: data[0].length > 15 ? 15 : data[0].length,
          })

          if (selectedCategory) {
            globalOptions.categoryField = selectedCategory
            command.setOptionValueWithSource('categoryField', selectedCategory, 'env')
          }
        }
      }
      catch (_e) {
      }
    }

    const csv = Papa.unparse(data, { delimiter: '|' })

    const commandLineString = generateCommandLineString(globalOptions, command)

    fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), yaml.stringify({
      parsedCommandOptions: globalOptions,
      commandLineString,
    }, { lineWidth: 1000 }))
    parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
    fs.ensureDirSync(parsedOutputFile.dir)
    writeCsv(ReadStream.from(csv), {
      ...globalOptions,
      parsedOutputFile,
      bytesRead,
    })
  })

const _csvCommands = program.command('csv')
  .description('Parse a CSV file')
  .addOption(makeFilePathOption('CSV'))
  .addOption(skipLinesOption)
  .addOption(rowCountOption)
  .addOption(includesHeaderOption)
  .action(async (options, command) => {
    const globalOptions = command.optsWithGlobals<CSVOptionsWithGlobals>()

    globalOptions.command = 'CSV'
    globalOptions.filePath = await checkAndResolveFilePath({
      fileType: 'CSV',
      argFilePath: globalOptions.filePath,
    })
    command.setOptionValueWithSource('filePath', globalOptions.filePath, 'env')
    if (isUndefined(globalOptions.rangeIncludesHeader)) {
      globalOptions.rangeIncludesHeader = await Prompts.confirm({
        message: `Does ${basename(globalOptions.filePath)} include a header row?`,
        default: true,
      })
      command.setOptionValueWithSource('rangeIncludesHeader', globalOptions.rangeIncludesHeader, 'env')
    }
    if (globalOptions.rangeIncludesHeader === false && globalOptions.header === true) {
      globalOptions.header = false
      command.parent?.setOptionValueWithSource('header', false, 'env')
    }

    const parsedOutputFile = generateParsedCsvFilePath({
      parsedInputFile: parse(globalOptions.filePath),
      filters: globalOptions.rowFilters,
    })

    fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), yaml.stringify({
      parsedCommandOptions: globalOptions,
      commandLineString: generateCommandLineString(globalOptions, command),
    }, { lineWidth: 1000 }))
    parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
    fs.ensureDirSync(parsedOutputFile.dir)

    const sourceStream = createReadStream(globalOptions.filePath, 'utf-8')

    const inputStreamReader = new PassThrough({ encoding: 'utf-8' })

    const lineReader = createInterface({ input: sourceStream })

    let skippedLines = 0

    let bytesRead = 0

    lineReader.on('close', () => {
      writeCsv(inputStreamReader, {
        ...globalOptions,
        parsedOutputFile,
        skippedLines: skippedLines - 1,

        bytesRead,
      })
    })
    lineReader.on('line', (line) => {
      if ('skipLines' in globalOptions && (globalOptions.skipLines || -1) > 0 && skippedLines < (globalOptions.skipLines || -1)) {
        skippedLines++
      }
      else {
        const formattedLine = `${line}\n`

        bytesRead += Buffer.from(formattedLine).length
        inputStreamReader.write(formattedLine)
      }
    })
  })

type CsvCommand = typeof _csvCommands

type ExcelCommand = typeof _excelCommands

type ProgramCommand = typeof program

export type CSVOptions = ReturnType<CsvCommand['opts']>

export type ExcelOptions = ReturnType<ExcelCommand['opts']>

export type ProgramCommandOptions = ReturnType<ProgramCommand['opts']>

export type CSVOptionsWithGlobals = Simplify<CSVOptions & ProgramCommandOptions & {
  skippedLines: number
  rowCount: number
  parsedOutputFile: Omit<ParsedPath, 'base'>
  bytesRead: number
  command: `CSV`
}>

export type ExcelOptionsWithGlobals = Simplify<ExcelOptions & ProgramCommandOptions & {
  parsedOutputFile: Omit<ParsedPath, 'base'>
  bytesRead: number
  command: `Excel`
}>

export type CombinedProgramOptions = Simplify<CSVOptions & ExcelOptions & ProgramCommandOptions>

program.parse(process.argv)
async function updateCommandOptions(command, globalOptions) {
  for (const commandOption of command.options) {
    const attributeName = commandOption.attributeName() as keyof typeof globalOptions

    const val = command.getOptionValue(attributeName)

    const source = command.getOptionValueSource(attributeName)

    if (typeof source !== 'undefined' && source !== 'env') {
      const optionMessage = `Should ${chalk.yellowBright(commandOption.long)} be set to ${chalk.cyanBright(val)}?\n(${commandOption.description})`

      const [, setValueAnswer] = await tryPrompts('confirm')({
        message: optionMessage,
        default: true,
      })

      if (setValueAnswer === false) {
        if (commandOption.argChoices) {
          const [, optionValue] = await tryPrompts('select')({
            message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})`,
            choices: commandOption.argChoices,
            default: val,
          })

          // globalOptions[attributeName] = optionValue
        }
        else if (typeof val === 'boolean') {
          const [, optionValue] = await tryPrompts('select')({
            message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})`,
            default: val,
            choices: [{
              name: 'true',
              value: true,
            }, {
              name: 'false',
              value: false,
            }],
          })

          // globalOptions[attributeName] = optionValue
        }
        else {
          const [, optionValue] = await tryPrompts('input')({ message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})` })

          // globalOptions[attributeName] = optionValue
        }
      }
    }
  }
}
