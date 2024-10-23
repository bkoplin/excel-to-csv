import type {
  ConditionalPick,
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
import timers from 'node:timers/promises'
import { Command } from '@commander-js/extra-typings'
import * as Prompts from '@inquirer/prompts'
import chalk from 'chalk'
import fs from 'fs-extra'
import { isUndefined } from 'lodash-es'
import ora, { oraPromise } from 'ora'
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
import categoryOption from './options/categoryField'
import delimiterOption from './options/delimiter'
import maxFileSizeOption from './options/fileSize'
import fromLineOption from './options/fromLine'
import includesHeaderOption from './options/includesHeader'
import makeFilePathOption from './options/makeFilePath'
import filterTypeOption from './options/matchType'
import filterValuesOption from './options/rowFilter'
import sheetNameOption from './options/sheetName'
import sheetRangeOption from './options/sheetRange'
import toLineOption from './options/toLineOption'
import writeHeaderOption from './options/writeHeader'
import writeCsv from './writeCsv'

type PromptsType = ConditionalPick<typeof Prompts, (...args: any[]) => any>

type PromptKeys = StringKeyOf<PromptsType>

const spinner = ora({
  hideCursor: false,
  discardStdin: false,
})

async function tryPrompt<Value>(type: PromptKeys, timeout = 5000) {
  return tryit(opts => Prompts[type]<Value>(opts, { signal: AbortSignal.timeout(timeout) }))
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

    // ora({
    //   hideCursor: false,
    //   discardStdin: false,
    //   text: `Reading ${filename(globalOptions.filePath)}`,
    // }).start()

    const {
      wb,
      bytesRead,
    } = await oraPromise(async (_spinner) => {
      // spinner.text = `Reading ${filename(globalOptions.filePath)}`

      const d = await getWorkbook(globalOptions.filePath)

      await timers.setTimeout(1000)

      return d
    }, {
      text: `Reading ${basename(globalOptions.filePath)}`,
      successText: chalk.greenBright(`Successfully read ${basename(globalOptions.filePath)}`),
      failText: chalk.redBright(`failure reading ${basename(globalOptions.filePath)}`),

    })

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
      const selectedRange = await setRange(wb, globalOptions.sheet)

      command.setOptionValueWithSource('range', selectedRange, 'env')
      globalOptions.range = selectedRange
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

    const [fields, ...data] = extractDataFromWorksheet(parsedRange, ws)

    const groupingOptions = [...fields as string[], new Prompts.Separator()]

    const hasNullUndefinedField = fields.some(f => typeof f === 'undefined' || f === null)

    if (globalOptions.rangeIncludesHeader === true && !globalOptions.categoryField && !hasNullUndefinedField) {
      await selectGroupingField(groupingOptions, command)
      globalOptions.categoryField = command.parent!.getOptionValue('categoryField') as string
    }
    else if (hasNullUndefinedField) {
      spinner.warn(chalk.yellowBright(`The selected range does not seem to include a header row; you will need to restart if you want to group or filter columns`))
      command.parent!.setOptionValueWithSource('categoryField', undefined, 'env')
      command.parent!.setOptionValueWithSource('header', undefined, 'env')
      command.parent!.setOptionValueWithSource('rangeIncludesHeader', undefined, 'env')
      command.setOptionValueWithSource('rowFilters', undefined, 'env')
      await timers.setTimeout(2500)
    }

    const csv = Papa.unparse([fields, ...data], { delimiter: '|' })

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
async function selectGroupingField(groupingOptions: (string | Prompts.Separator)[], command: Command): Promise<void> {
  const [confirmErr, confirmCategory] = await tryit(Prompts.confirm)({
    message: 'Would you like to select a field to split the file into separate files?',
    default: false,
  }, { signal: AbortSignal.timeout(7500) })

  if (confirmCategory === true) {
    const [categoryErr, selectedCategory] = await tryit(Prompts.select<string>)({
      message: `Select a column to group rows from input file by...`,
      choices: groupingOptions,
      loop: true,
      // pageSize: groupingOptions.length > 15 ? 15 : groupingOptions.length,
    })

    if (selectedCategory) {
      // globalOptions.categoryField = selectedCategory
      command.parent!.setOptionValueWithSource('categoryField', selectedCategory, 'env')
    }
  }
}
async function updateCommandOptions(command, globalOptions) {
  for (const commandOption of command.options) {
    const attributeName = commandOption.attributeName() as keyof typeof globalOptions

    const val = command.getOptionValue(attributeName)

    const source = command.getOptionValueSource(attributeName)

    if (typeof source !== 'undefined' && source !== 'env') {
      const optionMessage = `Should ${chalk.yellowBright(commandOption.long)} be set to ${chalk.cyanBright(val)}?\n(${commandOption.description})`

      const [, setValueAnswer] = await tryPrompt('confirm')({
        message: optionMessage,
        default: true,
      })

      if (setValueAnswer === false) {
        if (commandOption.argChoices) {
          const [, optionValue] = await tryPrompt('select')({
            message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})`,
            choices: commandOption.argChoices,
            default: val,
          })

          // globalOptions[attributeName] = optionValue
        }
        else if (typeof val === 'boolean') {
          const [, optionValue] = await tryPrompt('select')({
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
          const [, optionValue] = await tryPrompt('input')({ message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})` })

          // globalOptions[attributeName] = optionValue
        }
      }
    }
  }
}
