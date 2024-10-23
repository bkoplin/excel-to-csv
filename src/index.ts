import type * as csv from 'csv'
import type { Info } from 'csv-parse'
import type {
  JsonPrimitive,
  Primitive,
} from 'type-fest'
import type {
  CSVOptionsWithGlobals,
  ExcelOptionsWithGlobals,
} from './types'
import { createReadStream } from 'node:fs'
import { basename } from 'node:path'
import { PassThrough } from 'node:stream'
import timers from 'node:timers/promises'
import { isTruthy } from '@antfu/utils'
import { Command } from '@commander-js/extra-typings'
import * as Prompts from '@inquirer/prompts'
import chalk from 'chalk'
import { parse as parser } from 'csv-parse'
import { stringify as stringifier } from 'csv-stringify'
import fs from 'fs-extra'
import {
  isArray,
  isEmpty,
  isNull,
  isUndefined,
} from 'lodash-es'
import ora, { oraPromise } from 'ora'
import Papa from 'papaparse'
import {
  join,
  parse,
} from 'pathe'
import { filename } from 'pathe/utils'
import {
  get,
  zipToObject,
} from 'radash'
import { transform } from 'stream-transform'
import yaml from 'yaml'
import pkg from '../package.json'
import {
  compareAndLogRanges,
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
  formatHeaderValues,
  generateCommandLineString,
  generateParsedCsvFilePath,
  selectGroupingField,

  tryPrompt,
} from './helpers'
import categoryOption from './options/categoryField'
import delimiterOption from './options/delimiter'
import fileSizeOption from './options/fileSize'
import fromLineOption from './options/fromLine'
import makeFilePathOption from './options/makeFilePath'
import filterTypeOption from './options/matchType'
import includesHeaderOption from './options/rangeIncludesHeader'
import toLineOption from './options/rowCount'
import filterValuesOption from './options/rowFilters'
import sheetNameOption from './options/sheetName'
import sheetRangeOption from './options/sheetRange'
import writeHeaderOption from './options/writeHeader'
import writeCsv from './writeCsv'

const spinner = ora({
  hideCursor: false,
  discardStdin: false,
})

export const program = new Command(pkg.name).version(pkg.version)
.description('A CLI tool to parse and split Excel Files and split CSV files, includes the ability to filter and group into smaller files based on a column value and/or file size')
.showSuggestionAfterError(true)
.configureHelp({ sortSubcommands: true })

export const _excelCommands = program.command('excel')
  .description('Parse an Excel file')
  .addOption(makeFilePathOption('Excel'))
  .addOption(fileSizeOption)
  .addOption(includesHeaderOption)
  .addOption(writeHeaderOption)
  .addOption(filterValuesOption)
  .addOption(categoryOption)
  .addOption(filterTypeOption)
  .addOption(sheetNameOption)
  .addOption(sheetRangeOption)
  .action(async (options: ExcelOptionsWithGlobals, command) => {
    options.command = 'Excel'

    const newFilePath = await checkAndResolveFilePath({
      fileType: 'Excel',
      argFilePath: options.filePath,
    })

    if (newFilePath !== options.filePath) {
      command.setOptionValueWithSource('filePath', newFilePath, 'env')
    }
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

      const d = await getWorkbook(options.filePath)

      await timers.setTimeout(1000)

      return d
    }, {
      text: `Reading ${basename(options.filePath)}`,
      successText: chalk.greenBright(`Successfully read ${basename(options.filePath)}`),
      failText: chalk.redBright(`failure reading ${basename(options.filePath)}`),

    })

    if (typeof options.sheetName !== 'string' || !wb.SheetNames.includes(options.sheetName)) {
      options.sheetName = await setSheetName(wb)
      command.setOptionValueWithSource('sheet', options.sheetName, 'env')
    }

    const parsedOutputFile = generateParsedCsvFilePath({
      parsedInputFile: parse(options.filePath),
      filters: options.rowFilters,
      sheetName: options.sheetName,
    })

    const ws = wb.Sheets[options.sheetName!]

    parsedOutputFile.name = `${parsedOutputFile.name} ${options.sheetName}`
    if (typeof ws === 'undefined') {
      spinner.fail(`The worksheet "${options.sheetName}" does not exist in the Excel file ${filename(options.filePath)}`)
      process.exit(1)
    }
    if (!isOverlappingRange(ws, options.range)) {
      const selectedRange = await setRange(wb, options.sheetName)

      command.setOptionValueWithSource('range', selectedRange, 'env')
      options.range = selectedRange

      const {

        parsedWorksheetRange,

        parsedRange,
        worksheetRange,
      } = extractRangeInfo(ws, options.range)

      compareAndLogRanges(parsedRange, parsedWorksheetRange, options.range, worksheetRange)
    }
    if (isUndefined(options.rangeIncludesHeader)) {
      options.rangeIncludesHeader = await setRangeIncludesHeader(options.range, options.rangeIncludesHeader)
      command.setOptionValueWithSource('rangeIncludesHeader', options.rangeIncludesHeader, 'env')
    }
    if (options.rangeIncludesHeader === false && options.writeHeader === true) {
      options.writeHeader = false
      command.setOptionValueWithSource('writeHeader', false, 'env')
    }
    // await updateCommandOptions(command, globalOptions)

    const { parsedRange } = extractRangeInfo(ws, options.range)

    const [fields, ...data] = extractDataFromWorksheet(parsedRange, ws)

    const groupingOptions = [...fields as string[], new Prompts.Separator()]

    const hasNullUndefinedField = fields.some(f => typeof f === 'undefined' || f === null)

    if (options.rangeIncludesHeader === true && !options.categoryField && !hasNullUndefinedField) {
      const newCategory = await selectGroupingField(groupingOptions, command)

      if (isTruthy(newCategory)) {
        options.categoryField = newCategory
        command.setOptionValueWithSource('categoryField', newCategory, 'env')
      }
    }
    else if (hasNullUndefinedField) {
      spinner.warn(chalk.yellowBright(`The selected range does not seem to include a header row; you will need to restart if you want to group or filter columns`))
      command.setOptionValueWithSource('categoryField', undefined, 'env')
      command.setOptionValueWithSource('writeHeader', false, 'env')
      command.setOptionValueWithSource('rangeIncludesHeader', false, 'env')
      command.setOptionValueWithSource('rowFilters', {}, 'env')
      options = {
        ...options,
        categoryField: undefined,
        writeHeader: false,
        rangeIncludesHeader: false,
        rowFilters: {},
      }
      await timers.setTimeout(2500)
    }

    const csv = Papa.unparse([fields, ...data], { delimiter: '|' })

    const commandLineString = generateCommandLineString(options, command)

    const transformStream = makeTransformStream(data.map(values => zipToObject(fields, values)), options)

    transformStream.on('data', (data) => {
      console.log(data)
    })
    fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), yaml.stringify({
      parsedCommandOptions: options,
      commandLineString,
    }, { lineWidth: 1000 }))
    parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
    fs.ensureDirSync(parsedOutputFile.dir)
    // writeCsv(ReadStream.from(csv), {
    //   ...options,
    //   parsedOutputFile,
    //   bytesRead,
    // })
  })

export const _csvCommands = program.command('csv')
  .description('Parse a CSV file')
  .addOption(makeFilePathOption('CSV'))
  .addOption(fromLineOption)
  .addOption(toLineOption)
  .addOption(fileSizeOption)
  .addOption(includesHeaderOption)
  .addOption(writeHeaderOption)
  .addOption(filterValuesOption)
  .addOption(categoryOption)
  .addOption(delimiterOption)
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
    if (globalOptions.rangeIncludesHeader === false && globalOptions.writeHeader === true) {
      globalOptions.writeHeader = false
      command.parent?.setOptionValueWithSource('writeHeader', false, 'env')
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

    let recordCount = 0

    let skippedLines = 1 - globalOptions.fromLine

    let header: Exclude<csv.parser.ColumnOption, Primitive>[] = []

    let tryToSetCategory = true

    let isWriting = false

    let bytesRead = 0

    let inputStreamReader: csv.stringifier.Stringifier
    // const inputStreamReader = stringifier({
    //   bom: true,
    //   columns: globalOptions.rangeIncludesHeader && header.length > 0 ? header : undefined,
    //   header: globalOptions.rangeIncludesHeader ? header.length > 0 : undefined,
    // })

    const sourceStream = createReadStream(globalOptions.filePath, 'utf-8')

    const outputStream = new PassThrough({ encoding: 'utf-8' })

    const lineReader = parser({
      bom: true,
      from_line: globalOptions.fromLine,
      to_line: isNull(globalOptions.toLine) ? undefined : globalOptions.toLine,
      trim: true,
      delimiter: globalOptions.delimiter,
      columns: (record: string[]) => {
        if (globalOptions.rangeIncludesHeader !== true)
          return false

        return formatHeaderValues({ data: record })

        // return header
      },
      info: true,
      skip_records_with_error: true,
      on_record: ({
        info,
        record,
      }: {
        info: Info
        record: Record<string, string> | Array<string>
      }) => {
        bytesRead = info.bytes
        recordCount = info.records
        skippedLines = info.lines - info.records
        if (header.length === 0 && isArray(info.columns)) {
          header = info.columns as Exclude<csv.parser.ColumnOption, Primitive>[]
        }

        return {
          info,
          record,
        }
      },
    })

    // .pipe(outputStream)
    // .pipe(outputStream)
    lineReader.on('data', async ({
      // info,
      record,
    }: {
      info: Info
      record: Record<string, string> | Array<string>
    }) => {
      if (globalOptions.rangeIncludesHeader === true && !globalOptions.categoryField && tryToSetCategory) {
        lineReader.pause()
        await selectGroupingField(Object.keys(record), command)
        globalOptions.categoryField = command.parent!.getOptionValue('categoryField') as string
        tryToSetCategory = false
        lineReader.resume()
      }
      if (typeof inputStreamReader === 'undefined') {
        if (header.length > 0 && globalOptions.rangeIncludesHeader) {
          inputStreamReader = stringifier({
            bom: true,
            columns: header.map(({ name }) => ({
              name,
              key: name,
            })),
            header: globalOptions.header,
          })
        }
        else {
          inputStreamReader = stringifier({ bom: true })
        }
        lineReader.pipe(inputStreamReader).pipe(outputStream)
      }
      else if (isWriting === false) {
        writeCsv(outputStream, globalOptions, {
          parsedOutputFile,
          skippedLines,
          bytesRead,
          spinner,
          files: [],
          fields: (header ?? []).map(h => h.name),
          parsedLines: skippedLines + recordCount,
        })
        isWriting = true
      }
      // inputStreamReader.write(line)
      // else {
      //   inputStreamReader.write(line)
      // }
    })
    sourceStream.pipe(lineReader)
  })

program.parse(process.argv)
function makeTransformStream<T>(data: T[], options) {
  return transform<T>(data, (record) => {
    const filterCriteria = options.rowFilters

    if (isArray(record)) {
      return record
    }
    else if (isEmpty(filterCriteria)) {
      return record
    }
    else {
      const testResults: boolean[] = []

      for (const filterKey in filterCriteria) {
        const filterVal = get(filterCriteria, filterKey, []) as JsonPrimitive[]

        const filterTest = filterVal.includes(get(record, filterKey, false))

        testResults.push(filterTest)
      }
      if (options.matchType === 'all' && testResults.every(v => v === true)) {
        return record
      }
      else if (options.matchType === 'any' && testResults.includes(true)) {
        return record
      }
      else if (options.matchType === 'none' && testResults.every(v => v === false)) {
        return record
      }
      else {
        return null
      }
    }
  }, { parallel: 1 })
}
async function updateCommandOptions(command, globalOptions) {
  for (const commandOption of command.options) {
    const attributeName = commandOption.attributeName() as keyof typeof globalOptions

    const val = command.getOptionValue(attributeName)

    const source = command.getOptionValueSource(attributeName)

    if (typeof source !== 'undefined' && source !== 'env') {
      const optionMessage = `Should ${chalk.yellowBright(commandOption.long)} be set to ${chalk.cyanBright(val)}?\n(${commandOption.description})`

      const [, setValueAnswer] = await tryPrompt('confirm', {
        message: optionMessage,
        default: true,

      })

      if (setValueAnswer === false) {
        if (commandOption.argChoices) {
          const [, optionValue] = await tryPrompt('select', {
            message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})`,
            choices: commandOption.argChoices,
            default: val,
          })

          // globalOptions[attributeName] = optionValue
        }
        else if (typeof val === 'boolean') {
          const [, optionValue] = await tryPrompt('select', {
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
