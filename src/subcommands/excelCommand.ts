import type { JsonPrimitive } from 'type-fest'
import { basename } from 'node:path'
import timers from 'node:timers/promises'
import {
  Command,
  Option,
} from '@commander-js/extra-typings'
import * as Prompts from '@inquirer/prompts'
import chalk from 'chalk'
import { stringify } from 'csv'
import fs from 'fs-extra'
import {
  isNil,
  isUndefined,
} from 'lodash-es'
import ora, { oraPromise } from 'ora'
import {
  join,
  parse,
} from 'pathe'
import { filename } from 'pathe/utils'
import { zipToObject } from 'radash'
import { transform } from 'stream-transform'
import {
  compareAndLogRanges,
  extractDataFromWorksheet,
  extractRangeInfo,
  getWorkbook,
  isOverlappingRange,
  setRange,
  setRangeIncludesHeader,
  setSheetName,
} from '../excel'
import {
  applyFilters,
  checkAndResolveFilePath,
  generateCommandLineString,
  generateParsedCsvFilePath,
  stringifyCommandOptions,
  tryPrompt,
} from '../helpers'
import categoryOption from '../options/categoryField'
import fileSizeOption from '../options/fileSize'
import makeFilePathOption from '../options/makeFilePath'
import filterTypeOption from '../options/matchType'
import includesHeaderOption from '../options/rangeIncludesHeader'
import filterValuesOption from '../options/rowFilters'
import sheetNameOption from '../options/sheetName'
import sheetRangeOption from '../options/sheetRange'
import writeHeaderOption from '../options/writeHeader'

const spinner = ora({
  hideCursor: false,
  discardStdin: false,
})

export const excelCommamd = new Command('excel')
  .description('A CLI tool to parse, filter and split Excel files and write the results to new CSV files of a specified size')
  .addOption(makeFilePathOption('Excel'))
  .addOption(fileSizeOption)
  .addOption(includesHeaderOption)
  .addOption(writeHeaderOption)
  .addOption(filterValuesOption)
  .addOption(categoryOption)
  .addOption(filterTypeOption)
  .addOption(sheetNameOption)
  .addOption(sheetRangeOption)
  .addOption(new Option('--bytes-read [number]', 'the number of bytes read from the input file').default(0 as const)
    .hideHelp(true))
  .addOption(new Option<'--command [string]', `Excel`, `Excel`, `Excel`>('--command [string]').default(`Excel` as const)
    .preset(`Excel` as const)
    .hideHelp(true))
  .action(excelCommandAction)

export async function excelCommandAction(this: typeof excelCommamd) {
  const options = this.opts()

  const dataStream = transform(data => data)

  // dataStream.on('readable', () => {
  //   const d = dataStream.read()

  //   console.log(d)
  // })

  const newFilePath = await checkAndResolveFilePath({
    fileType: 'Excel',
    argFilePath: options.filePath,
  })

  if (newFilePath !== options.filePath) {
    this.setOptionValueWithSource('filePath', newFilePath, 'env')
  }

  const {
    wb,
    bytesRead,
  } = await oraPromise(async (_spinner) => {
    const d = await getWorkbook(options.filePath)

    await timers.setTimeout(1000)

    return d
  }, {
    text: `Reading ${basename(options.filePath)}`,
    successText: chalk.greenBright(`Successfully read ${basename(options.filePath)}`),
    failText: chalk.redBright(`failure reading ${basename(options.filePath)}`),
  })

  this.setOptionValueWithSource('bytesRead', bytesRead, 'default')
  if (typeof options.sheetName !== 'string' || !wb.SheetNames.includes(options.sheetName)) {
    options.sheetName = await setSheetName(wb)
    this.setOptionValueWithSource('sheet', options.sheetName, 'env')
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
  if (!isOverlappingRange(ws, options.sheetRange)) {
    const selectedRange = await setRange(wb, options.sheetName)

    this.setOptionValueWithSource('sheetRange', selectedRange, 'env')
    options.sheetRange = selectedRange

    const {
      parsedWorksheetRange,
      parsedRange,
      worksheetRange,
    } = extractRangeInfo(ws, options.sheetRange)

    compareAndLogRanges(parsedRange, parsedWorksheetRange, options.sheetRange, worksheetRange)
  }
  if (isUndefined(options.rangeIncludesHeader)) {
    options.rangeIncludesHeader = await setRangeIncludesHeader(options.sheetRange, options.rangeIncludesHeader)
    this.setOptionValueWithSource('rangeIncludesHeader', options.rangeIncludesHeader, 'env')
  }
  if (options.rangeIncludesHeader === false && options.writeHeader === true) {
    options.writeHeader = false
    this.setOptionValueWithSource('writeHeader', false, 'env')
  }

  const { parsedRange } = extractRangeInfo(ws, options.sheetRange)

  const [fields, ...data] = extractDataFromWorksheet(parsedRange, ws)

  const firstRowHasNilValue = fields.some(f => isNil(f))

  if (!this.opts().categoryField) {
    let newCategory: string

    const [, confirmCategory] = await tryPrompt('confirm', {
      message: 'Would you like to select a field to split the file into separate files?',
      default: false,
    }, { signal: AbortSignal.timeout(7500) })

    if (confirmCategory === true) {
      if (options.rangeIncludesHeader === true && !firstRowHasNilValue) {
        newCategory = await tryPrompt('select', {
          message: `Select a column to group rows from input file by...`,
          choices: [...(fields as string[]).sort(), new Prompts.Separator()],
          loop: true,
        })
      }
      else {
        newCategory = await tryPrompt('number', {
          min: 1,
          max: fields.length,
          message: 'Select a column number to group by',
          default: undefined,
        }) as unknown as string
      }
      if (typeof newCategory === 'string' && newCategory.length) {
        options.categoryField = newCategory
        this.setOptionValueWithSource('categoryField', newCategory, 'env')
      }
    }
  }
  if (firstRowHasNilValue) {
    spinner.warn(chalk.yellowBright(`The first row in the selected range contains null values; parsing and load may fail`))
    await timers.setTimeout(2500)
  }

  const commandLineString = generateCommandLineString(options, this)

  fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), stringifyCommandOptions(options, commandLineString))
  parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
  fs.ensureDirSync(parsedOutputFile.dir)

  const finalData: Array<Record<string, JsonPrimitive>> | Array<JsonPrimitive[]> = []

  if (options.rangeIncludesHeader === true) {
    for (const values of data) {
      const dataObject = {
        ...zipToObject(fields as string[], values),
        source_file: basename(options.filePath),
        source_sheet: options.sheetName,
        source_range: options.sheetRange,
      }

      if (applyFilters(options)(dataObject)) {
        dataStream.write(dataObject)
      }
    }
  }
  else {
    for (const dataObject of [[...fields, 'source_file', 'source_sheet', 'source_range'], ...data.map(v => [...v, basename(options.filePath), options.sheetName, options.sheetRange])]) {
      if (applyFilters(options)(dataObject)) {
        dataStream.write(dataObject)
      }
    }
  }

  const stringifyStream = stringify({
    bom: true,
    columns: options.rangeIncludesHeader ? fields : undefined,
    header: options.writeHeader,
  })

  stringifyStream.on('data', (data) => {
    console.log(data.toString())
  })
  dataStream.pipe(stringifyStream)
}
