import type { JsonPrimitive } from 'type-fest'
import type { FileMetrics } from '../types'
import { createWriteStream } from 'node:fs'
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
  concat,
  isEmpty,
  isNil,
  isUndefined,
} from 'lodash-es'
import ora, { oraPromise } from 'ora'
import {
  format,
  join,
  parse,
} from 'pathe'
import { filename } from 'pathe/utils'
import {
  get,
  zipToObject,
} from 'radash'
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
  createCsvFileName,
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

  // dataStream.on('readable', () => {
  //   const d = dataStream.read()

  //   console.log(d)
  // })

  const newFilePath = await checkAndResolveFilePath({
    fileType: 'Excel',
    argFilePath: options.filePath,
  })

  if (newFilePath !== options.filePath) {
    this.setOptionValueWithSource('filePath', newFilePath, 'cli')
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
    this.setOptionValueWithSource('sheetName', options.sheetName, 'cli')
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

    this.setOptionValueWithSource('sheetRange', selectedRange, 'cli')
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
    this.setOptionValueWithSource('rangeIncludesHeader', options.rangeIncludesHeader, 'cli')
  }
  if (options.rangeIncludesHeader === false && options.writeHeader === true) {
    options.writeHeader = false
    this.setOptionValueWithSource('writeHeader', false, 'cli')
  }

  const { parsedRange } = extractRangeInfo(ws, options.sheetRange)

  const [fields, ...data] = extractDataFromWorksheet(parsedRange, ws)

  const rowMetaData = [basename(options.filePath), options.sheetName, options.sheetRange]

  const firstRowHasNilValue = fields.some(f => isNil(f))

  const categoryOption = get(options, 'categoryField', [])

  if (isEmpty(categoryOption)) {
    const [, confirmCategory] = await tryPrompt('confirm', {
      message: 'Would you like to select a one or more fields to split the file into separate files?',
      default: false,
    })

    if (confirmCategory === true) {
      if (options.rangeIncludesHeader === true && !firstRowHasNilValue) {
        const [,newCategory] = await tryPrompt('checkbox', {
          message: `Select a column to group rows from input file by...`,
          choices: [...fields as string[], new Prompts.Separator()],
          loop: true,
          pageSize: fields.length > 15 ? 15 : 7,
        })

        if (typeof newCategory !== 'undefined') {
          options.categoryField = newCategory
          this.setOptionValueWithSource('categoryField', newCategory, 'cli')
        }
      }
      else {
        const [,newCategory] = await tryPrompt('number', {
          min: 1,
          max: fields.length,
          message: 'Select a column number to group by',
          default: undefined,
        }) as unknown as string

        if (typeof newCategory !== 'undefined') {
          options.categoryField = [newCategory]
          this.setOptionValueWithSource('categoryField', [newCategory], 'cli')
        }
      }
    }
  }
  if (firstRowHasNilValue) {
    spinner.warn(chalk.yellowBright(`The first row in the selected range contains null values; parsing and load may fail`))
    await timers.setTimeout(2500)
  }

  const stringifyStream = stringify({
    bom: true,
    columns: options.rangeIncludesHeader ? concat(fields, ['source_file', 'source_sheet', 'source_range']) : undefined,
    header: options.writeHeader,
  })

  stringifyStream.on('data', (data) => {

  })

  const filterStream = transform((values: JsonPrimitive[]) => {
    if (get(options, 'rangeIncludesHeader') === true) {
      const dataObject = zipToObject(concat(fields, ['source_file', 'source_sheet', 'source_range']), concat(values, rowMetaData))

      if (applyFilters(options)(dataObject))
        return dataObject
      else return null
    }
    else {
      const dataObject = concat(values, rowMetaData)

      if (applyFilters(options)(dataObject))
        return values
      else return null
    }
  })

  filterStream.pipe(stringifyStream)

  const commandLineString = generateCommandLineString(options, this)

  fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), stringifyCommandOptions(options, commandLineString))
  parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
  fs.ensureDirSync(parsedOutputFile.dir)
  if (options.rangeIncludesHeader !== true) {
    filterStream.write(fields)
  }
  for (const row of data) {
    filterStream.write(row)
  }

  const files: FileMetrics[] = []

  const outputFilePath = format({
    ...parsedOutputFile,
    name: createCsvFileName({
      ...options,
      parsedOutputFile,

    }, options.fileSize ? 1 : undefined),
  })

  files.push({
    BYTES: 0,
    FILENUM: 1,
    ROWS: 0,
    CATEGORY: options.categoryField,
    FILTER: options.rowFilters,
    PATH: outputFilePath,
  })
  stringifyStream.on('data', (data) => {
    files[files.length - 1].BYTES += Buffer.from(data).length
    files[files.length - 1].ROWS += 1
    // console.log({
    //   byteLength: data.length,
    //   string: data.toString(),
    // })
  })
}
function writeCsvOutput(options: {
  parsedOutputFile: Omit<ParsedPath, 'base'>
  skippedLines: number | undefined
  bytesRead: number | undefined
  spinner: Ora
  files: FileMetrics[]
  fields: string[]
  parsedLines: number
}, commandOptions, csvOutput: string) {
  if (options.files.length === 0) {
    const FILENUM = (commandOptions.fileSize ? 1 : undefined)

    const outputFilePath = format({
      ...options.parsedOutputFile,
      name: createCsvFileName(options, FILENUM),
    })

    const stream = createWriteStream(outputFilePath, 'utf-8')

    // stream.on('finish', () => {
    //   parser.pause()
    //   const totalRows = sumBy(options.files, 'ROWS')
    //   spinner.text = chalk.magentaBright(`PARSED ${numbro(parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `) + chalk.yellow(`FINISHED WITH "${filename(outputFilePath)}"`)
    //   delay(() => parser.resume(), 500)
    // })
    const activeFileObject = {
      BYTES: 0,
      FILENUM,
      ROWS: 0,
      CATEGORY: options.category,
      FILTER: commandOptions.rowFilters,
      PATH: outputFilePath,
      stream,
    }

    // parser.pause()
    options.files.push(activeFileObject)
    writeToActiveStream(activeFileObject.PATH, csvOutput, options)

    const totalRows = sumBy(options.files, 'ROWS')

    options.spinner.text = chalk.magentaBright(`PARSED ${numbro(options.parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `) + chalk.yellow(`CREATED "${filename(outputFilePath)};"`)
    // await new Promise(resolve => delay(() => resolve(parser.resume()), 500))
  }
  else {
    let activeFileIndex = !isUndefined(options.category) ? findLastIndex(options.files, { CATEGORY: options.category }) : (options.files.length - 1)

    if (activeFileIndex > -1 && !isUndefined(commandOptions.fileSize) && isNumber(commandOptions.fileSize) && (options.files[activeFileIndex].BYTES + Buffer.from(csvOutput).length) > (commandOptions.fileSize * 1024 * 1024)) {
      const activeFileObject = options.files[activeFileIndex]

      if (activeFileObject.stream?.writableNeedDrain) {
        activeFileObject.stream.once('drain', () => {
          activeFileObject!.stream!.close()
        })
      }
      else {
        activeFileObject.stream!.close()
      }

      const FILENUM = activeFileObject.FILENUM! + 1

      const outputFilePath = format({
        ...options.parsedOutputFile,
        name: createCsvFileName(options, FILENUM),
      })

      const stream = createWriteStream(outputFilePath, 'utf-8')

      const newActiveFileObject = {
        BYTES: 0,
        FILENUM,
        ROWS: 0,
        PATH: outputFilePath,
        CATEGORY: options.category,
        FILTER: commandOptions.rowFilters,
        stream,
      }

      options.files.push(newActiveFileObject)
      activeFileIndex = options.files.length - 1
      writeToActiveStream(activeFileObject.PATH, csvOutput, options)
    }
    else {
      writeToActiveStream(options.files[activeFileIndex].PATH, csvOutput, options)
    }
  }
}
