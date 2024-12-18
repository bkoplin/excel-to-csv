import type { JsonPrimitive } from 'type-fest'
import type { FileMetrics } from '../types'
import { once } from 'node:events'
import {
  createWriteStream,
  writeFileSync,
} from 'node:fs'
import { basename } from 'node:path'
import {
  pipeline,
  Readable,
} from 'node:stream'
import { isDef } from '@antfu/utils'
import {
  Command,
  Option,
} from '@commander-js/extra-typings'
import * as Prompts from '@inquirer/prompts'
import chalk from 'chalk'
import { stringify } from 'csv/sync'
import filenamify from 'filenamify'
import fs from 'fs-extra'
import {
  at,
  concat,
  find,
  has,
  isEmpty,
  isNil,
  isObjectLike,
  isString,
  isUndefined,
  last,
  mapValues,
  omit,
  sumBy,
} from 'lodash-es'
import numbro from 'numbro'
import ora from 'ora'
import Papa from 'papaparse'
import {
  join,
  parse,
  relative,
} from 'pathe'
import { filename } from 'pathe/utils'
import {
  get,
  isArray,
  shake,
  sleep,
  zipToObject,
} from 'radash'
import { transform } from 'stream-transform'
import Table from 'table-layout'
import { stringify as makeYAML } from 'yaml'
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

  const newFilePath = await checkAndResolveFilePath({
    fileType: 'Excel',
    argFilePath: options.filePath,
  })

  if (newFilePath !== options.filePath) {
    this.setOptionValueWithSource('filePath', newFilePath, 'cli')
  }
  // spinner.start(chalk.magentaBright(`Reading ${basename(options.filePath)} (this may take a minute)`))

  const {
    wb,
    bytesRead,
  } = await getWorkbook(options.filePath)

  // spinner.succeed(chalk.greenBright(`Successfully read ${basename(options.filePath)}`))

  options.bytesRead = bytesRead
  // failText: chalk.redBright(`failure reading ${basename(options.filePath)}`),
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

  let fields: string[] = []

  const allSourceData = extractDataFromWorksheet(parsedRange, ws)

  const firstRowHasNilValue = isArray(allSourceData?.[0]) && allSourceData[0].some(f => isNil(f))

  if (firstRowHasNilValue) {
    spinner.warn(chalk.yellowBright(`The first row in the selected range contains null values; columns have been named "Column 1", "Column 2", etc.`))
    await sleep(2500)
  }
  if (options.rangeIncludesHeader && !firstRowHasNilValue) {
    fields = allSourceData.shift() as string[]
  }
  else {
    fields = allSourceData[0].map((_, i) => `Column ${i + 1}`)
  }

  const rowMetaData = [basename(options.filePath), options.sheetName, options.sheetRange]

  const categoryOption = get(options, 'categoryField', [])

  if (isEmpty(categoryOption)) {
    const [, confirmCategory] = await tryPrompt('confirm', {
      message: 'Would you like to select a one or more fields to split the file into separate files?',
      default: false,
    })

    if (confirmCategory === true) {
      const [,newCategory] = await tryPrompt('checkbox', {
        message: `Select a column to group rows from input file by...`,
        choices: [...fields.map(value => ({
          name: value,
          value,
        })), new Prompts.Separator()],
        loop: true,
        pageSize: fields.length > 15 ? 15 : 7,
      })

      if (typeof newCategory !== 'undefined') {
        options.categoryField = newCategory as string[]
        this.setOptionValueWithSource('categoryField', newCategory, 'cli')
      }
    }
  }

  const commandLineString = generateCommandLineString(options, this)

  fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), stringifyCommandOptions(options, commandLineString))
  parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
  fs.ensureDirSync(parsedOutputFile.dir)
  spinner.start(chalk.magentaBright(`PARSING "${filename(options.filePath)}" with the selected options`))

  const fileCategoryObject: Record<string, FileMetrics[]> = {}

  const inputDataStream = await Readable.from(allSourceData)

  const columns = concat(fields, ['source_file', 'source_sheet', 'source_range'])

  let totalParsedLines = 0

  const objectifyStream = transform<JsonPrimitive[]>((chunk, callback) => {
    const values = chunk.map(v => isString(v) ? v.trim() : v)

    const d = zipToObject(columns, concat(values, rowMetaData))

    totalParsedLines++
    callback(null, d)
  })

  const filterStream = transform<Record<string, JsonPrimitive>>((chunk, callback) => {
    const filteredResult = applyFilters(options)(chunk)

    if (filteredResult)
      callback(null, chunk)
    else callback(null, null)
  })

  const headerLine = stringify([columns])

  writeFileSync(join(parsedOutputFile.dir, '..', `${parsedOutputFile.name} HEADER.csv`), headerLine, 'utf8')

  const categoryStream = transform(async (chunk, callback) => {
    const CATEGORY: 'default' | string = isEmpty(options.categoryField)
      ? `default`
      : at(chunk, options.categoryField).map(v => isEmpty(v) || isNil(v) ? 'EMPTY' : v).join(' ')

    let PATH = parsedOutputFile.name

    if (CATEGORY !== 'default')
      PATH += ` ${CATEGORY}`

    if (isDef(options.rowFilters) && !isEmpty(options.rowFilters))
      PATH += ` FILTERED`

    PATH = filenamify(PATH, { replacement: '_' })
    if (!has(fileCategoryObject, CATEGORY)) {
      const line = stringify([chunk], {
        header: true,
        columns: concat(fields, ['source_file', 'source_sheet', 'source_range']),
      })

      const lineBufferLength = Buffer.from(line).length

      const FILENUM = typeof options.fileSize === 'number' && options.fileSize > 0 ? 1 : undefined

      PATH += ` ${FILENUM}`
      PATH = join(parsedOutputFile.dir, `${PATH}.csv`)

      const writeStream = createWriteStream(PATH, 'utf-8')

      const fileObject = {
        CATEGORY,
        BYTES: lineBufferLength,
        ROWS: 1,
        FILENUM,
        PATH,
        FILTER: options.rowFilters,
        stream: writeStream,
      }

      writeStream.on('finish', async () => {
        filterStream.pause()

        const thisFileObject = find(fileCategoryObject[CATEGORY], { PATH })!

        const formattedBytes = numbro(thisFileObject.BYTES).format({
          output: 'byte',
          spaceSeparated: true,
          base: 'binary',
          average: true,
          mantissa: 2,
          optionalMantissa: true,
        })

        const formattedLineCount = numbro(thisFileObject.ROWS).format({ thousandSeparated: true })

        spinner.text = (chalk.yellow(`FINISHED WITH "${basename(thisFileObject.PATH)}"; WROTE `) + chalk.magentaBright(`${formattedBytes} BYTES, `) + chalk.greenBright(`${formattedLineCount} LINES; `))
        await sleep(1000)
        filterStream.resume()
      })
      writeStream.write(line)
      fileCategoryObject[CATEGORY] = [fileObject]
      spinner.text = chalk.yellowBright(`CREATED "${basename(PATH)}" FOR CATEGORY "${CATEGORY}"`)
      await sleep(750)
    }
    else {
      let line = stringify([chunk], {
        header: false,
        columns,
      })

      const lineBufferLength = Buffer.from(line).length

      const fileObject = last(fileCategoryObject[CATEGORY])!

      const maxFileSizeBytes = typeof options.fileSize === 'number' && options.fileSize > 0 ? (options.fileSize ?? 0) * 1024 * 1024 : Infinity

      if (lineBufferLength + fileObject.BYTES > (maxFileSizeBytes)) {
        if (fileObject.stream!.writableNeedDrain)
          await once(fileObject.stream!, 'drain')

        const FILENUM = fileObject.FILENUM! + 1

        PATH = join(parsedOutputFile.dir, `${PATH} ${FILENUM}.csv`)

        const writeStream = createWriteStream(PATH, 'utf-8')

        writeStream.on('finish', async () => {
          filterStream.pause()

          const thisFileObject = find(fileCategoryObject[CATEGORY], { PATH })!

          const formattedBytes = numbro(thisFileObject.BYTES).format({
            output: 'byte',
            spaceSeparated: true,
            base: 'binary',
            average: true,
            mantissa: 2,
            optionalMantissa: true,
          })

          const formattedLineCount = numbro(thisFileObject.ROWS).format({ thousandSeparated: true })

          spinner.text = (chalk.yellow(`FINISHED WITH "${basename(thisFileObject.PATH)}"; WROTE `) + chalk.magentaBright(`${formattedBytes} BYTES, `) + chalk.greenBright(`${formattedLineCount} LINES; `))
          await sleep(1000)
          filterStream.resume()
        })
        line = stringify([chunk], {
          header: options.writeHeader,
          columns,
        })
        writeStream.write(line)
        fileCategoryObject[CATEGORY].push({
          CATEGORY,
          BYTES: lineBufferLength,
          ROWS: 1,
          FILENUM,
          PATH,
          FILTER: options.rowFilters,
          stream: writeStream,
        })
      }
      else {
        if (fileObject.stream!.writableNeedDrain)
          await once(fileObject.stream!, 'drain')

        fileObject.BYTES += lineBufferLength
        fileObject.ROWS++
        fileObject.stream!.write(line)
      }
    }
    callback()
  })

  pipeline(
    inputDataStream,
    objectifyStream,
    filterStream,
    categoryStream,
    async (err) => {
      if (err) {
        spinner.fail(chalk.redBright(`Error parsing and splitting ${filename(options.filePath)}, ${err.message}`))
        process.exit(1)
      }
      else {
        const files = fileCategoryObject

        const fileObjectArray = Object.values(files).flat()

        const totalRows = sumBy(fileObjectArray, 'ROWS')

        const totalFiles = fileObjectArray.length

        const totalBytes = sumBy(fileObjectArray, 'BYTES')

        const formattedTotalRows = numbro(totalRows).format({ thousandSeparated: true })

        const formattedTotalParsedLines = numbro(totalParsedLines).format({ thousandSeparated: true })

        const formattedTotalFiles = numbro(totalFiles).format({ thousandSeparated: true })

        const formattedTotalBytes = numbro(totalBytes).format({
          output: 'byte',
          spaceSeparated: true,
          base: 'general',
          average: true,
          mantissa: 2,
          optionalMantissa: true,
        })

        const parseJobOutputs = fileObjectArray.map((o) => {
          return mapValues(shake(omit(o, ['stream', 'FILENUM']), v => isUndefined(v)), (v, k) => {
            if (k === 'CATEGORY') {
              return `${options.categoryField.join(' + ')} = ${v}`
            }
            else {
              return isObjectLike(v) ? !isEmpty(v) ? makeYAML(v) : undefined : v
            }
          })
        })

        for (const file of fileObjectArray) {
          if (isDef(file.stream) && file.stream.writableFinished !== true) {
            if (file.stream.writableNeedDrain)
              await once(file.stream, 'drain')

            await new Promise<void>(resolve => file.stream!.end(async () => {
              const formattedBytes = numbro(file.BYTES).format({
                output: 'byte',
                spaceSeparated: true,
                base: 'binary',
                average: true,
                mantissa: 2,
                optionalMantissa: true,
              })

              const formattedLineCount = numbro(file.ROWS).format({ thousandSeparated: true })

              spinner.text = (chalk.yellow(`FINISHED WITH "${basename(file.PATH)}"; WROTE `) + chalk.magentaBright(`${formattedBytes} BYTES, `) + chalk.greenBright(`${formattedLineCount} LINES; `))
              await sleep(1000)
              resolve()
            }))
          }

          continue
        }

        const logTable = new Table(fileObjectArray, {
          maxWidth: 600,
          ignoreEmptyColumns: true,
          columns: [
            {
              name: 'CATEGORY',
              get: cellValue => cellValue === undefined ? '' : chalk.bold(chalk.yellow(cellValue)),
            },
            {
              name: 'PATH',
              get: cellValue => chalk.cyan(join('../', relative(parse(options.filePath).dir, cellValue as string))),
            },
            {
              name: 'FILENUM',
              get: _cellValue => '',
            },
            {
              name: 'stream',
              get: _cellValue => '',
            },
            {
              name: 'ROWS',
              get: cellValue => numbro(cellValue).format({ thousandSeparated: true }),
              padding: {
                left: 'ROWS: ',
                right: ' ',
              },
            },
            {
              name: 'BYTES',
              get: (cellValue) => {
                const formattedValue = numbro(cellValue)

                return formattedValue.format({
                  output: 'byte',
                  spaceSeparated: true,
                  base: 'general',
                  average: true,
                  mantissa: 2,
                  optionalMantissa: true,
                })
              },
            },
          ],

        })

        const parseResultsCsv = Papa.unparse(parseJobOutputs)

        const summaryString = makeYAML({
          'TOTAL LINES PARSED': formattedTotalParsedLines,
          'TOTAL NON-HEADER ROWS WRITTEN': formattedTotalRows,

          'TOTAL BYTES WRITTEN': formattedTotalBytes,
          'TOTAL FILES': numbro(totalFiles).format({ thousandSeparated: true }),

        })

        fs.outputFileSync(join(parsedOutputFile.dir, '..', `PARSE AND SPLIT OUTPUT FILES.csv`), parseResultsCsv)
        fs.outputFileSync(join(parsedOutputFile.dir, '..', `PARSE AND SPLIT SUMMARY.yaml`), summaryString)

        let spinnerText = `SUCCESSFULLY READ ${chalk.magentaBright(formattedTotalParsedLines)} LINES AND WROTE ${chalk.green(formattedTotalRows)} LINES TO ${chalk.green(formattedTotalFiles)} FILES OF TOTAL SIZE ${chalk.green(formattedTotalBytes)}\n`

        if (options.writeHeader)
          spinnerText += chalk.yellow(`THE HEADER IS WRITTEN TO EACH FILE\n`)
        else spinnerText += chalk.yellow(`THE HEADER FOR ALL FILES IS ${chalk.cyan(`"${parse(join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`)).base}"`)}\n`)

        spinner.stopAndPersist({
          symbol: '🚀',
          text: `${spinnerText}\n${logTable.toString()}`,
        })
      }
    },
  )
}
