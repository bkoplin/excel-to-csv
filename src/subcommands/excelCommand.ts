import type { Stringifier } from 'csv-stringify'
import type {
  JsonPrimitive,
  Simplify,
} from 'type-fest'
import type { FileMetrics } from '../types'
import { once } from 'node:events'
import { createWriteStream } from 'node:fs'
import { basename } from 'node:path'
import {
  pipeline,
  Readable,
  Transform,
} from 'node:stream'
import timers from 'node:timers/promises'
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
  has,
  isEmpty,
  isNil,
  isString,
  isUndefined,
  last,
} from 'lodash-es'
import numbro from 'numbro'
import ora, { oraPromise } from 'ora'
import {
  join,
  parse,
} from 'pathe'
import { filename } from 'pathe/utils'
import {
  get,
  isArray,
  zipToObject,
} from 'radash'
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

  let fields: string[] = []

  const allSourceData = extractDataFromWorksheet(parsedRange, ws)

  const firstRowHasNilValue = isArray(allSourceData?.[0]) && allSourceData[0].some(f => isNil(f))

  if (firstRowHasNilValue) {
    spinner.warn(chalk.yellowBright(`The first row in the selected range contains null values; columns have been named "Column 1", "Column 2", etc.`))
    await timers.setTimeout(2500)
  }
  if (options.rangeIncludesHeader && !firstRowHasNilValue) {
    fields = allSourceData.shift()
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
      // if (options.rangeIncludesHeader === true && !firstRowHasNilValue) {
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
        options.categoryField = newCategory
        this.setOptionValueWithSource('categoryField', newCategory, 'cli')
      }
      // }
      // else {
      //   const [, newCategory] = await tryPrompt('number', {
      //     min: 1,
      //     max: fields.length,
      //     message: 'Select the 1-indexed column number to group by',
      //     default: undefined,
      //   })

      //   if (typeof newCategory !== 'undefined') {
      //     options.categoryField = [`${newCategory - 1}`]
      //     this.setOptionValueWithSource('categoryField', [`${newCategory - 1}`], 'cli')
      //   }
      // }
    }
  }
  // if (firstRowHasNilValue) {
  //   spinner.warn(chalk.yellowBright(`The first row in the selected range contains null values; parsing and load may fail`))
  //   await timers.setTimeout(2500)
  // }

  const files: FileMetrics[] = []

  const makeDataObjects = new Transform({
    objectMode: true,
    transform(chunk: JsonPrimitive[], encoding, callback: (error?: Error | null, data?: Record<string, string | number | boolean | null>) => void) {
      // (inputValues: JsonPrimitive[]) => {
      const values = chunk.map(v => isString(v) ? v.trim() : v)

      const dataObject = zipToObject(concat(fields, ['source_file', 'source_sheet', 'source_range']), concat(values, rowMetaData))

      if (applyFilters(options)(dataObject))
        callback(null, dataObject)
        // }
    },
  })

  type RowSet = Simplify<{
    lines: Array<Buffer>
    fileName: string
    stringifier: Stringifier
    fileNumber: number
  } & FileMetrics>

  const commandLineString = generateCommandLineString(options, this)

  fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), stringifyCommandOptions(options, commandLineString))
  parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
  fs.ensureDirSync(parsedOutputFile.dir)
  spinner.start(chalk.magentaBright(`PARSING "${filename(options.filePath)}"`))

  const fileCategoryObject: Record<string, FileMetrics[]> = {}

  const inputDataStream = await Readable.from(allSourceData)

  const objectifyStream = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      const values = chunk.map(v => isString(v) ? v.trim() : v)

      const d = zipToObject(concat(fields, ['source_file', 'source_sheet', 'source_range']), concat(values, rowMetaData))

      callback(null, d)
    },

  })

  const filterStream = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      if (applyFilters(options)(chunk))
        callback(null, chunk)
    },
  })

  const categoryStream = new Transform({
    objectMode: true,
    async transform(chunk, encoding, callback) {
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

        spinner.text = chalk.yellowBright(`IDENTIFIED CATEGORY "${CATEGORY}"`)
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

        writeStream.on('finish', () => {
          const formattedBytes = numbro(fileObject.BYTES).format({
            output: 'byte',
            spaceSeparated: true,
            base: 'binary',
            average: true,
            mantissa: 2,
            optionalMantissa: true,
          })

          const formattedLineCount = numbro(fileObject.ROWS).format({ thousandSeparated: true })

          spinner.text = (chalk.yellow(`FINISHED WITH "${basename(fileObject.PATH)}"; WROTE `) + chalk.magentaBright(`${formattedBytes} BYTES, `) + chalk.greenBright(`${formattedLineCount} LINES; `))
        })
        writeStream.write(line)
        fileCategoryObject[CATEGORY] = [fileObject]
      }
      else {
        let line = stringify([chunk], {
          header: false,
          columns: concat(fields, ['source_file', 'source_sheet', 'source_range']),
        })

        const lineBufferLength = Buffer.from(line).length

        const fileObject = last(fileCategoryObject[CATEGORY])!

        const maxFileSizeBytes = typeof options.fileSize === 'number' && options.fileSize > 0 ? (options.fileSize ?? 0) * 1024 * 1024 : Infinity

        if (lineBufferLength + fileObject.BYTES > (maxFileSizeBytes)) {
          if (fileObject.stream!.writableNeedDrain)
            await once(fileObject.stream!, 'drain')

          fileObject.stream!.close()

          const FILENUM = fileObject.FILENUM! + 1

          PATH = join(parsedOutputFile.dir, `${PATH} ${FILENUM}.csv`)

          const writeStream = createWriteStream(PATH, 'utf-8')

          writeStream.on('finish', () => {
            const formattedBytes = numbro(fileObject.BYTES).format({
              output: 'byte',
              spaceSeparated: true,
              base: 'binary',
              average: true,
              mantissa: 2,
              optionalMantissa: true,
            })

            const formattedLineCount = numbro(fileObject.ROWS).format({ thousandSeparated: true })

            spinner.text = (chalk.yellow(`FINISHED WITH "${basename(fileObject.PATH)}"; WROTE `) + chalk.magentaBright(`${formattedBytes} BYTES, `) + chalk.greenBright(`${formattedLineCount} LINES; `))
          })
          line = stringify([chunk], {
            header: options.writeHeader,
            columns: concat(fields, ['source_file', 'source_sheet', 'source_range']),
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
        // acc[findIndex(acc, { CATEGORY })].stringifier.write(chunk)
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
    },
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

        for (const category in files) {
          for (const file of files[category]) {
            if (isDef(file.stream) && file.stream.writableFinished !== true) {
              if (file.stream.writableNeedDrain)
                await once(file.stream, 'drain')

              file.stream.close()
            }
          }
        }
        spinner.succeed(`${chalk.greenBright(`Successfully parsed and split ${filename(options.filePath)}`)}. ${chalk.italic('Write functions will continue to run until all streams are closed')}`)
      }
    },
  )
}
// .map((chunk: JsonPrimitive): Record<string, JsonPrimitive> => {
// }, { concurrency: 20 })
// .filter<Record<string, JsonPrimitive>>(d => applyFilters(options)(d), { concurrency: 20 })
// .reduce((acc: RowSet[], chunk: Record<string, JsonPrimitive>) => {

//   return acc
// }, [] as RowSet[])

// for (const rowSet/ of inputDK

// .reduce((acc: number, chunk: string) => {})
// .pipe(makeDataObjects).pipe(stringifier)
// let headerline: Buffer

// for await (const rowSet of inputDataStream) {
//   if (!headerline && files.length === 0 && (row as Buffer).length > 0) {
//     headerline = row
//   }

//   const CATEGORY = isEmpty(options.categoryField) ? 'default' : at(row, options.categoryField).join(' ')

//   const fileIndex = files.findIndex(f => f.CATEGORY === CATEGORY)

//   if (fileIndex === -1) {
//     const fileNumber = typeof options.fileSize === 'number' && options.fileSize > 0 ? 1 : undefined

//     const formattedFileName = createCsvFileName({
//       parsedOutputFile,
//       category: CATEGORY,
//     }, fileNumber)

//     const outputFilePath = format({
//       dir: parsedOutputFile.dir,
//       ext: '.csv',
//       name: formattedFileName,
//     })

//     const destinationStream = fs.createWriteStream(outputFilePath, 'utf-8')

//     files.push({
//       BYTES: row.length,
//       CATEGORY,
//       FILENUM: fileNumber,
//       ROWS: 1,
//       stream: destinationStream,
//       FILTER: options.rowFilters,
//     })

//     // const fileIndex = files.findIndex(f => f.PATH === outputFilePath)

// const formattedBytes = numbro(files[fileIndex].BYTES).format({
//   output: 'byte',
//   spaceSeparated: true,
//   base: 'binary',
//   average: true,
//   mantissa: 2,
//   optionalMantissa: true,
// })

// const formattedLineCount = numbro(files[fileIndex].ROWS).format({ thousandSeparated: true })

//     spinner.info(chalk.magentaBright(`CREATED "${basename(outputFilePath)}"`))
//   }
// }

// if (options.rangeIncludesHeader !== true) {
//   filterStream.write(fields)
// }
// for (const row of data) {
//   filterStream.write(row)
// }
// filterStream.pipe(fileUpdateStream)
