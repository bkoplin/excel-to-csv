import type {
  Callback,
  ColumnOption,
  Options,
  Parser,
} from 'csv-parse'
import type { HandlerCallback } from 'stream-transform'
import type {
  JsonObject,
  JsonPrimitive,
  Merge,
  PositiveInfinity,
} from 'type-fest'
import type {
  CSVOptions,
  FileMetrics,
} from '../types'
import type { CsvDataPayload } from './../types'
import { once } from 'node:events'
import {
  createReadStream,
  createWriteStream,
} from 'node:fs'
import { pipeline } from 'node:stream'
import {
  isDef,
  isNull,
} from '@antfu/utils'
import { Command } from '@commander-js/extra-typings'
import { Separator } from '@inquirer/core'
import chalk from 'chalk'
import { parse as parser } from 'csv'
import { stringify as csvStringifySync } from 'csv/sync'
import filenamify from 'filenamify'
import fs from 'fs-extra'
import {
  at,
  concat,
  find,
  isEmpty,
  isNil,
  isObjectLike,
  isUndefined,
  mapValues,
  sumBy,
} from 'lodash-es'
import numbro from 'numbro'
import ora from 'ora'
import Papa from 'papaparse'
import {
  basename,
  join,
  parse,
  relative,
} from 'pathe'
import { filename } from 'pathe/utils'
import {
  alphabetical,
  clone,
  get,
  isEqual,
  isString,
  omit,
  pick,
  shake,
  sleep,
} from 'radash'
import { transform } from 'stream-transform'
import Table from 'table-layout'
import { stringify as makeYAML } from 'yaml'
import {
  applyFilters,
  checkAndResolveFilePath,
  formatHeaderValues,
  generateCommandLineString,
  generateParsedCsvFilePath,
  stringifyCommandOptions,
  tryPrompt,
} from '../helpers'
import categoryOption from '../options/categoryField'
import delimiter from '../options/delimiter'
import escape from '../options/escape'
import fileSizeOption from '../options/fileSize'
import fromLine from '../options/fromLine'
import makeFilePathOption from '../options/makeFilePath'
import matchType from '../options/matchType'
import quote from '../options/quote'
import includesHeaderOption from '../options/rangeIncludesHeader'
import rowCount from '../options/rowCount'
import filterValuesOption from '../options/rowFilters'
import writeHeaderOption from '../options/writeHeader'

const spinner = ora({
  hideCursor: false,
  discardStdin: false,
})

export const csvCommand = new Command('csv')
  .description('Parse a CSV file')
  .addOption(makeFilePathOption('CSV'))
  .addOption(fromLine)
  .addOption(rowCount)
  .addOption(fileSizeOption)
  .addOption(includesHeaderOption)
  .addOption(writeHeaderOption)
  .addOption(filterValuesOption)
  .addOption(matchType)
  .addOption(categoryOption)
  .addOption(delimiter)
  .addOption(quote)
  .addOption(escape)
  .action(async function () {
    const options = this.opts() as unknown as CSVOptions

    const newFilePath = await checkAndResolveFilePath({
      fileType: 'CSV',
      argFilePath: options.filePath,
    })

    if (newFilePath !== options.filePath) {
      this.setOptionValueWithSource('filePath', newFilePath, 'cli')
    }
    spinner.text = (chalk.magentaBright(`Reading ${basename(options.filePath)} (this may take a minute)`))

    const inputStream = createReadStream(options.filePath, 'utf8')

    await new Promise<void>(resolve => inputStream.on('readable', async () => {
      spinner.succeed(chalk.greenBright(`SUCCESSFULLY OPENED ${basename(options.filePath)}`))
      await sleep(750)
      resolve()
    }))

    const parsedOutputFile = generateParsedCsvFilePath({
      parsedInputFile: parse(options.filePath),
      filters: options.rowFilters,

    })

    if (isUndefined(options.rangeIncludesHeader)) {
      const [,includesHeaderAnswer] = await tryPrompt('confirm', {
        message: `Does ${basename(options.filePath)} include a header row?`,
        default: true,
      })

      options.rangeIncludesHeader = includesHeaderAnswer ?? false
      this.setOptionValueWithSource('rangeIncludesHeader', options.rangeIncludesHeader, 'cli')
    }
    if (options.rangeIncludesHeader === false && options.writeHeader === true) {
      options.writeHeader = false
      this.setOptionValueWithSource('writeHeader', false, 'cli')
    }
    spinner.text = (chalk.magentaBright(`PARSING "${filename(options.filePath)}" with the selected options`))

    const workingCategoryObjects: Record<string, FileMetrics> = {}

    const fileMetrics: FileMetrics[] = []

    let fields: string[] = []

    let columns: string[] = []

    const skippableLines: [string, number][] = []

    const readStream = createReadStream(options.filePath, 'utf-8')

    const commandLineStream = createWriteStream(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), 'utf-8')

    let totalParsedLines = 0

    let totalParsedRecords = 0

    let totalSkippedLines = get(options, 'fromLine', 0)

    let askToQuit: boolean

    const rowCountValue = get(options, 'rowCount', null)

    const fromLineValue = get(options, 'fromLine', 1)

    const rowMetadataColumns = ['source_file', 'line']

    let combinedRowCountFromLine: number | undefined

    if (!rowCountValue) {
      combinedRowCountFromLine = undefined
    }
    else {
      combinedRowCountFromLine = (rowCountValue || 1) + fromLineValue
    }

    let supressErrors = false

    let rawSplitHeaderRow: string[]

    const columnsFunction = (headerLine: string[]): ColumnOption[] => {
      rawSplitHeaderRow = headerLine

      const headerArray = headerLine.map(v => v.trim()).map(v => isEmpty(v) || isNil(v) ? false : v).map((v, i) => get(options, 'rangeIncludesHeader', false) ? v : `Column ${i + 1}`)

      fields = formatHeaderValues(headerArray).filter((v): v is string => v !== false)
      columns = concat(fields, rowMetadataColumns)
      fs.writeFileSync(join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`), csvStringifySync([columns]), 'utf8')

      return formatHeaderValues(headerArray) as (string | false)[]
    }

    const delimiterValue = get<CSVOptions['delimiter']>(options, 'delimiter', ',') === 'tab' ? '\t' : get(options, 'delimiter', ',')

    const csvSourceStream = (parser as unknown as (options: Options | undefined, callback?: Callback | undefined) => Parser)({
      bom: true,
      delimiter: delimiterValue,

      from_line: fromLineValue,
      to_line: combinedRowCountFromLine,
      encoding: 'utf-8',
      escape: get(options, 'escape'),

      info: true,
      trim: false,
      columns: columnsFunction,
      quote: get(options, 'quote'),
      relax_column_count: true,
      raw: true,
      on_record(chunk: CsvDataPayload) {
        if (isEqual(rawSplitHeaderRow, chunk.raw.split(delimiterValue)))
          return null

        return {
          record: chunk.record,
          info: chunk.info,
          raw: chunk.raw,
        }
      },
    })

    const filterRecordTransform = transform(async (chunk: CsvDataPayload, callback: HandlerCallback<{
      record: JsonObject
      info: CsvDataPayload['info']
    } | null>) => {
      const foundSkippableLine = skippableLines.find(([line]) => line === chunk.raw)

      if (typeof chunk === 'string' || typeof foundSkippableLine !== 'undefined') {
        totalSkippedLines++
        callback(null, null)
      }
      else {
        const categoryOption: string[] = get(options, 'categoryField', [])

        if (isEmpty(categoryOption) && this.getOptionValueSource('categoryField') !== 'cli' && fields.length > 0) {
          csvSourceStream.pause()

          const [, confirmCategory] = await tryPrompt('confirm', {
            message: 'Would you like to select a one or more fields to split the file into separate files?',
            default: false,
          })

          if (confirmCategory === true) {
            const columnChoices = fields.map((value, i) => ({
              name: `${chalk.yellowBright(value)} ${chalk.italic(`column ${i + 1}/${fields.length}`)}`,
              value,
            }))

            const [, newCategory] = await tryPrompt('checkbox', {
              message: `Select a column to group rows from input file by...`,
              choices: [...alphabetical(columnChoices, d => d.value), new Separator()],
              loop: true,
              pageSize: fields.length > 15 ? 15 : 7,
            })

            if (typeof newCategory !== 'undefined') {
              options.categoryField = newCategory as string[]
              this.setOptionValueWithSource('categoryField', newCategory, 'cli')
            }
            else {
              this.setOptionValueWithSource('categoryField', [], 'cli')
            }
          }
          else {
            this.setOptionValueWithSource('categoryField', [], 'cli')
          }
          csvSourceStream.resume()
        }
        else if (commandLineStream.writable) {
          const commandLineString = generateCommandLineString(options, this)

          commandLineStream.end(stringifyCommandOptions(options, commandLineString))
        }

        const chunkError = chunk.info.error

        if (chunkError) {
          csvSourceStream.pause()

          const errorMessage = get(chunk, 'info.error.message')

          const parsingErrorMessage = `Error parsing line ${chalk.bold.redBright(numbro(chunk.info.lines).format({ thousandSeparated: true }))} of ${chalk.bold.redBright(basename(options.filePath))}: ${chalk.italic.redBright(errorMessage)}`

          if (askToQuit !== false) {
            spinner.warn(`${parsingErrorMessage}\n${chalk.redBright('RAW LINE:')} ${chalk.yellowBright(JSON.stringify(chunk.raw))}`)

            const [, confirmQuit] = await tryPrompt('expand', {
              message: 'Would you like to quit parsing the file?',
              default: 'n',
              expanded: true,
              choices: [{
                name: 'yes',
                value: true,
                key: 'y',
              }, {
                name: 'no',
                value: false,
                key: 'n',
              }, {
                name: 'no and don\'t ask if I want to quit again',
                value: 'supress',
                key: 's',
              }],
            })

            if (confirmQuit === true) {
              const formattedLineCount = numbro(chunk.info.lines).format({ thousandSeparated: true })

              spinner.warn(chalk.cyanBright(`Quitting; consider re-running the program with the --from-line option set to ${chalk.redBright(formattedLineCount)} to set the header to line ${chalk.redBright(formattedLineCount)}`))
              callback(chunk.info.error)
            }
            else if (confirmQuit === 'supress') {
              askToQuit = false
            }
          }
          if (supressErrors === false) {
            const [, isSkippableLine] = await tryPrompt('expand', {
              message: `Is line ${chalk.redBright(chunk.info.lines)} skippable?\n\t${chalk.redBright('LINE:')} ${chalk.yellowBright(JSON.stringify(chunk.raw))}\n`,
              default: 'y',
              expanded: true,
              choices: [{
                name: 'yes',
                value: true,
                key: 'y',
              }, {
                name: 'no',
                value: false,
                key: 'n',
              }, {
                name: 'yes and don\'t ask about any more line errors',
                value: 'supress-yes',
                key: 'x',
              }, {
                name: 'no and don\'t ask about any more line errors',
                value: 'supress-no',
                key: 's',
              }],
            })

            if (isSkippableLine === true || isSkippableLine === 'supress-yes') {
              skippableLines.push([chunk.raw, chunk.info.lines])
              spinner.info(chalk.cyanBright(`SKIPPING LINE ${chalk.redBright(numbro(chunk.info.lines).format({ thousandSeparated: true }))} AND FUTURE EQUIVALENT LINES`))

              if (isSkippableLine === 'supress-yes')
                supressErrors = true

              await sleep(500)
            }
            else if (isSkippableLine === 'supress-no') {
              supressErrors = true

              let parsedRecord = mapValues(chunk.record, v => isString(v) ? v.trim() : v) as null | Merge<{ [index: string]: JsonPrimitive }, {
                record: JsonObject
                info: CsvDataPayload['info']
              }>

              parsedRecord = transformParsedRecord(parsedRecord, options, chunk)
              callback(null, parsedRecord)
            }
          }
          else {
            let parsedRecord = mapValues(chunk.record, v => isString(v) ? v.trim() : v) as null | Merge<{ [index: string]: JsonPrimitive }, {
              record: JsonObject
              info: CsvDataPayload['info']
            }>

            parsedRecord = transformParsedRecord(parsedRecord, options, chunk)
            callback(null, parsedRecord)
          }
          csvSourceStream.resume()
        }

        else {
          let parsedRecord = mapValues(chunk.record, v => isString(v) ? v.trim() : v) as null | Merge<{ [index: string]: JsonPrimitive }, {
            record: JsonObject
            info: CsvDataPayload['info']
          }>

          parsedRecord = transformParsedRecord(parsedRecord, options, chunk)
          callback(null, parsedRecord)
        }
      }
    })

    filterRecordTransform.on('data', async (chunk: CsvDataPayload) => {
      const {
        record,
        info,
      } = chunk

      fs.ensureDirSync(join(parsedOutputFile.dir, 'DATA'))

      const maxFileSizeBytes = getMaxFileSize(options)

      const CATEGORY: 'default' | string = isEmpty(options.categoryField)
        ? `default`
        : at(record, options.categoryField)
          .map(v => isEmpty(v) || isNil(v) ? 'EMPTY' : v)
          .join(' ')

      let baseOutputPath = parsedOutputFile.name

      if (CATEGORY !== 'default')
        baseOutputPath += ` ${CATEGORY}`

      if (isDef(options.rowFilters) && !isEmpty(options.rowFilters))
        baseOutputPath += ` FILTERED`

      if (!(CATEGORY in workingCategoryObjects)) {
        const line = csvStringifySync([record], {
          header: options.writeHeader,
          columns,
        })

        const lineBufferLength = Buffer.from(line).length

        const FILENUM = 1

        const PATH = join(parsedOutputFile.dir, 'DATA', `${filenamify(baseOutputPath, { replacement: '_' })} ${FILENUM}.csv`)

        const writeStream = createWriteStream(PATH, 'utf-8')

        workingCategoryObjects[CATEGORY] = {
          CATEGORY,
          BYTES: lineBufferLength,
          ROWS: 1,
          FILENUM,
          PATH,
          FILTER: options.rowFilters,
          stream: writeStream,
        }
        if (!workingCategoryObjects[CATEGORY].stream!.write(line)) {
          filterRecordTransform.pause()
          await once(workingCategoryObjects[CATEGORY].stream!, 'drain')
          filterRecordTransform.resume()
        }
        fileMetrics.push(workingCategoryObjects[CATEGORY])

        if (CATEGORY === 'default')
          spinner.info(chalk.yellowBright(`CREATED "${basename(PATH)}"`))
        else spinner.info(chalk.yellowBright(`CREATED "${basename(PATH)}" FOR CATEGORY "${CATEGORY}"`))
      }
      else {
        const line = csvStringifySync([record], {
          header: false,
          columns,
        })

        const lineBufferLength = Buffer.from(line).length

        if (!find(fileMetrics, { PATH: workingCategoryObjects[CATEGORY].PATH }))
          fileMetrics.push(workingCategoryObjects[CATEGORY])

        if ((lineBufferLength + workingCategoryObjects[CATEGORY].BYTES) > (maxFileSizeBytes)) {
          workingCategoryObjects[CATEGORY].stream!.end()
          workingCategoryObjects[CATEGORY] = clone(omit(workingCategoryObjects[CATEGORY], ['stream']))
          workingCategoryObjects[CATEGORY].FILENUM! += 1
          workingCategoryObjects[CATEGORY].BYTES = 0
          workingCategoryObjects[CATEGORY].ROWS = 0
          workingCategoryObjects[CATEGORY].PATH = join(parsedOutputFile.dir, 'DATA', `${filenamify(baseOutputPath, { replacement: '_' })} ${workingCategoryObjects[CATEGORY].FILENUM}.csv`)
          workingCategoryObjects[CATEGORY].stream = createWriteStream(workingCategoryObjects[CATEGORY].PATH, 'utf-8')

          if (CATEGORY === 'default')
            spinner.info(chalk.yellowBright(`CREATED "${basename(workingCategoryObjects[CATEGORY].PATH)}"`))
          else spinner.info(chalk.yellowBright(`CREATED "${basename(workingCategoryObjects[CATEGORY].PATH)}" FOR CATEGORY "${CATEGORY}"`))
        }
        if (!workingCategoryObjects[CATEGORY].stream!.write(line)) {
          filterRecordTransform.pause()
          await once(workingCategoryObjects[CATEGORY].stream!, 'drain')
          filterRecordTransform.resume()
        }
        workingCategoryObjects[CATEGORY].BYTES += lineBufferLength
        workingCategoryObjects[CATEGORY].ROWS++
        totalParsedLines = get(chunk, 'info.lines', totalParsedLines) - get(options, 'fromLine', 0)
        totalParsedRecords = get(chunk, 'info.records', totalParsedRecords)
      }
      for (const fileMetric of fileMetrics) {
        if (fileMetric.PATH === workingCategoryObjects[CATEGORY].PATH) {
          fileMetric.BYTES = workingCategoryObjects[CATEGORY].BYTES
          fileMetric.ROWS = workingCategoryObjects[CATEGORY].ROWS
        }
      }

      const totalWrittenRecords = sumBy(fileMetrics, 'ROWS')

      if ((totalWrittenRecords % 10000) === 0 && totalWrittenRecords > 0) {
        spinner.info(`${chalk.magentaBright(`READ ${numbro(info.lines).format({ thousandSeparated: true })} LINES`)}; ${chalk.greenBright(`WROTE ${numbro(totalWrittenRecords).format({ thousandSeparated: true })} RECORDS`)}`)
      }
    })
    filterRecordTransform.on('end', async () => {
      for (const file of fileMetrics) {
        if (file.stream!.writableNeedDrain === true) {
          await new Promise<void>((resolve) => {
            file.stream!.once('drain', () => {
              file.stream?.close()
              resolve()
            })
          })
        }
      }

      const table = new Table(fileMetrics.map(o => pick(o, ['CATEGORY', 'PATH', 'ROWS', 'BYTES'])), {
        maxWidth: 600,
        ignoreEmptyColumns: true,
        columns: [
          {
            name: 'CATEGORY',
            get: cellValue => cellValue === undefined || cellValue === 'default' ? '' : chalk.bold(chalk.yellow(cellValue)),
          },
          {
            name: 'PATH',
            get: cellValue => chalk.cyan(join('../', relative(parse(options.filePath).dir, cellValue as string))),
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

      const totalRows = sumBy(fileMetrics, 'ROWS')

      const totalFiles = fileMetrics.length

      const totalBytes = sumBy(fileMetrics, 'BYTES')

      const formattedTotalRows = numbro(totalRows).format({ thousandSeparated: true })

      const formattedTotalParsedLines = numbro(totalParsedLines).format({ thousandSeparated: true })

      const formattedTotalParsedRecords = numbro(totalParsedRecords).format({ thousandSeparated: true })

      const formattedTotalSkippedRecords = numbro(totalSkippedLines).format({ thousandSeparated: true })

      const formattedTotalFiles = numbro(totalFiles).format({ thousandSeparated: true })

      const formattedTotalBytes = numbro(totalBytes).format({
        output: 'byte',
        spaceSeparated: true,
        base: 'general',
        average: true,
        mantissa: 2,
        optionalMantissa: true,
      })

      const parseOutputs = fileMetrics.map((o) => {
        return mapValues(shake(omit(o, ['stream', 'FILENUM']), v => isUndefined(v)), (v, k) => {
          if (k === 'CATEGORY' && !isEmpty(options.categoryField)) {
            return `${options.categoryField.join(' + ')} = ${v}`
          }
          else {
            return isObjectLike(v) ? !isEmpty(v) ? makeYAML(v) : undefined : v
          }
        })
      })

      const parseResultsCsv = Papa.unparse(parseOutputs)

      const summaryString = makeYAML({
        'TOTAL LINES PARSED': formattedTotalParsedLines,
        'TOTAL RECORDS PARSED': formattedTotalParsedRecords,
        'TOTAL LINES SKIPPED': formattedTotalSkippedRecords,
        'TOTAL NON-HEADER ROWS WRITTEN': formattedTotalRows,

        'TOTAL BYTES WRITTEN': formattedTotalBytes,
        'TOTAL FILES': numbro(totalFiles).format({ thousandSeparated: true }),

      })

      fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OUTPUT FILES.csv`), parseResultsCsv)
      fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT SUMMARY.yaml`), summaryString)

      let spinnerText = `SUCCESSFULLY READ ${chalk.magentaBright(formattedTotalParsedLines)} LINES AND WROTE ${chalk.green(formattedTotalRows)} LINES TO ${chalk.green(formattedTotalFiles)} FILES OF TOTAL SIZE ${chalk.green(formattedTotalBytes)}\n`

      if (options.writeHeader)
        spinnerText += chalk.yellow(`THE HEADER IS WRITTEN TO EACH FILE\n`)
      else spinnerText += chalk.yellow(`THE HEADER FOR ALL FILES IS ${chalk.cyan(`"${parse(join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`)).base}"`)}\n`)

      spinner.stopAndPersist({
        symbol: '🚀',
        text: `${spinnerText}\n${table.toString()}`,
      })
    })
    pipeline(
      readStream,
      csvSourceStream,
      async (err) => {
        if (err && err.code !== 'ERR_STREAM_PREMATURE_CLOSE') {
          spinner.fail(chalk.redBright(`Error parsing and splitting ${filename(options.filePath)}, ${err.message}`))
          process.exit(1)
        }
      },
    )
    csvSourceStream.pipe(filterRecordTransform)
  })

function transformParsedRecord(parsedRecord: null | Merge<{ [index: string]: JsonPrimitive }, {
  record: JsonObject
  info: CsvDataPayload['info']
}>, options: CSVOptions, chunk: CsvDataPayload): null | Merge<{ [index: string]: JsonPrimitive }, {
  record: JsonObject
  info: CsvDataPayload['info']
}> {
  if (!isNull(parsedRecord) && applyFilters(options)(parsedRecord as { [index: string]: JsonPrimitive })) {
    parsedRecord.record = {
      ...(parsedRecord as { [index: string]: JsonPrimitive }),
      ...{
        source_file: basename(options.filePath),
        line: chunk.info.lines,
      },
    }
    parsedRecord.info = chunk.info
  }
  else {
    parsedRecord = null
  }

  return parsedRecord
}
function getMaxFileSize(options: CSVOptions): number | PositiveInfinity {
  const { fileSize } = options

  if (typeof fileSize === 'number' && fileSize > 0)
    return fileSize * 1024 * 1024
  else return Infinity
}
