import type {
  CastingContext,
  Info,
} from 'csv-parse'
import type { HandlerCallback } from 'stream-transform'
import type {
  JsonObject,
  JsonPrimitive,
  PositiveInfinity,
} from 'type-fest'
import type {
  CSVOptions,
  FileMetrics,
} from '../types'
import { once } from 'node:events'
import {
  createReadStream,
  createWriteStream,
} from 'node:fs'
import { pipeline } from 'node:stream'
import { isDef } from '@antfu/utils'
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
  map,
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
  .addOption(categoryOption)
  .addOption(delimiter)
  .addOption(quote)
  .addOption(escape)
  .action(async function () {
    const options = this.opts()

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
      spinner.succeed(chalk.greenBright(`Successfully OPENED ${basename(options.filePath)}`))
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

    let rawHeaderLine: string

    const rawPartialLine = {
      line: 0,
      rowArray: [] as string[],
      row: '',
    }

    const skippableLines: [string, number][] = []

    const readStream = createReadStream(options.filePath, 'utf-8')

    const commandLineStream = createWriteStream(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), 'utf-8')

    let totalParsedLines = 0

    let totalParsedRecords = 0

    let totalSkippedLines = get(options, 'fromLine', 0)

    let askAboutErrors: boolean

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

    let rawSplitHeaderRow: string[]

    const columnsFunction = (headerLine: string[]) => {
      rawSplitHeaderRow = headerLine

      const headerArray = headerLine.map(v => v.trim()).map(v => isEmpty(v) || isNil(v) ? false : v).map((v, i) => get(options, 'rangeIncludesHeader', false) ? v : `Column ${i + 1}`)

      fields = formatHeaderValues(headerArray).filter((v): v is string => v !== false)
      columns = concat(fields, rowMetadataColumns)
      fs.writeFileSync(join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`), csvStringifySync([columns]), 'utf8')

      return formatHeaderValues(headerArray)
    }

    const csvSourceStream = parser({
      bom: true,
      delimiter: get(options, 'delimiter', ','),

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
        if (isEqual(rawSplitHeaderRow, chunk.raw.split(get(options, 'delimiter', ','))))
          return null

        return {
          record: chunk.record,
          info: chunk.info,
          raw: chunk.raw,
        }
      },
    })

    interface CsvDataPayload {
      record: Record<string, JsonPrimitive>
      info: Info & CastingContext
      raw: string
    }

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

          if (askAboutErrors !== false) {
            spinner.warn(`${parsingErrorMessage}\n${chalk.redBright('RAW LINE:')} ${chalk.yellowBright(JSON.stringify(chunk.raw))}`)

            const [, confirmQuit] = await tryPrompt('confirm', {
              message: 'Would you like to quit parsing the file?',
              default: false,
            })

            if (confirmQuit === true) {
              const formattedLineCount = numbro(chunk.info.lines).format({ thousandSeparated: true })

              spinner.warn(chalk.cyanBright(`Quitting; consider re-running the program with the --from-line option set to ${chalk.redBright(formattedLineCount)} to set the header to line ${chalk.redBright(formattedLineCount)}`))
              callback(chunk.info.error)
            }
            else if (isUndefined(askAboutErrors)) {
              const [, confirmAboutAsking] = await tryPrompt('confirm', {
                message: 'Would you like to be asked if you want to quit due to future parsing errors from this file?',
                default: false,
              })

              if (confirmAboutAsking === true)
                askAboutErrors = true
              else askAboutErrors = false
            }
          }

          const [, isSkippableLine] = await tryPrompt('confirm', {
            message: `Is line ${chalk.redBright(chunk.info.lines)} skippable?\nLINE: ${chalk.yellowBright(JSON.stringify(chunk.raw))}\n`,
            default: true,
          })

          if (isSkippableLine === true) {
            skippableLines.push([chunk.raw, chunk.info.lines])
            spinner.info(chalk.cyanBright(`SKIPPING LINE ${chalk.redBright(numbro(chunk.info.lines).format({ thousandSeparated: true }))} AND FUTURE EQUIVALENT LINES`))
            await sleep(500)
          }
          csvSourceStream.resume()
        }

        else {
          const parsedRecord = mapValues(chunk.record, v => isString(v) ? v.trim() : v)

          if (applyFilters(options)(parsedRecord)) {
            callback(null, {
              record: {
                ...parsedRecord,
                ...{
                  source_file: basename(options.filePath),
                  line: chunk.info.lines,
                },
              },
              info: chunk.info,
            })
          }
          else {
            callback(null, null)
          }
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
        spinner.info(chalk.yellowBright(`CREATED "${basename(PATH)}" FOR CATEGORY "${CATEGORY}"`))
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
          spinner.info(chalk.yellowBright(`CREATED "${basename(workingCategoryObjects[CATEGORY].PATH)}" FOR CATEGORY "${CATEGORY}"`))
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
      // csvFileProcessor.resume()
    })
    //   if (err)
    //     spinner.fail(chalk.redBright(`Error parsing and splitting ${filename(options.filePath)}, ${err.message}`))
    // })
    // readStream.pipe(csvSourceStream).pipe(filterRecordTransform, { end: false })
    pipeline(
      readStream,
      csvSourceStream,
      filterRecordTransform,
      async (err) => {
        if (err && err.code !== 'ERR_STREAM_PREMATURE_CLOSE') {
          spinner.fail(chalk.redBright(`Error parsing and splitting ${filename(options.filePath)}, ${err.message}`))
          process.exit(1)
        }

        const newMetrics = await map(fileMetrics, async (file, i) => {
          if (file.stream!.writableFinished !== true) {
            await once(file.stream!, 'finish')
          }

          return Promise.resolve(file)
        })

        const table = new Table(newMetrics.map(o => pick(o, ['CATEGORY', 'PATH', 'ROWS', 'BYTES'])), {
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

        const totalRows = sumBy(newMetrics, 'ROWS')

        const totalFiles = newMetrics.length

        const totalBytes = sumBy(newMetrics, 'BYTES')

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

        const parseOutputs = newMetrics.map((o) => {
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
      },
    )

    // pipeline(
    //   readStream,
    //   csvSourceStream,
    // )
  })

function getMaxFileSize(options: CSVOptions): number | PositiveInfinity {
  const { fileSize } = options

  if (typeof fileSize === 'number' && fileSize > 0)
    return fileSize * 1024 * 1024
  else return Infinity
}
