import type {
  CastingContext,
  Info,
} from 'csv-parse'
import type { JsonPrimitive } from 'type-fest'
import type { FileMetrics } from '../types'
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
  has,
  isEmpty,
  isNil,
  isObjectLike,
  isUndefined,
  last,
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
  get,
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
    // options.bytesRead = bytesRead
    // failText: chalk.redBright(`failure reading ${basename(options.filePath)}`),
    // this.setOptionValueWithSource('bytesRead', bytesRead, 'default')
    // if (typeof options.sheetName !== 'string' || !wb.SheetNames.includes(options.sheetName)) {
    //   options.sheetName = await setSheetName(wb)
    //   this.setOptionValueWithSource('sheetName', options.sheetName, 'cli')
    // }

    const parsedOutputFile = generateParsedCsvFilePath({
      parsedInputFile: parse(options.filePath),
      filters: options.rowFilters,
      //   sheetName: options.sheetName,
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

    const fileCategoryObject: Record<string, FileMetrics[]> = {}

    let fields: string[] = []

    let columns: string[] = []

    let rawHeaderLine: string

    const rawPartialLine = {
      line: 0,
      rowArray: [] as string[],
      row: '',
    }

    const skippableLines: string[] = []

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

      // if (get(options, 'rangeIncludesHeader'))
      //   combinedRowCountFromLine--
    }
    const columnsFunction = (headerLine: string[]) => {
      const headerArray = headerLine.map(v => v.trim()).map(v => isEmpty(v) || isNil(v) ? false : v).map((v, i) => get(options, 'rangeIncludesHeader', false) ? v : `Column ${i + 1}`)

      fields = formatHeaderValues(headerArray).filter((v): v is string => v !== false)
      columns = concat(fields, rowMetadataColumns)
      fs.writeFileSync(join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`), csvStringifySync([columns]), 'utf8')

      return formatHeaderValues(headerArray)
    }

    const csvSourceStream = parser({
      bom: true,
      delimiter: get(options, 'delimiter', ','),
      // record_delimiter: '\r\n',
      from_line: fromLineValue,
      to_line: combinedRowCountFromLine,
      encoding: 'utf-8',
      escape: get(options, 'escape'),
      // relax_quotes: true,
      info: true,
      trim: false,
      columns: columnsFunction,
      quote: get(options, 'quote'),
      relax_column_count: true,
      raw: true,
      // on_record(chunk: CsvDataPayload) {
      //   return {
      //     record: chunk.record,
      //     info: chunk.info,
      //     raw: chunk.raw,
      //   }
      // },
    })

    interface CsvDataPayload {
      record: Record<string, JsonPrimitive>
      info: Info & CastingContext
      raw: string
    }

    const filterRecordTransform = transform(async (chunk: CsvDataPayload, callback) => {
      if (typeof chunk === 'string' || skippableLines.includes(chunk.raw)) {
        totalSkippedLines++

        if (has(chunk, 'info.lines'))
          spinner.info(chalk.cyanBright(`SKIPPING LINE ${numbro(chunk.info.lines).format({ thousandSeparated: true })}`))

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

        totalParsedLines = get(chunk, 'info.lines', totalParsedLines) - get(options, 'fromLine', 0)
        totalParsedRecords = get(chunk, 'info.records', totalParsedRecords)
        if (chunkError) {
          csvSourceStream.pause()

          const errorMessage = get(chunk, 'info.error.message')

          const parsingErrorMessage = `Error parsing line ${chalk.bold.redBright(numbro(totalParsedLines).format({ thousandSeparated: true }))} of ${chalk.bold.redBright(basename(options.filePath))}: ${chalk.italic.redBright(errorMessage)}`

          // if (typeof errorMessage === 'string') {
          //   parsingErrorMessage += `: ${chalk.italic.redBright(errorMessage)}`
          // }
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
                message: 'Would you like to be asked about future errors in parsing the file?',
                default: false,
              })

              if (confirmAboutAsking === true)
                askAboutErrors = true
              else askAboutErrors = false
            }
          // confirmQuitChoice = false
          }
          // else {
          //   spinner.info(`${parsingErrorMessage}`)
          // }

          const [, isSkippableLine] = await tryPrompt('confirm', {
            message: `Is line ${chalk.redBright(chunk.info.lines)} skippable?\nLINE: ${chalk.yellowBright(JSON.stringify(chunk.raw))}\n`,
            default: true,
          })

          if (isSkippableLine === true) {
            skippableLines.push(chunk.raw)
            spinner.info(chalk.cyanBright(`SKIPPING LINE ${chalk.redBright(numbro(chunk.info.lines).format({ thousandSeparated: true }))} AND FUTURE EQUIVALENT LINES`))
            await sleep(1000)
          }
          csvSourceStream.resume()
        }

        else {
          if (applyFilters(options)(chunk.record)) {
            const parsedRecord = mapValues(chunk.record, v => isString(v) ? v.trim() : v)

            callback(null, {
              ...parsedRecord,
              ...{
                source_file: basename(options.filePath),
                line: chunk.info.lines,
              },
            })
          }
          else {
            callback(null, null)
          }
        }
      }
    })

    // csvSourceStream.on('data', async (chunk: CsvDataPayload) => {

    //   filterRecordTransform.write(chunk.record)
    // })
    // const allSourceData = extractDataFromWorksheet(parsedRange, ws)

    // const firstRowHasNilValue = isArray(allSourceData?.[0]) && allSourceData[0].some(f => isNil(f))

    // if (firstRowHasNilValue) {
    //   spinner.warn(chalk.yellowBright(`The first row in the selected range contains null values; columns have been named "Column 1", "Column 2", etc.`))
    //   await sleep(2500)
    // }
    // if (options.rangeIncludesHeader && !firstRowHasNilValue) {
    //   fields = allSourceData.shift() as string[]
    // }
    // else {
    //   fields = allSourceData[0].map((_, i) => `Column ${i + 1}`)
    // }

    // const categoryOption = get(options, 'categoryField', [])

    // if (isEmpty(categoryOption)) {
    //   const [, confirmCategory] = await tryPrompt('confirm', {
    //     message: 'Would you like to select a one or more fields to split the file into separate files?',
    //     default: false,
    //   })

    //   if (confirmCategory === true) {
    //     const [, newCategory] = await tryPrompt('checkbox', {
    //       message: `Select a column to group rows from input file by...`,
    //       choices: [...fields.map(value => ({
    //         name: value,
    //         value,
    //       })), new Prompts.Separator()],
    //       loop: true,
    //       pageSize: fields.length > 15 ? 15 : 7,
    //     })

    //     if (typeof newCategory !== 'undefined') {
    //       options.categoryField = newCategory as string[]
    //       this.setOptionValueWithSource('categoryField', newCategory, 'cli')
    //     }
    //   }
    // }

    const csvFileProcessor = transform(async (chunk, callback) => {
      fs.ensureDirSync(join(parsedOutputFile.dir, 'DATA'))

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
        const line = csvStringifySync([chunk], {
          header: true,
          columns,
        })

        const lineBufferLength = Buffer.from(line).length

        const FILENUM = typeof options.fileSize === 'number' && options.fileSize > 0 ? 1 : undefined

        if (typeof FILENUM !== 'undefined')
          PATH += ` ${FILENUM}`

        PATH = join(parsedOutputFile.dir, 'DATA', `${PATH}.csv`)

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
          csvSourceStream.pause()

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
          csvSourceStream.resume()
        })
        writeStream.write(line)
        fileCategoryObject[CATEGORY] = [fileObject]
        spinner.text = chalk.yellowBright(`CREATED "${basename(PATH)}" FOR CATEGORY "${CATEGORY}"`)
        await sleep(750)
      }
      else {
        let line = csvStringifySync([chunk], {
          header: false,
          columns,
        })

        const lineBufferLength = Buffer.from(line).length

        const fileObject = last(fileCategoryObject[CATEGORY])!

        const maxFileSizeBytes = typeof options.fileSize === 'number' && options.fileSize > 0 ? (options.fileSize ?? 0) * 1024 * 1024 : Infinity

        if (lineBufferLength + fileObject.BYTES > (maxFileSizeBytes)) {
          if (fileObject.stream!.writableNeedDrain)
            await once(fileObject.stream!, 'drain')

          const FILENUM = typeof options.fileSize === 'number' && options.fileSize > 0 ? 1 : undefined

          if (typeof FILENUM !== 'undefined')
            PATH += ` ${FILENUM}`

          PATH = join(parsedOutputFile.dir, 'DATA', `${PATH}.csv`)

          const writeStream = createWriteStream(PATH, 'utf-8')

          writeStream.on('finish', async () => {
            csvSourceStream.pause()

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
            csvSourceStream.resume()
          })
          line = csvStringifySync([chunk], {
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
      readStream,
      csvSourceStream,
      filterRecordTransform,
      csvFileProcessor,
      // perLineTransformStream,
      // categoryStream,
      async (err) => {
        if (err && err.code !== 'ERR_STREAM_PREMATURE_CLOSE') {
          spinner.fail(chalk.redBright(`Error parsing and splitting ${filename(options.filePath)}, ${err.message}`))
          process.exit(1)
        }
        else {
          const files = fileCategoryObject

          const fileObjectArray = Object.values(files).flat()

          await Promise.all(fileObjectArray.map(async (file) => {
            if (isDef(file.stream) && file.stream.writableFinished !== true) {
              if (file.stream.writableNeedDrain)
                await once(file.stream, 'drain')

              await new Promise<void>(resolve => file.stream!.close(async () => {
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

            return true
          }))

          const table = new Table(fileObjectArray.map(o => pick(o, ['CATEGORY', 'PATH', 'ROWS', 'BYTES'])), {
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

          const totalRows = sumBy(fileObjectArray, 'ROWS')

          const totalFiles = fileObjectArray.length

          const totalBytes = sumBy(fileObjectArray, 'BYTES')

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

          const parseOutputs = fileObjectArray.map((o) => {
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
        }
      },
    )
  })
