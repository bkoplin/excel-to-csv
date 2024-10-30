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
import * as Prompts from '@inquirer/prompts'
import chalk from 'chalk'
import { parse as parser } from 'csv'
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
  basename,
  join,
  parse,
  relative,
} from 'pathe'
import { filename } from 'pathe/utils'
import {
  get,
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
import fileSizeOption from '../options/fileSize'
import fromLine from '../options/fromLine'
import makeFilePathOption from '../options/makeFilePath'
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
      await sleep(1000)
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

    let fields: string[] = []

    let rawHeaderLine: string

    const rawPartialLine = {
      line: 0,
      row: '',
    }

    const skippableLines: string[] = []

    const readStream = createReadStream(options.filePath, 'utf-8')

    let totalParsedLines = 0

    let totalParsedRecords = 0

    let totalSkippedRecords = get(options, 'fromLine', 0)

    let confirmQuitChoice: boolean

    const rowCountValue = get(options, 'rowCount', null)

    const fromLineValue = get(options, 'fromLine', 1)

    const rowMetaData = [basename(options.filePath), fromLineValue, rowCountValue]

    const combinedRowCountFromLine = !rowCountValue ? undefined : (rowCountValue || 1) + fromLineValue

    const columnsFunction = get(options, 'rangeIncludesHeader', false)
      ? (headerLine: string[]) => {
          const headerArray = headerLine.map(v => v.trim()).map(v => isEmpty(v) || isNil(v) ? false : v)

          fields = formatHeaderValues(headerArray).filter((v): v is string => v !== false)

          return formatHeaderValues(headerArray)
        }
      : false

    const csvSourceStream = parser({
      bom: true,
      delimiter: get(options, 'delimiter', ','),
      from_line: fromLineValue,
      to_line: combinedRowCountFromLine,
      encoding: 'utf-8',
      relax_quotes: true,
      info: true,
      trim: false,
      columns: columnsFunction,
      quote: false,
      relax_column_count: true,
      raw: true,
    })

    const perLineTransformStream = transform(async (chunk: {
      record: Record<string, JsonPrimitive>
      info: Info & CastingContext
      raw: string
    }, callback) => {
      const categoryOption = get(options, 'categoryField', [])

      if (isEmpty(categoryOption) && this.getOptionValueSource('categoryField') !== 'cli') {
        csvSourceStream.pause()

        const [, confirmCategory] = await tryPrompt('confirm', {
          message: 'Would you like to select a one or more fields to split the file into separate files?',
          default: false,
        })

        if (confirmCategory === true) {
          const [, newCategory] = await tryPrompt('checkbox', {
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
          else {
            this.setOptionValueWithSource('categoryField', [], 'cli')
          }
        }
        else {
          this.setOptionValueWithSource('categoryField', [], 'cli')
        }
        csvSourceStream.resume()
      }

      if (applyFilters(options)(chunk.record))
        callback(null, chunk.record)
      else callback(null, null)
    })

    csvSourceStream.on('readable', async () => {
      let chunk: null | {
        info: Info & CastingContext
        record: Record<string, JsonPrimitive>
        raw: string
      }

      while ((chunk = csvSourceStream.read()) !== null) {
        if (typeof chunk === 'string' || skippableLines.includes(chunk.raw) || rawHeaderLine === chunk.raw || typeof chunk.raw === 'undefined') {
          totalSkippedRecords++
        }

        // const rowArray = get(chunk, 'record', []).map(name => name!.trim())
        // // const columns = get(chunk.info, 'columns', [])

        // if (isEmpty(fields) && get(options, 'rangeIncludesHeader', false)) {
        //   rawHeaderLine = chunk.raw
        //   if (options.rangeIncludesHeader && isArray(chunk.record)) {
        //     fields = formatHeaderValues({ data: rowArray })
        //   }
        //   else if (isArray(rowArray)) {
        //     fields = rowArray.map((_, i) => `Column ${i + 1}`)
        //   }
        // }

        if (get(chunk, 'info.lines'))
          totalParsedLines = chunk.info.lines

        if (get(chunk, 'info.records'))
          totalParsedRecords = chunk.info.records

        if (get(chunk, 'info.error')) {
          if (isUndefined(confirmQuitChoice) && chunk.info.error.code === 'CSV_RECORD_INCONSISTENT_FIELDS_LENGTH') {
            const parsingErrorMessage = `Error parsing line ${chalk.bold.redBright(numbro(chunk.info.lines).format({ thousandSeparated: true }))} of ${chalk.bold.redBright(basename(options.filePath))}: ${chalk.italic.redBright(chunk.info?.error?.message)}`

            spinner.warn(parsingErrorMessage)

            const [, confirmQuit] = await tryPrompt('confirm', {
              message: 'Would you like to quit parsing the file?',
              default: false,
            })

            if (confirmQuit === true) {
              spinner.fail(chalk.redBright(`Quitting; consider re-running the program with the --from-line option set to ${chunk.info.lines} to set the header to line ${numbro(chunk.info.lines).format({ thousandSeparated: true })}`))
              process.exit(1)
            }
            confirmQuitChoice = false
          }
          else if (!skippableLines.includes(chunk.raw)) {
            const [, isSkippableLine] = await tryPrompt('confirm', {
              message: `Is this line skippable?\nLINE: ${chalk.yellowBright(JSON.stringify(chunk.raw))}`,
              default: true,
            })

            if (isSkippableLine === true) {
              skippableLines.push(chunk.raw)
            }
          }
          else if (rawPartialLine.line === 0) {
            rawPartialLine.line = get(chunk, 'info.lines', 0)
            rawPartialLine.row += get(chunk, 'raw', '')
          }
          else if ((rawPartialLine.line + 1) === get(chunk, 'info.lines', 0)) {
            rawPartialLine.row += chunk.raw

            const fullRoawString = rawPartialLine.row

            // skippableLines.push(rawPartialLine.row)
            rawPartialLine.line = 0
            rawPartialLine.row = ''
            csvSourceStream.unshift(fullRoawString.split(get(options, 'delimiter', ',')).map(v => `"${v}"`)
              .join(get(options, 'delimiter', ',')))
          }
          else {
            perLineTransformStream.push(chunk)
          }
        }
        else {
          perLineTransformStream.push(chunk)
        }
      }
    })
    readStream.pipe(csvSourceStream)

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

    const commandLineString = generateCommandLineString(options, this)

    fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), stringifyCommandOptions(options, commandLineString))
    parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
    fs.ensureDirSync(parsedOutputFile.dir)
    spinner.text = (chalk.magentaBright(`PARSING "${filename(options.filePath)}" with the selected options`))

    const fileCategoryObject: Record<string, FileMetrics[]> = {}

    const columns = concat(fields, ['source_file', 'from_line', 'record_count'])

    const headerLine = stringify([columns])

    fs.writeFileSync(join(parsedOutputFile.dir, '..', `${parsedOutputFile.name} HEADER.csv`), headerLine, 'utf8')

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
      // readStream,
      // csvSourceStream,
      perLineTransformStream,
      categoryStream,
      async (err) => {
        if (err) {
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

          const table = new Table(fileObjectArray, {
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
                get: cellValue => '',
              },
              {
                name: 'stream',
                get: cellValue => '',
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

          const formattedTotalSkippedRecords = numbro(totalSkippedRecords).format({ thousandSeparated: true })

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
              if (k === 'CATEGORY') {
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

          fs.outputFileSync(join(parsedOutputFile.dir, '..', `PARSE AND SPLIT OUTPUT FILES.csv`), parseResultsCsv)
          fs.outputFileSync(join(parsedOutputFile.dir, '..', `PARSE AND SPLIT SUMMARY.yaml`), summaryString)

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
