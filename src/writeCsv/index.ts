import type { Readable } from 'node:stream'
import type { JsonPrimitive } from 'type-fest'
import type {
  CSVOptionsWithGlobals,
  ExcelOptionsWithGlobals,
} from '../index'
import type { FileMetrics } from './types'
import { createWriteStream } from 'node:fs'
import {
  readFile,
  writeFile,
} from 'node:fs/promises'
import { objectEntries } from '@antfu/utils'
import chalk from 'chalk'
import filenamify from 'filenamify'
import fs from 'fs-extra'
import {
  delay,
  findIndex,
  findLast,
  isBoolean,
  isEmpty,
  isNil,
  isNull,
  isNumber,
  isObjectLike,
  isString,
  isUndefined,
  last,
  maxBy,
  padStart,
  sumBy,
  toInteger,
} from 'lodash-es'
import numbro from 'numbro'
import ora from 'ora'
import Papa from 'papaparse'
import {
  format,
  join,
  parse,
  relative,
} from 'pathe'
import { filename } from 'pathe/utils'
import {
  alphabetical,
  mapValues,
  omit,
  pick,
  shake,
  zipToObject,
} from 'radash'
import Table from 'table-layout'
import yaml from 'yaml'

export default async function<Options extends ExcelOptionsWithGlobals | CSVOptionsWithGlobals>(inputFile: Readable, options: Options): Promise<void> {
  const {
    filePath: inputFilePath,
    categoryField = '',
    fileSize: maxFileSizeInMb,
    matchType,
    rowFilters: filters = {},
    parsedOutputFile,
    intputFields = [],
  } = options

  const writeHeaderOnEachFile = options.header

  const spinner = ora({
    hideCursor: false,
    discardStdin: false,
  })

  const files: Array<FileMetrics> = []

  const parsedInputFile = parse(inputFilePath)

  spinner.start(`Parsing ${chalk.cyan(filename(inputFilePath))}`)

  let fields: string[] = []

  const headerFilePath = join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`)

  let skippedLines = 'skippedLines' in options ? options.skippedLines : 0

  const bytesRead = options.bytesRead

  let parsedLines = 0

  const tryCategory = typeof categoryField === 'string' && categoryField.length === 0

  Papa.parse<JsonPrimitive[] >(inputFile, {
    async step(results, parser) {
      if ('rowCount' in options && options.rowCount === parsedLines) {
        parser.abort()
      }

      else if (!fields.length && options.rangeIncludesHeader === true && Array.isArray(results.data)) {
        const headerFile = createWriteStream(headerFilePath, 'utf-8')

        fields = formatHeaderValues({ data: results.data })
        headerFile.end(Papa.unparse([fields]))
      }
      else if (Array.isArray(results.data) && fields.length && results.data.length !== fields.length) {
        skippedLines++
      }
      else if (results.errors.length) {
        skippedLines++
      }

      else {
        const thisRow = Array.isArray(results.data) ? fields.length ? zipToObject(fields, results.data) : results.data : results.data

        parsedLines++

        const filtersArray = objectEntries(filters) as Array<[string, JsonPrimitive[]]>

        let filterTest = isEmpty(filters) || Array.isArray(thisRow)

        if (!filterTest && !Array.isArray(thisRow)) {
          if (matchType === 'none') {
            filterTest = filtersArray.every(([field, value]) => !value.includes(thisRow[field]))
          }
          else if (matchType === 'any') {
            filterTest = filtersArray.some(([field, value]) => value.includes(thisRow[field]))
          }
          else {
            filterTest = filtersArray.every(([field, value]) => value.includes(thisRow[field]))
          }
        }

        let category: string | undefined

        if (filterTest) {
          if (!isEmpty(categoryField) && !isUndefined(categoryField) && !isNull(categoryField) && !isBoolean(categoryField)) {
            const rawCategory = !Array.isArray(thisRow) ? categoryField in thisRow ? thisRow[categoryField as string] : undefined : undefined

            if (isUndefined(rawCategory))
              category = undefined
            else if (isString(rawCategory))
              category = isEmpty(rawCategory) ? 'EMPTY' : rawCategory
            else if (isNull(rawCategory))
              category = 'NULL'
            else if (isNumber(rawCategory))
              category = toInteger(rawCategory).toString()
            else category = `${rawCategory}`
          }

          let activeFileObject = (isNil(category) ? last(files) : findLast(files, a => a.CATEGORY === category))

          const csvOutput = Papa.unparse([results.data])

          const csvRowLength = Buffer.from(csvOutput).length

          if (isUndefined(activeFileObject)) {
            const defaultFileNumber = (maxFileSizeInMb ? 1 : undefined)

            const outputFilePath = format({
              ...parsedOutputFile,
              name: generateCsvFileName({
                fileNumber: defaultFileNumber,
                category,
              }),
            })

            const stream = createWriteStream(outputFilePath, 'utf-8')

            // stream.on('finish', () => {
            //   parser.pause()

            //   const totalRows = sumBy(files, 'ROWS')

            //   spinner.text = chalk.magentaBright(`PARSED ${numbro(parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `) + chalk.yellow(`FINISHED WITH "${filename(outputFilePath)}"`)
            //   delay(() => parser.resume(), 500)
            // })
            if (stream.write(`${csvOutput}\n`)) {
              parser.pause()
              stream.once('drain', () => {
                parser.resume()
              })
            }
            activeFileObject = {
              BYTES: csvRowLength,
              FILENUM: (maxFileSizeInMb ? 1 : undefined),
              ROWS: 1,
              CATEGORY: category,
              FILTER: filters,
              PATH: outputFilePath,
              stream,
            }
            parser.pause()

            const totalRows = sumBy(files, 'ROWS')

            spinner.text = chalk.magentaBright(`PARSED ${numbro(parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `) + chalk.yellow(`CREATED "${filename(outputFilePath)};"`)
            files.push(activeFileObject)
            await new Promise(resolve => delay(() => resolve(parser.resume()), 500))
          }
          else if (!isUndefined(activeFileObject) && !isUndefined(maxFileSizeInMb) && isNumber(maxFileSizeInMb) && (activeFileObject.BYTES + csvRowLength) > (maxFileSizeInMb * 1024 * 1024)) {
            if (activeFileObject.stream?.writableNeedDrain) {
              activeFileObject.stream.once('drain', () => {
                activeFileObject!.stream!.close()
              })
            }
            else {
              activeFileObject.stream!.close()
            }

            const outputFilePath = format({
              ...parsedOutputFile,
              name: generateCsvFileName({
                fileNumber: activeFileObject.FILENUM! + 1,
                category,
              }),
            })

            const stream = createWriteStream(outputFilePath, 'utf-8')

            // stream.on('finish', () => {
            //   parser.pause()

            //   const totalRows = sumBy(files, 'ROWS')

            //   spinner.text = chalk.magentaBright(`PARSED ${numbro(parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `) + chalk.yellow(`FINISHED WITH "${filename(outputFilePath)}"`)
            //   delay(() => parser.resume(), 500)
            // })
            if (stream.write(`${csvOutput}\n`)) {
              parser.pause()
              stream.once('drain', () => {
                parser.resume()
              })
            }

            const newActiveFileObject = {
              BYTES: csvRowLength,
              FILENUM: activeFileObject.FILENUM! + 1,
              ROWS: 1,
              FILTER: filters,
              PATH: outputFilePath,
              CATEGORY: category!,
              stream,
            }

            files.push(newActiveFileObject)
          }
          else {
            activeFileObject.BYTES += csvRowLength
            activeFileObject.ROWS += 1

            const currentFileIndex = findIndex(files, { PATH: activeFileObject.PATH })

            files[currentFileIndex] = activeFileObject
            if (!activeFileObject.stream!.write(`${csvOutput}\n`)) {
              parser.pause()
              activeFileObject.stream!.once('drain', () => {
                parser.resume()
              })
            }
          }
          if ((parsedLines % 1000) === 0 && parsedLines > 0) {
            const totalRows = sumBy(files, 'ROWS')

            spinner.text = chalk.magentaBright(`PARSED ${numbro(parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `) + chalk.yellow(`WRITING "${parse(activeFileObject.PATH).base};"`)
          }
        }
      }
    },
    complete: async () => {
      for (const file of files) {
        const stream = file.stream

        if (typeof stream !== 'undefined') {
          await new Promise((resolve) => {
            stream.once('finish', () => {
              resolve(true)
            })
            if (stream.writableNeedDrain === true) {
              stream.once('drain', () => {
                stream.end()
              })
            }
            else {
              stream.end()
            }
          })
        }
      }

      const maxFileNumLength = `${maxBy(files.filter(o => typeof o.FILENUM !== 'undefined'), 'FILENUM')?.FILENUM ?? ''}`.length

      const parseResults = alphabetical(files, o => o.FILENUM ? `${o.CATEGORY}${padStart(`${o.FILENUM}`, maxFileNumLength, '0')}` : o.CATEGORY ?? o.PATH).map(o => pick(o, ['CATEGORY', 'ROWS', 'BYTES', 'PATH']))

      const parseOutputs = files.map((o) => {
        return mapValues(shake(omit(o, ['stream']), v => isUndefined(v)), v => isObjectLike(v) ? !isEmpty(v) ? yaml.stringify(v) : undefined : v)
      })

      const parseResultsCsv = Papa.unparse(parseOutputs)

      const totalRows = sumBy(files, 'ROWS')

      const totalBytes = sumBy(files, 'BYTES')

      // spinner.text = chalk.magentaBright(`PARSED ${numbro(parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `)

      const totalFiles = files.length

      const summaryString = yaml.stringify({
        'TOTAL NON-HEADER ROWS PARSED': numbro(parsedLines).format({ thousandSeparated: true }),
        'TOTAL NON-HEADER ROWS SKIPPED': numbro(skippedLines).format({ thousandSeparated: true }),
        'TOTAL NON-HEADER ROWS WRITTEN': numbro(totalRows).format({ thousandSeparated: true }),
        'TOTAL BYTES READ': numbro(bytesRead).format({
          output: 'byte',
          spaceSeparated: true,
          base: 'binary',
          average: true,
          mantissa: 2,
          optionalMantissa: true,
        }),
        'TOTAL BYTES WRITTEN': numbro(totalBytes).format({
          output: 'byte',
          spaceSeparated: true,
          base: 'binary',
          average: true,
          mantissa: 2,
          optionalMantissa: true,
        }),
        'TOTAL FILES': numbro(totalFiles).format({ thousandSeparated: true }),

      })

      fs.outputFileSync(join(parsedOutputFile.dir, '..', `PARSE AND SPLIT OUTPUT FILES.csv`), parseResultsCsv)
      fs.outputFileSync(join(parsedOutputFile.dir, '..', `PARSE AND SPLIT SUMMARY.yaml`), summaryString)
      if (writeHeaderOnEachFile) {
        for (const file of files) {
          const header = Papa.unparse([formatHeaderValues({ data: fields })])

          const openFile = await readFile(file.PATH, 'utf-8')

          await writeFile(file.PATH, `${header}\n${openFile}`, 'utf-8')
        }
      }

      const table = new Table(parseResults, {
        maxWidth: 600,
        ignoreEmptyColumns: true,
        columns: [
          {
            name: 'CATEGORY',
            get: cellValue => cellValue === undefined ? '' : chalk.bold(chalk.yellow(cellValue)),
          },
          {
            name: 'PATH',
            get: cellValue => chalk.cyan(join('../', relative(parsedInputFile.dir, cellValue as string))),
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

      const formattedParsedLines = numbro(parsedLines).format({ thousandSeparated: true })

      const formattedTotalRows = numbro(totalRows).format({ thousandSeparated: true })

      const formattedTotalFiles = numbro(totalFiles).format({ thousandSeparated: true })

      const formattedTotalBytes = numbro(totalBytes).format({
        output: 'byte',
        spaceSeparated: true,
        base: 'general',
        average: true,
        mantissa: 2,
        optionalMantissa: true,
      })

      let spinnerText = `SUCCESSFULLY PARSED ${chalk.green(formattedParsedLines)} LINES INTO ${chalk.green(formattedTotalRows)} LINES ACROSS ${chalk.green(formattedTotalFiles)} FILES OF TOTAL SIZE ${chalk.green(formattedTotalBytes)}\n`

      if (writeHeaderOnEachFile)
        spinnerText += chalk.yellow(`THE HEADER IS WRITTEN TO EACH FILE\n`)
      else spinnerText += chalk.yellow(`THE HEADER FOR ALL FILES IS ${chalk.cyan(`"${parse(headerFilePath).base}"`)}\n`)

      spinner.stopAndPersist({
        symbol: '🚀',
        text: `${spinnerText}\n${table.toString()}`,
      })
      process.exit()
    },
    error(error, _file) {
      spinner.fail(chalk.red('FAILED TO PARSE FILES\n') + error)
    },

  })
  function generateCsvFileName({
    fileNumber,
    category,
  }: {
    fileNumber?: number
    category?: string | null
  } = {}): string {
    let csvFileName = parsedOutputFile.name

    if (typeof category !== 'undefined' && category !== null)
      csvFileName += ` ${category}`

    if (typeof fileNumber !== 'undefined')
      csvFileName += ` ${padStart(`${fileNumber}`, 4, '0')}`

    return filenamify(csvFileName, { replacement: '_' })
  }
}
function formatHeaderValues(results: { data: JsonPrimitive[] }): string[] {
  return results.data.map((value, index, self) => {
    const occurrencesAfter = self.slice(index + 1).filter(v => v === value).length

    const occurrencesBefore = self.slice(0, index).filter(v => v === value).length + 1

    return (occurrencesAfter + occurrencesBefore) > 1 ? `${value}_${occurrencesBefore}` : `${value}`
  })
}
