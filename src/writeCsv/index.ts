import type { ParsedPath } from 'node:path'
import type { Readable } from 'node:stream'
import type { Ora } from 'ora'
import type {
  JsonObject,
  JsonPrimitive,
} from 'type-fest'
import type {
  CSVOptionsWithGlobals,
  ExcelOptionsWithGlobals,
  FileMetrics,

} from '../types'
import { createWriteStream } from 'node:fs'
import {
  readFile,
  writeFile,
} from 'node:fs/promises'
import { objectEntries } from '@antfu/utils'
import chalk from 'chalk'
import fs from 'fs-extra'
import {
  findLastIndex,
  get,
  isEmpty,
  isNull,
  isNumber,
  isObjectLike,
  isString,
  isUndefined,
  maxBy,
  padStart,
  sumBy,
  toInteger,
} from 'lodash-es'
import numbro from 'numbro'
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
import {
  createCsvFileName,
  createHeaderFile,
  formatHeaderValues,
  writeToActiveStream,
} from '../helpers'

export default async function<Options extends ExcelOptionsWithGlobals | CSVOptionsWithGlobals>(inputFile: Readable, commandOptions: Options, options: {
  parsedOutputFile: Omit<ParsedPath, 'base'>
  skippedLines: number | undefined
  bytesRead: number | undefined
  spinner: Ora
  files: FileMetrics[]
  fields: string[]
  parsedLines: number
}): Promise<void> {
  // let spinner: Ora

  // let files: Array<FileMetrics>

  // let fields: string[]

  // const skippedLines = 'skippedLines' in options ? options.skippedLines : 0

  // const bytesRead = options.bytesRead

  // let parsedLines: number

  Papa.parse<JsonPrimitive[] | JsonObject >(inputFile, {
    async step(results, _parser) {
      // if (get(options, 'rowCount') === parsedLines) {
      //   parser.abort()
      // }

      // else
      if (!(options.fields ?? []).length && commandOptions.rangeIncludesHeader === true && Array.isArray(results.data)) {
        createHeaderFile(options, results)
      }
      else if ((Array.isArray(results.data) && options.fields.length && results.data.length !== options.fields.length) || results.errors.length) {
        options.skippedLines = (options.skippedLines ?? 0) + 1
      }
      else {
        options.parsedLines = (options.parsedLines ?? 0) + 1

        const {
          isUnfiltered,
          thisRow,
        } = filterData(results, options, commandOptions)

        if (isUnfiltered) {
          if (isString(commandOptions.categoryField) && !Array.isArray(thisRow)) {
            const rawCategory = get(thisRow, commandOptions.categoryField, undefined)

            if (isUndefined(rawCategory))
              options.category = undefined
            else if (typeof rawCategory === 'string')
              options.category = isEmpty(rawCategory) ? 'EMPTY' : rawCategory
            else if (isNull(rawCategory))
              options.category = 'NULL'
            else if (isNumber(rawCategory))
              options.category = toInteger(rawCategory).toString()
            else options.category = `${rawCategory}`
          }

          const csvOutput = Papa.unparse([results.data])

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
          if ((options.parsedLines % 1000) === 0 && options.parsedLines > 0) {
            const totalRows = sumBy(options.files, 'ROWS')

            options.spinner.text = chalk.magentaBright(`PARSED ${numbro(options.parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `) + chalk.yellow(`WRITING "${parse(options.files[activeFileIndex].PATH).base};"`)
          }
        }
      }
    },
    complete: async () => {
      for (const file of options.files) {
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

      const maxFileNumLength = `${maxBy(options.files.filter(o => typeof o.FILENUM !== 'undefined'), 'FILENUM')?.FILENUM ?? ''}`.length

      const parseResults = alphabetical(options.files, o => o.FILENUM ? `${o.CATEGORY}${padStart(`${o.FILENUM}`, maxFileNumLength, '0')}` : o.CATEGORY ?? o.PATH).map(o => pick(o, ['CATEGORY', 'ROWS', 'BYTES', 'PATH']))

      const parseOutputs = options.files.map((o) => {
        return mapValues(shake(omit(o, ['stream']), v => isUndefined(v)), v => isObjectLike(v) ? !isEmpty(v) ? yaml.stringify(v) : undefined : v)
      })

      const parseResultsCsv = Papa.unparse(parseOutputs)

      const totalRows = sumBy(options.files, 'ROWS')

      const totalBytes = sumBy(options.files, 'BYTES')

      // spinner.text = chalk.magentaBright(`PARSED ${numbro(parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `)

      const totalFiles = options.files.length

      const summaryString = yaml.stringify({
        'TOTAL NON-HEADER ROWS PARSED': numbro(options.parsedLines).format({ thousandSeparated: true }),
        'TOTAL NON-HEADER ROWS SKIPPED': numbro(options.skippedLines).format({ thousandSeparated: true }),
        'TOTAL NON-HEADER ROWS WRITTEN': numbro(totalRows).format({ thousandSeparated: true }),
        'TOTAL BYTES READ': numbro(options.bytesRead).format({
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

      fs.outputFileSync(join(options.parsedOutputFile.dir, '..', `PARSE AND SPLIT OUTPUT FILES.csv`), parseResultsCsv)
      fs.outputFileSync(join(options.parsedOutputFile.dir, '..', `PARSE AND SPLIT SUMMARY.yaml`), summaryString)
      if (commandOptions.header) {
        for (const file of options.files) {
          const header = Papa.unparse([formatHeaderValues({ data: options.fields })])

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
            get: cellValue => chalk.cyan(join('../', relative(parse(commandOptions.filePath).dir, cellValue as string))),
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

      const formattedParsedLines = numbro(options.parsedLines).format({ thousandSeparated: true })

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

      if (commandOptions.header)
        spinnerText += chalk.yellow(`THE HEADER IS WRITTEN TO EACH FILE\n`)
      else spinnerText += chalk.yellow(`THE HEADER FOR ALL FILES IS ${chalk.cyan(`"${parse(join(options.parsedOutputFile.dir, `${options.parsedOutputFile.name} HEADER.csv`)).base}"`)}\n`)

      options.spinner.stopAndPersist({
        symbol: '🚀',
        text: `${spinnerText}\n${table.toString()}`,
      })
      process.exit()
    },
    error(error, _file) {
      options.spinner.fail(chalk.red('FAILED TO PARSE FILES\n') + error)
    },

  })
}
export function filterData(results: Papa.ParseStepResult<JsonPrimitive[] | JsonObject>, options: {
  parsedOutputFile: Omit<ParsedPath, 'base'>
  skippedLines: number | undefined
  bytesRead: number | undefined
  spinner: Ora
  files: FileMetrics[]
  fields: string[]
  parsedLines: number
}, commandOptions): {
    isUnfiltered: boolean
    thisRow: Record<string, JsonPrimitive> | JsonPrimitive[]
  } {
  const thisRow = Array.isArray(results.data) && options.fields.length ? zipToObject(options.fields, results.data) : results.data

  const filtersArray = objectEntries(commandOptions.rowFilters ?? {}) as Array<[string, JsonPrimitive[]]>

  let isUnfiltered = isEmpty(commandOptions.rowFilters ?? {}) || Array.isArray(thisRow)

  if (!isUnfiltered && !Array.isArray(thisRow)) {
    if (commandOptions.matchType === 'none') {
      isUnfiltered = filtersArray.every(([field, value]) => !value.includes(thisRow[field]))
    }
    else if (commandOptions.matchType === 'any') {
      isUnfiltered = filtersArray.some(([field, value]) => value.includes(thisRow[field]))
    }
    else {
      isUnfiltered = filtersArray.every(([field, value]) => value.includes(thisRow[field]))
    }
  }

  return {
    isUnfiltered,
    thisRow,
  }
}
export function handleCsvOutput(options: {
  parsedOutputFile: Omit<ParsedPath, 'base'>
  skippedLines: number | undefined
  bytesRead: number | undefined
  spinner: Ora
  files: FileMetrics[]
  fields: string[]
  parsedLines: number
}, commandOptions, csvOutput: string) {
  let activeFileIndex = options.files.length === 0 ? -1 : options.category ? findLastIndex(options.files, { CATEGORY: options.category }) : (options.files.length - 1)

  if (activeFileIndex === -1) {
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
    activeFileIndex = options.files.length - 1
    writeToActiveStream(activeFileObject.PATH, csvOutput, options)

    const totalRows = sumBy(options.files, 'ROWS')

    options.spinner.text = chalk.magentaBright(`PARSED ${numbro(options.parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `) + chalk.yellow(`CREATED "${filename(outputFilePath)};"`)
    // await new Promise(resolve => delay(() => resolve(parser.resume()), 500))
  }
  else if (activeFileIndex > -1 && !isUndefined(commandOptions.fileSize) && isNumber(commandOptions.fileSize) && (options.files[activeFileIndex].BYTES + Buffer.from(csvOutput).length) > (commandOptions.fileSize * 1024 * 1024)) {
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
  if ((options.parsedLines % 1000) === 0 && options.parsedLines > 0) {
    const totalRows = sumBy(options.files, 'ROWS')

    options.spinner.text = chalk.magentaBright(`PARSED ${numbro(options.parsedLines).format({ thousandSeparated: true })} LINES; `) + chalk.greenBright(`WROTE ${numbro(totalRows).format({ thousandSeparated: true })} LINES; `) + chalk.yellow(`WRITING "${parse(options.files[activeFileIndex].PATH).base};"`)
  }
}
