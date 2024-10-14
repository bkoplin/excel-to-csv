import './table-layout.d'
import { createWriteStream } from 'node:fs'
import {
  appendFile,
  readFile,
  writeFile,
} from 'node:fs/promises'
import type { Readable } from 'node:stream'
import {
  format,
  join,
  parse,
  relative,
} from 'pathe'
import { filename } from 'pathe/utils'
import Table from 'table-layout'
import numbro from 'numbro'
import {
  alphabetical,
  isPrimitive,
  pick,
  zipToObject,
} from 'radash'
import {
  camelCase,
  delay,
  findIndex,
  findLast,
  isEmpty,
  isNil,
  isNull,
  isUndefined,
  last,
  maxBy,
  noop,
  padStart,
  sumBy,
  upperFirst,
} from 'lodash-es'
import Papa from 'papaparse'
import type { JsonPrimitive } from 'type-fest'
import ora from 'ora'
import picocolors from 'picocolors'
import yaml from 'yaml'
// import type { CommandOptions } from '..'
import { objectEntries } from '@antfu/utils'
import fs from 'fs-extra'
import type { FileMetrics } from './types'
import type { GlobalOptions } from '@'

// interface CommandOptions {
//   rowFilters?: true | Record<string, JsonPrimitive[]> | undefined
//   categoryField?: string | true | undefined
//   matchType: true | 'all' | 'any' | 'none'
//   fileSize?: number | undefined
//   header: boolean
//   inputFilePath: string
//   range: string
//   rangeIncludesHeader: boolean
// }

export default async function<Options extends GlobalOptions>(inputFile: Readable, options: Options): Promise<void> {
  const splitOptions = yaml.stringify(options)
  const {
    inputFilePath,
    categoryField = '',
    fileSize: maxFileSizeInMb,
    matchType,
    rowFilters: filters = {},
    parsedOutputFile,
  } = options
  // const filters = options.filters ?? []
  const writeHeaderOnEachFile = options.header
  const spinner = ora({
    hideCursor: false,
    discardStdin: false,
  })
  const files: Array<FileMetrics> = []
  const parsedInputFile = parse(inputFilePath)
  spinner.start(`Parsing ${picocolors.cyan(filename(inputFilePath))}`)
  // ensureDirSync(parsedOutputFile.dir)

  // const reader = createReadStream(inputFilePath, 'utf-8')
  let fields: string[] = []
  const headerFilePath = join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`)
  const headerFile = createWriteStream(headerFilePath, 'utf-8')
  let parsedLines = 0
  Papa.parse<JsonPrimitive[]>(inputFile, {
    async step(results, parser) {
      parser.pause()
      if (headerFile.writable && Array.isArray(results.data) && !fields.length) {
        fields = results.data as string[]
        headerFile.end(Papa.unparse([formatHeaderValues(results)]))
      }
      else {
        const thisRow = zipToObject(fields, results.data)
        parsedLines++
        const filtersArray = objectEntries(filters) as Array<[string, JsonPrimitive[]]>
        if (isEmpty(filters) || filtersArray.every(([field, value]) => value.includes(thisRow[field]) && matchType === 'all') || filtersArray.some(([field, value]) => value.includes(thisRow[field]) && matchType === 'any') || filtersArray.every(([field, value]) => !value.includes(thisRow[field]) && matchType === 'none')) {
          const csvOutput = Papa.unparse([results.data])
          const csvRowLength = Buffer.from(csvOutput).length
          let category: string | undefined
          const rawCategory = categoryField in thisRow ? thisRow[categoryField as string] : undefined
          if (isPrimitive(rawCategory) && !isEmpty(categoryField))
            category = isEmpty(rawCategory) ? 'EMPTY' : JSON.stringify(rawCategory)
          else if (isNull(rawCategory) && !isEmpty(categoryField))
            category = 'NULL'
          else
            category = undefined
          // const category = isString(thisRow[categoryField as string]) ? isEmpty(thisRow[categoryField as string]) ? 'EMPTY' : thisRow[categoryField as string] : isNull(thisRow[categoryField as string]) ? 'NULL' : undefined
          let activeFileObject = (isNil(category) ? last(files) : findLast(files, a => a.CATEGORY === category))

          if (isUndefined(activeFileObject)) {
            const defaultFileNumber = (maxFileSizeInMb ? 1 : undefined)
            const defaultCsvFileName = generateCsvFileName({
              fileNumber: defaultFileNumber,
              category,
            })
            activeFileObject = {
              BYTES: csvRowLength,
              FILENUM: (maxFileSizeInMb ? 1 : undefined),
              ROWS: 1,
              CATEGORY: category!,
              PATH: format({
                ...parsedOutputFile,
                name: generateCsvFileName({
                  fileNumber: defaultFileNumber,
                  category,
                }),
              }),
            }
            spinner.text = `CREATED ${picocolors.yellow(`"${defaultCsvFileName}"`)}`
            await appendFile(activeFileObject.PATH, `${csvOutput}\n`, { encoding: 'utf-8' })
            files.push(activeFileObject)
          }
          else if (!isUndefined(activeFileObject) && !isUndefined(maxFileSizeInMb) && (activeFileObject.BYTES + csvRowLength) > (maxFileSizeInMb * 1024 * 1024)) {
            spinner.text = `FINISHED WITH ${picocolors.yellow(`"${filename(activeFileObject.PATH)}"`)}`
            await delay(noop, 1500)
            const newActiveFileObject = {
              BYTES: csvRowLength,
              FILENUM: activeFileObject.FILENUM! + 1,
              ROWS: 1,
              PATH: format({
                ...parsedOutputFile,
                name: generateCsvFileName({
                  fileNumber: activeFileObject.FILENUM! + 1,
                  category,
                }),
              }),
              CATEGORY: category!,
            }
            await appendFile(newActiveFileObject.PATH, `${csvOutput}\n`, { encoding: 'utf-8' })
            files.push(newActiveFileObject)
          }
          else {
            activeFileObject.BYTES += csvRowLength
            activeFileObject.ROWS += 1
            const currentFileIndex = findIndex(files, { PATH: activeFileObject.PATH })
            files[currentFileIndex] = activeFileObject
            spinner.text = `WRITING ${picocolors.yellow(`"${parse(activeFileObject.PATH).base}"`)}`
            await appendFile(activeFileObject.PATH, `${csvOutput}\n`, { encoding: 'utf-8' })
          }
        }
      }
      parser.resume()
    },
    complete: async () => {
      const maxFileNumLength = `${maxBy(files.filter(o => typeof o.FILENUM !== 'undefined'), 'FILENUM')?.FILENUM ?? ''}`.length
      const parseResults = alphabetical(files, o => o.FILENUM ? `${o.CATEGORY}${padStart(`${o.FILENUM}`, maxFileNumLength, '0')}` : o.CATEGORY ?? o.PATH).map(o => pick(o, ['CATEGORY', 'ROWS', 'BYTES', 'PATH']))
      const totalRows = sumBy(files, 'ROWS')
      const totalBytes = sumBy(files, 'BYTES')
      const totalFiles = files.length
      const summaryString = yaml.stringify({
        'TOTAL ROWS': totalRows,
        'TOTAL BYTES': totalBytes,
        'TOTAL FILES': totalFiles,
        'OUTPUT FILES': parseResults,
      })
      fs.outputFileSync(join(parsedOutputFile.dir, '..', `PARSE AND SPLIT SUMMARY.yaml`), summaryString)
      if (writeHeaderOnEachFile) {
        for (const file of files) {
          const header = Papa.unparse([{ data: formatHeaderValues(fields) }])
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
            get: cellValue => cellValue === undefined ? '' : picocolors.bold(picocolors.yellow(cellValue)),
          },
          {
            name: 'PATH',
            get: cellValue => picocolors.cyan(join('../', relative(parsedInputFile.dir, cellValue as string))),
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
      let spinnerText = `SUCCESSFULLY PARSED ${picocolors.green(formattedParsedLines)} LINES INTO ${picocolors.green(formattedTotalRows)} LINES ACROSS ${picocolors.green(formattedTotalFiles)} FILES OF TOTAL SIZE ${picocolors.green(formattedTotalBytes)}\n`

      if (writeHeaderOnEachFile)
        spinnerText += picocolors.yellow(`THE HEADER IS WRITTEN TO EACH FILE\n`)
      else spinnerText += picocolors.yellow(`THE HEADER FOR ALL FILES IS ${picocolors.cyan(`"${parse(headerFilePath).base}"`)}\n`)
      spinner.succeed(`${spinnerText}\n${table.toString()}`)
      process.exit()
    },
    error(error, file) {
      spinner.fail(picocolors.red('FAILED TO PARSE FILES\n') + error)
    },

    header: false,
    transform: value => value.trim() === '' ? null : value.trim(),
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
      csvFileName += ` ${upperFirst(camelCase(category))}`
    if (typeof fileNumber !== 'undefined')
      csvFileName += ` ${padStart(`${fileNumber}`, 4, '0')}`

    return csvFileName
  }
}
function formatHeaderValues(results: Papa.ParseStepResult<string[]>): string[] {
  return results.data.map((value, index, self) => {
    const occurrencesAfter = self.slice(index + 1).filter(v => v === value).length
    const occurrencesBefore = self.slice(0, index).filter(v => v === value).length + 1
    return (occurrencesAfter + occurrencesBefore) > 1 ? `${value}_${occurrencesBefore}` : value
  })
}
