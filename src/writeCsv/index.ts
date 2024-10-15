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
  mapValues,
  pick,
  shake,
  zipToObject,
} from 'radash'
import {
  camelCase,
  findIndex,
  findLast,
  isEmpty,
  isNil,
  isNull,
  isObjectLike,
  isUndefined,
  last,
  maxBy,
  padStart,
  sumBy,
  upperFirst,
} from 'lodash-es'
import Papa from 'papaparse'
import type { JsonPrimitive } from 'type-fest'
import ora from 'ora'
import chalk from 'chalk'
import yaml from 'yaml'
import { objectEntries } from '@antfu/utils'
import fs from 'fs-extra'
import type { FileMetrics } from './types'
import type { GlobalOptions } from '@'

export default async function<Options extends GlobalOptions>(inputFile: Readable, options: Options): Promise<void> {
  const {
    inputFilePath,
    categoryField = '',
    fileSize: maxFileSizeInMb,
    matchType,
    rowFilters: filters = {},
    parsedOutputFile,
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
        if (isEmpty(filters) || filtersArray.every(([field, value]) => (value.includes(thisRow[field]) && matchType === 'all') || (!value.includes(thisRow[field]) && matchType === 'none')) || filtersArray.some(([field, value]) => value.includes(thisRow[field]) && matchType === 'any')) {
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
              CATEGORY: category,
              FILTER: filters,
              PATH: format({
                ...parsedOutputFile,
                name: generateCsvFileName({
                  fileNumber: defaultFileNumber,
                  category,
                }),
              }),
            }
            spinner.text = chalk.yellow(`CREATED "${defaultCsvFileName}"`)
            await appendFile(activeFileObject.PATH, `${csvOutput}\n`, { encoding: 'utf-8' })
            files.push(activeFileObject)
          }
          else if (!isUndefined(activeFileObject) && !isUndefined(maxFileSizeInMb) && (activeFileObject.BYTES + csvRowLength) > (maxFileSizeInMb * 1024 * 1024)) {
            spinner.text = chalk.yellow(`FINISHED WITH "${filename(activeFileObject.PATH)}"`)
            // await delay(noop, 1500)
            const newActiveFileObject = {
              BYTES: csvRowLength,
              FILENUM: activeFileObject.FILENUM! + 1,
              ROWS: 1,
              FILTER: filters,
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
            spinner.text = chalk.yellow(`WRITING "${parse(activeFileObject.PATH).base}"`)
            await appendFile(activeFileObject.PATH, `${csvOutput}\n`, { encoding: 'utf-8' })
          }
        }
      }
      parser.resume()
    },
    complete: async () => {
      const maxFileNumLength = `${maxBy(files.filter(o => typeof o.FILENUM !== 'undefined'), 'FILENUM')?.FILENUM ?? ''}`.length
      const parseResults = alphabetical(files, o => o.FILENUM ? `${o.CATEGORY}${padStart(`${o.FILENUM}`, maxFileNumLength, '0')}` : o.CATEGORY ?? o.PATH).map(o => pick(o, ['CATEGORY', 'ROWS', 'BYTES', 'PATH']))
      const parseOutputs = files.map((o) => {
        return mapValues(shake(o, v => isNil(v) || isEmpty(v)), v => isObjectLike(v) ? yaml.stringify(v) : v)
      })
      const parseResultsCsv = Papa.unparse(parseOutputs)
      const totalRows = sumBy(files, 'ROWS')
      const totalBytes = sumBy(files, 'BYTES')
      const totalFiles = files.length
      const summaryString = yaml.stringify({
        'TOTAL ROWS': numbro(totalRows).format({ thousandSeparated: true }),
        'TOTAL BYTES': numbro(totalBytes).format({
          output: 'byte',
          spaceSeparated: true,
          base: 'general',
          average: true,
          mantissa: 2,
          optionalMantissa: true,
        }),
        'TOTAL FILES': numbro(totalFiles).format({ thousandSeparated: true }),
        // 'OUTPUT FILES': parseResults,
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
      spinner.succeed(`${spinnerText}\n${table.toString()}`)
      process.exit()
    },
    error(error, file) {
      spinner.fail(chalk.red('FAILED TO PARSE FILES\n') + error)
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
function formatHeaderValues(results: { data: JsonPrimitive[] }): string[] {
  return results.data.map((value, index, self) => {
    const occurrencesAfter = self.slice(index + 1).filter(v => v === value).length
    const occurrencesBefore = self.slice(0, index).filter(v => v === value).length + 1
    return (occurrencesAfter + occurrencesBefore) > 1 ? `${value}_${occurrencesBefore}` : `${value}`
  })
}
