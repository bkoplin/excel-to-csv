import './table-layout.d'
import {
  createReadStream,
  createWriteStream,
} from 'node:fs'
import {
  appendFile,
  readFile,
  writeFile,
} from 'node:fs/promises'
import {
  format,
  join,
  parse,
} from 'pathe'
import { filename } from 'pathe/utils'
import Table from 'table-layout'
import numbro from 'numbro'
import {
  alphabetical,
  isNumber,
  omit,
  pick,
  zipToObject,
} from 'radash'
import fs from 'fs-extra'
import {
  camelCase,
  delay,
  escape,
  findIndex,
  findLast,
  isUndefined,
  last,
  maxBy,
  noop,
  padStart,
  sumBy,
  upperFirst,
} from 'lodash-es'
import Papa from 'papaparse'
import type {
  JsonPrimitive,
  SetRequired,
} from 'type-fest'
import ora from 'ora'
import picocolors from 'picocolors'
import yaml from 'yaml'
import dayjs from 'dayjs'
import type { CommandOptions } from '../split-csv'
import type { FileMetrics } from './types'

export async function splitCSV<Options extends CommandOptions>(options: Options): Promise<void> {
  const splitOptions = yaml.stringify(options)
  const {
    inputFilePath,
    categoryField = '',
    maxFileSizeInMb,
    matchType,
    filters = [],
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
  const parsedOutputFile = omit(parsedInputFile, ['base'])
  parsedOutputFile.dir = join(parsedOutputFile.dir, `${parsedInputFile.name} PARSE JOBS`, dayjs().format('YYYY-MM-DD HH-MM') + (filters.length ? ' FILTERED' : ''))
  // parsedOutputFile.name = filters.length ? `${parsedInputFile.name} FILTERED` : parsedInputFile.name
  fs.emptyDirSync(parsedOutputFile.dir)
  // ensureDirSync(parsedOutputFile.dir)
  fs.outputFileSync(join(parsedOutputFile.dir, `${parsedOutputFile.name} OPTIONS.yaml`), splitOptions)
  const reader = createReadStream(inputFilePath, 'utf-8')
  let fields: string[] = []
  const headerFilePath = join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`)
  const headerFile = createWriteStream(headerFilePath, 'utf-8')
  let parsedLines = 0
  Papa.parse<JsonPrimitive[]>(reader, {
    async step(results, parser) {
      // parser.pause()
      if (headerFile.writable && Array.isArray(results.data) && !fields.length) {
        fields = results.data as string[]
        headerFile.end(Papa.unparse([results.data]))
      }
      else {
        const thisRow = zipToObject(fields, results.data)
        parsedLines++
        if (filters.length === 0 || (filters.every(([field, value]) => thisRow[field] === value) && matchType === 'all') || (filters.some(([field, value]) => thisRow[field] === value) && matchType === 'any') || (filters.every(([field, value]) => thisRow[field] !== value) && matchType === 'none')) {
          const csvOutput = Papa.unparse([results.data])
          const csvRowLength = Buffer.from(csvOutput).length
          const category = thisRow[categoryField] as string | undefined

          const defaultFileNumber = (maxFileSizeInMb ? 1 : undefined) as Options['maxFileSizeInMb'] extends number ? number : undefined
          const defaultCsvFileName = generateCsvFileName({
            fileNumber: defaultFileNumber,
            category,
          })
          const defaultFileObject: Options['maxFileSizeInMb'] extends number ? SetRequired<FileMetrics, 'FILENUM'> : FileMetrics = {
            BYTES: csvRowLength,
            FILENUM: defaultFileNumber,
            ROWS: 1,
            CATEGORY: category,
            PATH: format({
              ...parsedOutputFile,
              name: defaultCsvFileName,
            }),
          }
          let activeFileObject = (typeof category === 'undefined' ? last(files) : findLast(files, a => a.CATEGORY === category))
          if (isUndefined(activeFileObject)) {
            activeFileObject = defaultFileObject
            files.push(defaultFileObject)
            spinner.text = `Created ${picocolors.yellow(`"${defaultCsvFileName}"`)}`
            await delay(noop, 750)
          }
          else if (!isUndefined(activeFileObject) && !isUndefined(maxFileSizeInMb) && isNumber(maxFileSizeInMb) && (activeFileObject.BYTES + csvRowLength) > (maxFileSizeInMb * 1024 * 1024)) {
            files.push(activeFileObject)
            spinner.text = `FINISHED WITH ${picocolors.yellow(`"${csvFileName}"`)}`
            await delay(noop, 750)
            const fileNumber = activeFileObject.FILENUM! + 1
            const csvFileName = generateCsvFileName({
              fileNumber,
              category,
            })
            activeFileObject = {
              BYTES: csvRowLength,
              FILENUM: fileNumber,
              ROWS: 1,
              PATH: format({
                ...parsedOutputFile,
                name: csvFileName,
              }),
              CATEGORY: category,
            }
          }
          else {
            activeFileObject.BYTES += csvRowLength
            activeFileObject.ROWS += 1
            const currentFileIndex = findIndex(files, { PATH: activeFileObject.PATH })
            files[currentFileIndex] = activeFileObject
            spinner.text = `Writing ${picocolors.yellow(`"${parse(activeFileObject.PATH).base}"`)}`
          }

          await appendFile(activeFileObject.PATH, `${csvOutput}\n`, { encoding: 'utf-8' })
        }
      }
      // parser.resume()
    },
    complete: async () => {
      const maxFileNumLength = `${maxBy(files.filter(o => typeof o.FILENUM !== 'undefined'), 'FILENUM')?.FILENUM ?? ''}`.length
      const parseResults = alphabetical(files, o => o.FILENUM ? `${o.CATEGORY}${padStart(`${o.FILENUM}`, maxFileNumLength, '0')}` : o.CATEGORY ?? o.PATH).map(o => pick(o, ['CATEGORY', 'ROWS', 'BYTES', 'PATH']))
      const totalRows = sumBy(files, 'ROWS')
      const totalBytes = sumBy(files, 'BYTES')
      const totalFiles = files.length
      if (writeHeaderOnEachFile) {
        for (const file of files) {
          const header = Papa.unparse([fields])
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
            get: cellValue => picocolors.cyan(escape(cellValue as string)),
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
    category?: string
  } = {}): string {
    let csvFileName = parsedOutputFile.name

    if (typeof category !== 'undefined')
      csvFileName += ` ${upperFirst(camelCase(category))}`
    if (typeof fileNumber !== 'undefined')
      csvFileName += ` ${padStart(`${fileNumber}`, 4, '0')}`

    return csvFileName
  }
}
