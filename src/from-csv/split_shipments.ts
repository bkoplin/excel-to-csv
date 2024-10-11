import './table-layout.d'
import { createReadStream, createWriteStream } from 'node:fs'
import { open, writeFile } from 'node:fs/promises'
import path, { join, parse as pathParse, relative } from 'node:path'
import Table from 'table-layout'
import numbro from 'numbro'
import { alphabetical, flat, omit, pick, zipToObject } from 'radash'
import { emptyDirSync, ensureDirSync } from 'fs-extra'
import { camelCase, delay, findIndex, findLast, isUndefined, last, maxBy, noop, padStart, sumBy, upperFirst } from 'lodash-es'
import Papa from 'papaparse'
import type { JsonPrimitive, SetRequired } from 'type-fest'
import ora from 'ora'
import picocolors from 'picocolors'

import type { FileMetrics, SplitOptions } from './types'

// const inputFilePath = '/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-ReedSmithLLP/Illumina - CID 23-1561 - Documents/Facts - Sales and Marketing/SAP Reports/Shipments_Systems_2013 to 2024_20241007.csv'
const inputFilePath = '/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-Illumina,Inc./DOJ-CID 23-1561 & remediation - Documents/Facts - Sales and Marketing/Sales/SAP reports/Sample Data_Shipments_20241001.csv'

const filterValues = [['UltimateConsigneeCountryName', 'USA'], ['MaterialClassTypeName', 'System']]
const categoryField = 'Level3ProductLineDescription'
const maxFileSizeInMb = 20
splitCSV({
  inputFilePath: '/Users/benkoplin/Library/CloudStorage/OneDrive-ReedSmithLLP/Downloads/POS_File_CLIA_Q1_2024.csv',
  // filterValues: [['UltimateConsigneeCountryName', 'USA'], ['MaterialClassTypeName', 'System']],
  // categoryField: 'Level3ProductLineDescription',
  maxFileSizeInMb: 30,
  // writeHeaderOnEachFile: true,
})
export async function splitCSV<Options extends SplitOptions>({ inputFilePath, filterValues = [], categoryField = '', maxFileSizeInMb, writeHeaderOnEachFile = false }: Options): Promise<void> {
  const spinner = ora({
    hideCursor: false,
    discardStdin: false,
  })
  const files: Array<FileMetrics> = []
  const parsedInputFile = pathParse(inputFilePath)
  spinner.start(`Parsing ${picocolors.cyan(parsedInputFile.base)}`)
  const parsedOutputFile = omit(parsedInputFile, ['base'])
  parsedOutputFile.dir = join(parsedOutputFile.dir, `${parsedInputFile.name} PARSE JOB`)
  parsedOutputFile.name = filterValues.length ? flat(filterValues).join(' ') : parsedInputFile.name
  emptyDirSync(parsedOutputFile.dir)
  ensureDirSync(parsedOutputFile.dir)
  const reader = createReadStream(inputFilePath, 'utf-8')
  let fields: string[] = []
  const headerFilePath = join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`)
  const headerFile = createWriteStream(headerFilePath, 'utf-8')
  let parsedLines = 0
  Papa.parse<JsonPrimitive[]>(reader, {
    async step(results, parser) {
      parser.pause()
      if (headerFile.writable && Array.isArray(results.data) && !fields.length) {
        fields = results.data as string[]
        headerFile.end(Papa.unparse([results.data]))
      }
      else {
        const thisRow = zipToObject(fields, results.data)
        parsedLines++
        if (filterValues.length === 0 || filterValues.every(([field, value]) => thisRow[field] === value)) {
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
            PATH: path.format({
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
          else if (!isUndefined(activeFileObject) && !isUndefined(maxFileSizeInMb) && (activeFileObject.BYTES + csvRowLength) > (maxFileSizeInMb * 1024 * 1024)) {
            const fileNumber = activeFileObject.FILENUM! + 1
            const csvFileName = generateCsvFileName({
              fileNumber,
              category,
            })
            activeFileObject = {
              BYTES: csvRowLength,
              FILENUM: fileNumber,
              ROWS: 1,
              PATH: path.format({
                ...parsedOutputFile,
                name: csvFileName,
              }),
              CATEGORY: category,
            }
            files.push(activeFileObject)
            spinner.text = `FINISHED WITH ${picocolors.yellow(`"${csvFileName}"`)}`
            await delay(noop, 750)
          }
          else {
            activeFileObject.BYTES += csvRowLength
            activeFileObject.ROWS += 1
            const currentFileIndex = findIndex(files, { PATH: activeFileObject.PATH })
            files[currentFileIndex] = activeFileObject
            spinner.text = `Writing ${picocolors.yellow(`"${path.parse(activeFileObject.PATH).base}"`)}`
          }
          if (activeFileObject.BYTES === csvRowLength) {
            await writeFile(activeFileObject.PATH, csvOutput, {
              flag: 'a',
              encoding: 'utf-8',
            })
          }
          else {
            await writeFile(activeFileObject.PATH, `\n${csvOutput}`, {
              flag: 'a',
              encoding: 'utf-8',
            })
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
      if (writeHeaderOnEachFile) {
        for (const file of files) {
          const header = Papa.unparse([fields])
          const openFile = await open(file.PATH, 'a')
          await openFile.write(`${header}\n`, 0, 'utf-8')
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
            get: cellValue => picocolors.cyan(`./${relative(parsedInputFile.dir, cellValue as string)}`),
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
      let spinnerText = `SUCCESSFULLY PARSED ${picocolors.green(formattedParsedLines)} LINES INTO ${picocolors.green(formattedTotalRows)} LINES ACROSS ${picocolors.green(formattedTotalFiles)} FILES`
      if (writeHeaderOnEachFile)
        spinnerText += picocolors.yellow(` WITH HEADERS IN EACH FILE`)
      else spinnerText += picocolors.yellow(` WITH A SEPARATE HEADER FILE`)
      spinnerText += ` TOTALLING ${picocolors.green(formattedTotalBytes)}`
      spinner.succeed(`${spinnerText}\n${table.toString()}`)
      process.exit()
    },
    error(error, file) {
      spinner.fail(picocolors.red('FAILED TO PARSE FILES\n') + error)
    },

    header: false,
    transform: value => value.trim() === '' ? null : value.trim(),
  })
  function generateCsvFileName({ fileNumber, category }: {
    fileNumber?: number
    category?: string
  } = {}): string {
    let csvFileName = parsedOutputFile.name
    // } ${upperFirst(camelCase(category))} ${paddedFilesCount}.csv`
    if (typeof category !== 'undefined')
      csvFileName += ` ${upperFirst(camelCase(category))}`
    if (typeof fileNumber !== 'undefined')
      csvFileName += ` ${padStart(`${fileNumber}`, 4, '0')}`
    // const paddedFilesCount = padStart(`${fileNumber}`, 3, '0')
    return csvFileName
  }
}
