import './table-layout.d'
import { createReadStream, createWriteStream } from 'node:fs'
import { writeFile } from 'node:fs/promises'
import path, { join, parse as pathParse } from 'node:path'
import Table from 'table-layout'
import numbro from 'numbro'
import { alphabetical, flat, omit, pick, zipToObject } from 'radash'
import { emptyDirSync, ensureDirSync } from 'fs-extra'
import { camelCase, delay, findIndex, findLast, noop, padStart, sumBy, upperFirst } from 'lodash-es'
import Papa from 'papaparse'
import type { JsonPrimitive } from 'type-fest'
import ora from 'ora'
import picocolors from 'picocolors'

import type { FileMetrics } from './types'

const files: Array<FileMetrics> = []

// const inputFilePath = '/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-ReedSmithLLP/Illumina - CID 23-1561 - Documents/Facts - Sales and Marketing/SAP Reports/Shipments_Systems_2013 to 2024_20241007.csv'
const inputFilePath = '/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-Illumina,Inc./DOJ-CID 23-1561 & remediation - Documents/Facts - Sales and Marketing/Sales/SAP reports/Sample Data_Shipments_20241001.csv'

const filterValues = [['UltimateConsigneeCountryName', 'USA'], ['MaterialClassTypeName', 'System']]
const categoryField = 'Level3ProductLineDescription'
const parsedInputFile = pathParse(inputFilePath)
const parsedOutputFile = omit(parsedInputFile, ['base'])
parsedOutputFile.dir = join(parsedOutputFile.dir, `${camelCase(parsedInputFile.name)} PARSE JOB`)
parsedOutputFile.name = filterValues.length ? flat(filterValues).join(' ') : parsedInputFile.name
emptyDirSync(parsedOutputFile.dir)
ensureDirSync(parsedOutputFile.dir)
const reader = createReadStream(inputFilePath, 'utf-8')
let fields: string[] = []
const headerFilePath = join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`)
const headerFile = createWriteStream(headerFilePath, 'utf-8')
const spinner = ora({
  hideCursor: false,
  discardStdin: false,
})
let parsedLines = 0
spinner.start(`Parsing ${picocolors.cyan(parsedInputFile.base)}`)
const maxFileSizeInMb = 20
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
      if (filterValues.every(([field, value]) => thisRow[field] === value)) {
        const csvOutput = Papa.unparse([results.data])
        const csvRowLength = Buffer.from(csvOutput).length
        const category = thisRow[categoryField] as string
        let lastCategoryMatch = findLast(files, a => a.CATEGORY === category)
        if (lastCategoryMatch === undefined) {
          const csvFileName = generateCsvFileName(1, category)
          lastCategoryMatch = {
            BYTES: csvRowLength,
            FILENUM: 1,
            ROWS: 1,
            CATEGORY: category,
            PATH: join(parsedOutputFile.dir, csvFileName),
          }
          files.push(lastCategoryMatch)
          spinner.text = `Created ${picocolors.yellow(`"${csvFileName}"`)}`
        }
        else if ((lastCategoryMatch.BYTES + csvRowLength) > (maxFileSizeInMb * 1024 * 1024)) {
          const fileNumber = lastCategoryMatch.FILENUM + 1
          const csvFileName = generateCsvFileName(fileNumber, category)
          lastCategoryMatch = {
            BYTES: csvRowLength,
            FILENUM: fileNumber,
            ROWS: 1,
            PATH: join(parsedOutputFile.dir, csvFileName),
            CATEGORY: category,
          }
          files.push(lastCategoryMatch)
          spinner.text = `FINISHED WITH ${picocolors.yellow(`"${csvFileName}"`)}`
          await delay(noop, 750)
        }
        else {
          lastCategoryMatch.BYTES += csvRowLength
          lastCategoryMatch.ROWS += 1
          const currentFileIndex = findIndex(files, { PATH: lastCategoryMatch.PATH })
          files[currentFileIndex] = lastCategoryMatch
          spinner.text = `Writing ${picocolors.yellow(`"${path.parse(lastCategoryMatch.PATH).base}"`)}`
        }
        if (lastCategoryMatch.BYTES === csvRowLength) {
          await writeFile(lastCategoryMatch.PATH, csvOutput, {
            flag: 'a',
            encoding: 'utf-8',
          })
        }
        else {
          await writeFile(lastCategoryMatch.PATH, `\n${csvOutput}`, {
            flag: 'a',
            encoding: 'utf-8',
          })
        }
      }
    }
    parser.resume()
  },
  complete: () => {
    const parseResults = alphabetical(files, o => `${o.CATEGORY}${padStart(`${o.FILENUM}`, 3, '0')}`).map(o => pick(o, ['CATEGORY', 'ROWS', 'BYTES', 'PATH']))
    const totalRows = sumBy(files, 'ROWS')
    const totalBytes = sumBy(files, 'BYTES')
    const totalFiles = files.length
    const table = new Table(parseResults, {
      maxWidth: 600,
      columns: [
        {
          name: 'CATEGORY',
          get: cellValue => picocolors.yellow(cellValue),
        },
        {
          name: 'PATH',
          get: cellValue => picocolors.cyan(cellValue),
        },
        {
          name: 'ROWS',
          get: cellValue => `ROWS: ${numbro(cellValue).format({ thousandSeparated: true })}`,
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
    spinner.succeed(`SUCCESSFULLY PARSED ${picocolors.green(formattedParsedLines)} LINES INTO ${picocolors.green(formattedTotalRows)} LINES ACROSS ${picocolors.green(formattedTotalFiles)} FILES TOTALLING ${picocolors.green(formattedTotalBytes)}\n${table.toString()}`)
    process.exit()
  },
  error(error, file) {
    spinner.fail(picocolors.red('FAILED TO PARSE FILES\n') + error)
  },

  header: false,
  transform: value => value.trim() === '' ? null : value.trim(),
})

function generateCsvFileName(fileNumber: number, category: string): string {
  const paddedFilesCount = padStart(`${fileNumber}`, 3, '0')
  const csvFileName = `${parsedOutputFile.name} ${upperFirst(camelCase(category))} ${paddedFilesCount}.csv`
  return csvFileName
}
