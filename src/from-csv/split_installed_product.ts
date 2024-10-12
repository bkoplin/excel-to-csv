import { createReadStream, createWriteStream } from 'node:fs'
import { writeFile } from 'node:fs/promises'
import path, { join, parse as pathParse } from 'node:path'
import Table from 'table-layout'
import numbro from 'numbro'
import { stringify } from 'csv'
import { alphabetical, omit, pick, zipToObject } from 'radash'
import { emptyDirSync, ensureDirSync } from 'fs-extra'
import { camelCase, findIndex, padStart, upperFirst } from 'lodash-es'
import Papa from 'papaparse'
import type { JsonObject, ValueOf } from 'type-fest'
import ora from 'ora'
import picocolors from 'picocolors'
import type { FileMetrics } from './types'

const header: string[] = []
const indexObject: Record<string, number> = {}
const materialClassObject: Record<string, FileMetrics> = {}

const inputFilePath = '/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-ReedSmithLLP/Illumina - CID 23-1561 - Documents/Facts - Sales and Marketing/SAP Reports/Installed Product by Quarter_20241007.csv'

const parsedInputFile = pathParse(inputFilePath)
const parsedOutputFile = omit(parsedInputFile, ['base'])
parsedOutputFile.dir = join(parsedOutputFile.dir, `${camelCase(parsedInputFile.name)} PARSE JOB`)

emptyDirSync(parsedOutputFile.dir)
ensureDirSync(parsedOutputFile.dir)
const reader = createReadStream(inputFilePath, 'utf-8')
let headerFields: string[] = []
const headerFilePath = join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`)
const headerFile = createWriteStream(headerFilePath, 'utf-8')
const files: Array<ValueOf<typeof materialClassObject> & {
  CATEGORY: string
  PATH: string
}> = []
const spinner = ora({
  hideCursor: false,
  discardStdin: false,
})
let parsedLines = 0
spinner.start(`Parsing ${picocolors.cyan(parsedInputFile.base)}`)
Papa.parse(reader, {
  async step(results, parser) {
    parser.pause()
    if (headerFile.writable && Array.isArray(results.data)) {
      headerFields = results.data
      headerFile.end(Papa.unparse([results.data]))
    }
    else {
      const thisRow = zipToObject(headerFields, results.data) as JsonObject
      parsedLines++
      if (thisRow.UltimateConsigneeCountryName === 'USA' && thisRow.MaterialClassTypeName === 'System') {
        const thisMaterialClass = thisRow.Level3ProductLineDescription as string
        if (materialClassObject[thisMaterialClass] === undefined) {
          materialClassObject[thisMaterialClass] = {
            BYTES: 0,
            FILES: 1,
            ROWS: 0,
          }
        }
        const camelCasedMaterialClass = upperFirst(camelCase(thisMaterialClass))
        const paddedFilesCount = padStart(`${materialClassObject[thisMaterialClass].FILES}`, 3, '0')
        const csvFileName = `${parsedOutputFile.name} ${camelCasedMaterialClass} ${paddedFilesCount}.csv`
        let filePath = join(parsedOutputFile.dir, csvFileName)
        if (materialClassObject[thisMaterialClass].BYTES === 0) {
          spinner.text = picocolors.yellow(`Created ${csvFileName}`)
          files.push({
            ...materialClassObject[thisMaterialClass],
            CATEGORY: thisMaterialClass,
            PATH: filePath,
          })
        }
        const materialObjectIndex = findIndex(files, { PATH: filePath })
        const csvOutput = Papa.unparse([results.data])
        const csvRowLength = Buffer.from(csvOutput).length
        if (materialObjectIndex > -1) {
          files[materialObjectIndex].BYTES += csvRowLength
          files[materialObjectIndex].ROWS += 1
        }
        if ((materialClassObject[thisMaterialClass].BYTES + csvRowLength) > (20 * 1024 * 1024)) {
          materialClassObject[thisMaterialClass].FILES += 1
          const zeroPaddedFiles = padStart(`${materialClassObject[thisMaterialClass].FILES}`, 3, '0')
          const updatedFileName = `${parsedOutputFile.name} ${camelCasedMaterialClass} ${zeroPaddedFiles}.csv`
          spinner.text = picocolors.yellow(`Finished with ${updatedFileName}`)
          filePath = join(parsedOutputFile.dir, updatedFileName)
          spinner.text = picocolors.yellow(`Created ${csvFileName}`)

          await writeFile(filePath, csvOutput, {
            flag: 'a',
            encoding: 'utf-8',
          })
          materialClassObject[thisMaterialClass].BYTES = csvRowLength
          materialClassObject[thisMaterialClass].ROWS = 1
          files.push({
            ...materialClassObject[thisMaterialClass],
            CATEGORY: thisMaterialClass,
            PATH: filePath,
          })
        }
        else {
          if (materialClassObject[thisMaterialClass].BYTES === 0) {
            await writeFile(filePath, csvOutput, {
              flag: 'a',
              encoding: 'utf-8',
            })
          }
          else {
            await writeFile(filePath, `\n${csvOutput}`, {
              flag: 'a',
              encoding: 'utf-8',
            })
          }
          materialClassObject[thisMaterialClass].BYTES += csvRowLength
          materialClassObject[thisMaterialClass].ROWS += 1
        }
      }
    }
    parser.resume()
  },
  complete: (results) => {
    const parseResults = alphabetical(files, o => `${o.CATEGORY}${padStart(`${o.FILES}`, 3, '0')}`).map(o => pick(o, ['CATEGORY', 'ROWS', 'BYTES', 'PATH']))
    const table = new Table(parseResults, {
      maxWidth: 600,
      columns: [
        {
          name: 'CATEGORY',
          get: (cellValue: string) => picocolors.yellow(cellValue),
        },
        {
          name: 'PATH',
          get: cellValue => picocolors.cyan(path.parse(cellValue).base),
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

    spinner.succeed(picocolors.green(`SUCCESSFULLY PARSED ${parsedLines} LINES\n`) + table.toString())
    process.exit()
  },
  error(error, file) {
    spinner.fail(picocolors.red('FAILED TO PARSE FILES\n') + error)
  },

  header: false,
  transform: value => value.trim() === '' ? null : value.trim(),
})
// reader.on('data', (csvFile) => {
// })

const stringifier = stringify({
  quoted_empty: true,
})
