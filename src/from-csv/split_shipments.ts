import { createWriteStream } from 'node:fs'
import { readFile, writeFile } from 'node:fs/promises'
import path, { join, parse as pathParse } from 'node:path'
import Table from 'table-layout'
import numbro from 'numbro'
import { alphabetical, omit, pick, zipToObject } from 'radash'
import { emptyDirSync, ensureDirSync } from 'fs-extra'
import { camelCase, findIndex, padStart, upperFirst } from 'lodash-es'
import Papa from 'papaparse'
import type { JsonPrimitive, ValueOf } from 'type-fest'
import ora from 'ora'
import picocolors from 'picocolors'

import type { FileMetrics } from './types'

const materialClassObject: Record<string, FileMetrics> = {}
// const inputFilePath = '/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-ReedSmithLLP/Illumina - CID 23-1561 - Documents/Facts - Sales and Marketing/SAP Reports/2024.10.04 SAP Systems and Shipments/Sample Data_Shipments_20241001.csv'
const inputFilePath = '/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-ReedSmithLLP/Illumina - CID 23-1561 - Documents/Facts - Sales and Marketing/SAP Reports/Shipments_Systems_2013 to 2024_20241007.csv'
// const inputFilePath = '/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-ReedSmithLLP/Illumina - CID 23-1561 - Documents/Facts - Sales and Marketing/SAP Reports/Shipments_Systems_2013 to 2024_20241007.csv'
const filterValues = [['UltimateConsigneeCountryName', 'USA'], ['MaterialClassTypeName', 'System']]
const parsedInputFile = pathParse(inputFilePath)
const parsedOutputFile = omit(parsedInputFile, ['base'])
parsedOutputFile.dir = join(parsedOutputFile.dir, `${camelCase(parsedInputFile.name)} PARSE JOB`)
// parsedOutputFile.name = 'shipments_usa_2013_2024'
emptyDirSync(parsedOutputFile.dir)
ensureDirSync(parsedOutputFile.dir)
const reader = readFile(inputFilePath, 'utf-8')
let fields: string[] = []
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
const maxFileSizeInMb = 20
const filterField = 'UltimateConsigneeCountryName'
const filterValue = 'USA'
reader.then((csvFile) => {
  Papa.parse<JsonPrimitive[]>(csvFile, {
    async step(results, parser) {
      parser.pause()
      if (headerFile.writable && Array.isArray(results.data)) {
        fields = results.data as string[]
        headerFile.end(Papa.unparse([results.data]))
      }
      else {
        const thisRow = zipToObject(fields, results.data)
        parsedLines++
        if (filterValues.every(([field, value]) => thisRow[field] === value)) {
          const thisMaterialClass = thisRow.Level3ProductLineDescription as string
          const currentMaterialObject = materialClassObject[thisMaterialClass]
          if (currentMaterialObject === undefined) {
            materialClassObject[thisMaterialClass] = {
              BYTES: 0,
              FILENUM: 1,
              ROWS: 0,
            }
          }
          const camelCasedMaterialClass = upperFirst(camelCase(thisMaterialClass))
          const paddedFilesCount = padStart(`${materialClassObject[thisMaterialClass].FILENUM}`, 3, '0')
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
          if ((materialClassObject[thisMaterialClass].BYTES + csvRowLength) > (maxFileSizeInMb * 1024 * 1024)) {
            materialClassObject[thisMaterialClass].FILENUM += 1
            const zeroPaddedFiles = padStart(`${materialClassObject[thisMaterialClass].FILENUM}`, 3, '0')
            const updatedFileName = `${parsedOutputFile.name} ${camelCasedMaterialClass} ${zeroPaddedFiles}.csv`
            spinner.text = picocolors.yellow(`Finished with ${updatedFileName}`)
            filePath = join(parsedOutputFile.dir, updatedFileName)
            spinner.text = picocolors.yellow(`Created ${csvFileName}`)
            // if (!existsSync(filePath))
            //   writeFileSync(filePath, '', 'utf-8')
            //   byteLength = row.length
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
      const parseResults = alphabetical(files, o => `${o.CATEGORY}${padStart(`${o.FILENUM}`, 3, '0')}`).map(o => pick(o, ['CATEGORY', 'ROWS', 'BYTES', 'PATH']))
      const table = new Table(parseResults, {
        maxWidth: 600,
        columns: [
          {
            name: 'CATEGORY',
            get: cellValue => picocolors.yellow(cellValue),
          },
          {
            name: 'PATH',
            get: cellValue => picocolors.cyan(path.parse(cellValue).base),
          },
          {
            name: 'ROWS',
            get: cellValue => `ROWS: ${numbro(cellValue).format({ thousandSeparated: true })}`,
          },
          // {
          //   name: 'FILES',
          //   get: cellValue => picocolors.cyan(numbro(cellValue).format({ thousandSeparated: true })),
          // },
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
      // spinner.text = table.toString()
      spinner.succeed(picocolors.green(`SUCCESSFULLY PARSED ${numbro(parsedLines).format({ thousandSeparated: true })} LINES\n`) + table.toString())
      process.exit()
    },
    error(error, file) {
      spinner.fail(picocolors.red('FAILED TO PARSE FILES\n') + error)
    },
    // preview: 10,
    header: false,
    transform: value => value.trim() === '' ? null : value.trim(),
  })
})
// reader.on('data', (chunk) => {
//   parser.write(chunk)
// })
// const parser = parse({
//   bom: true,
//   trim: true,
//   //   cast: true,
//   //   cast_date: true,
// })
// parser.on('data', (record) => {
//   //   let record
//   //   if ((record = parser.read()) !== null) {
//   if (header.length === 0) {
//     // header = record as string[]
//     const groupedColumnNames = counting(record as string[], v => v)
//     header = (record as string[]).reverse().map((v) => {
//       if (groupedColumnNames[v] > 1) {
//         const count = groupedColumnNames[v]
//         groupedColumnNames[v] -= 1
//         return `${v} ${count - 1}`
//       }
//       return v
//     })
//       .reverse()
//     header.forEach((v, i) => indexObject[v] = i)
//     headerFile.end(Papa.unparse([header]))
//   }
//   else {
//     const UltimateConsigneeCountryName = record[indexObject.UltimateConsigneeCountryName]
//     if (UltimateConsigneeCountryName === 'USA') {
//       stringifier.write(record)
//     }
//   }
// //   }
// })
// oraPromise(async (spinner): Promise<Record<string, {
//   BYTES: number
//   FILES: number
//   ROWS: number
// }>> => {
//   spinner.start(`Parsing ${parsedInputFile.base}`)
//   try {
//     const val = await run()
//     const parseResults = alphabetical(objectEntries(val).map(([material, parseObject]) => ({
//       material,
//       ...parseObject,
//     })), o => `${o.material}${padStart(`${o.FILES}`, 3, '0')}`)
//     const table = new Table(parseResults, { maxWidth: 600 })
//     spinner.text = table.toString()
//     return val
//     // spinner.succeed()
//   }
//   catch (err) {
//     spinner.text = err?.message
//     throw err
//     // spinner.fail(picocolors.red('FAILED TO PARSE FILES\n') + err)
//   }
// }, {
//   successText: picocolors.green('SUCCESSFULLY PARSED FILES\n'),
//   failText: picocolors.red('FAILED TO PARSE FILES\n'),
// })
// async function run(): Promise<Record<string, {
//   BYTES: number
//   FILES: number
//   ROWS: number
// }>> {
//   const files = []
//   const ROWS = 0
//   const fileCount = 1

//   //   ensureFileSync(filePath)
//   // Report start
//   // Iterate through each records
//   for await (const row of stringifier) {
//     // Report current line
//     // Fake asynchronous operation
//     const theseRows = parseSync(row, { skip_empty_lines: true })

//     for await (const thisRow of theseRows) {
//       const thisMaterialClass = thisRow[indexObject.Level3ProductLineDescription]
//       if (materialClassObject[thisMaterialClass] === undefined) {
//         materialClassObject[thisMaterialClass] = {
//           BYTES: 0,
//           FILES: 1,
//           ROWS: 0,
//         }
//       }
//       let filePath = join(parsedOutputFile.dir, `${parsedOutputFile.name}_${snakeCase(thisMaterialClass)} ${materialClassObject[thisMaterialClass].FILES}.csv`)

//       const csvOutput = Papa.unparse([thisRow])
//       const csvRowLength = Buffer.from(csvOutput).length
//       if ((materialClassObject[thisMaterialClass].BYTES + csvRowLength) > (20 * 1024 * 1024)) {
//         files.push({
//           filePath,
//           BYTES: materialClassObject[thisMaterialClass].BYTES,
//         })
//         materialClassObject[thisMaterialClass].FILES += 1
//         filePath = join(parsedOutputFile.dir, `${parsedOutputFile.name}_${snakeCase(thisMaterialClass)} ${materialClassObject[thisMaterialClass].FILES}.csv`)
//         // if (!existsSync(filePath))
//         //   writeFileSync(filePath, '', 'utf-8')
//         //   byteLength = row.length
//         await writeFile(filePath, csvOutput, {
//           flag: 'a',
//           encoding: 'utf-8',
//         })
//         materialClassObject[thisMaterialClass].BYTES = csvRowLength
//         materialClassObject[thisMaterialClass].ROWS = 1
//       }
//       else {
//         await writeFile(filePath, `\n${csvOutput}`, {
//           flag: 'a',
//           encoding: 'utf-8',
//         })
//         materialClassObject[thisMaterialClass].BYTES += csvRowLength
//         materialClassObject[thisMaterialClass].ROWS += 1
//       }
//     }
//   }
//   // Report end
//   return materialClassObject
// }
// parser.on('readable', () => {
//   let record
//   while ((record = parser.read()) !== null) {
//     if (header.length === 0) {
//       const groupedColumnNames = counting(record as string[], v => v)
//       header = (record as string[]).reverse().map((v) => {
//         if (groupedColumnNames[v] > 1) {
//           const count = groupedColumnNames[v]
//           groupedColumnNames[v] -= 1
//           return `${v} ${count - 1}`
//         }
//         return v
//       })
//         .reverse()
//       writeData(Papa.unparse([header]), headerFile)
//     }
//     else if (record[55] === 'USA') {
//       writeData(record, stringifier)
//     }
//   }
// })
// Catch any error
// parser.on('error', (err) => {
//   console.error(err.message)
// })

// reader.pipe(parser).pipe(stringifier)
