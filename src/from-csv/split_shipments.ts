import { createReadStream, createWriteStream } from 'node:fs'
import { writeFile } from 'node:fs/promises'
import { join, parse as pathParse } from 'node:path'
import { parse, stringify } from 'csv'
import { parse as parseSync } from 'csv/sync'
import { alphabetical, counting, omit } from 'radash'
import { emptyDirSync, ensureDirSync } from 'fs-extra'
import { camelCase, padStart, snakeCase } from 'lodash-es'
import Papa from 'papaparse'
import { oraPromise } from 'ora'
import picocolors from 'picocolors'
import Table from 'table-layout'
import { objectEntries } from '@antfu/utils'

let header: string[] = []
const indexObject: Record<string, number> = {}
const materialClassObject: Record<string, {
  bytes: number
  fileNum: number
  rowCount: number
}> = {}
const inputFilePath = '/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-ReedSmithLLP/Illumina - CID 23-1561 - Documents/Facts - Sales and Marketing/SAP Reports/Shipments_Systems_2013 to 2024_20241007.csv'
const parsedInputFile = pathParse(inputFilePath)
const parsedOutputFile = omit(parsedInputFile, ['base'])
parsedOutputFile.dir = join(parsedOutputFile.dir, `${camelCase(parsedInputFile.name)} PARSE JOB`)
parsedOutputFile.name = 'shipments_usa_2013_2024'
emptyDirSync(parsedOutputFile.dir)
ensureDirSync(parsedOutputFile.dir)
const reader = createReadStream(inputFilePath, 'utf-8')
const headerFile = createWriteStream(join(parsedOutputFile.dir, `${parsedOutputFile.name} HEADER.csv`), 'utf-8')
const parser = parse({
  bom: true,
  trim: true,
//   cast: true,
//   cast_date: true,
})
reader.on('data', (chunk) => {
  parser.write(chunk)
})
const stringifier = stringify({
  quoted_empty: true,
})
parser.on('data', (record) => {
  //   let record
  //   if ((record = parser.read()) !== null) {
  if (header.length === 0) {
    // header = record as string[]
    const groupedColumnNames = counting(record as string[], v => v)
    header = (record as string[]).reverse().map((v) => {
      if (groupedColumnNames[v] > 1) {
        const count = groupedColumnNames[v]
        groupedColumnNames[v] -= 1
        return `${v} ${count - 1}`
      }
      return v
    })
      .reverse()
    header.forEach((v, i) => indexObject[v] = i)
    headerFile.end(Papa.unparse([header]))
  }
  else {
    const UltimateConsigneeCountryName = record[indexObject.UltimateConsigneeCountryName]
    if (UltimateConsigneeCountryName === 'USA') {
      stringifier.write(record)
    }
  }
//   }
})
oraPromise(async (spinner): Promise<Record<string, {
  bytes: number
  fileNum: number
  rowCount: number
}>> => {
  spinner.start(`Parsing ${parsedInputFile.base}`)
  try {
    const val = await run()
    const parseResults = alphabetical(objectEntries(val).map(([material, parseObject]) => ({
      material,
      ...parseObject,
    })), o => `${o.material}${padStart(`${o.fileNum}`, 3, '0')}`)
    const table = new Table(parseResults, { maxWidth: 600 })
    spinner.text = table.toString()
    return val
    // spinner.succeed()
  }
  catch (err) {
    spinner.text = err?.message
    throw err
    // spinner.fail(picocolors.red('FAILED TO PARSE FILES\n') + err)
  }
}, {
  successText: picocolors.green('SUCCESSFULLY PARSED FILES\n'),
  failText: picocolors.red('FAILED TO PARSE FILES\n'),
})
async function run(): Promise<Record<string, {
  bytes: number
  fileNum: number
  rowCount: number
}>> {
  const files = []
  const rowCount = 0
  const fileCount = 1

  //   ensureFileSync(filePath)
  // Report start
  // Iterate through each records
  for await (const row of stringifier) {
    // Report current line
    // Fake asynchronous operation
    const theseRows = parseSync(row, { skip_empty_lines: true })

    for await (const thisRow of theseRows) {
      const thisMaterialClass = thisRow[indexObject.Level3ProductLineDescription]
      if (materialClassObject[thisMaterialClass] === undefined) {
        materialClassObject[thisMaterialClass] = {
          bytes: 0,
          fileNum: 1,
          rowCount: 0,
        }
      }
      let filePath = join(parsedOutputFile.dir, `${parsedOutputFile.name}_${snakeCase(thisMaterialClass)} ${materialClassObject[thisMaterialClass].fileNum}.csv`)

      const csvOutput = Papa.unparse([thisRow])
      const csvRowLength = Buffer.from(csvOutput).length
      if ((materialClassObject[thisMaterialClass].bytes + csvRowLength) > (20 * 1024 * 1024)) {
        files.push({
          filePath,
          bytes: materialClassObject[thisMaterialClass].bytes,
        })
        materialClassObject[thisMaterialClass].fileNum += 1
        filePath = join(parsedOutputFile.dir, `${parsedOutputFile.name}_${snakeCase(thisMaterialClass)} ${materialClassObject[thisMaterialClass].fileNum}.csv`)
        // if (!existsSync(filePath))
        //   writeFileSync(filePath, '', 'utf-8')
        //   byteLength = row.length
        await writeFile(filePath, csvOutput, {
          flag: 'a',
          encoding: 'utf-8',
        })
        materialClassObject[thisMaterialClass].bytes = csvRowLength
        materialClassObject[thisMaterialClass].rowCount = 1
      }
      else {
        await writeFile(filePath, `\n${csvOutput}`, {
          flag: 'a',
          encoding: 'utf-8',
        })
        materialClassObject[thisMaterialClass].bytes += csvRowLength
        materialClassObject[thisMaterialClass].rowCount += 1
      }
    }
  }
  // Report end
  return materialClassObject
}
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
