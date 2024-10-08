import { createReadStream, createWriteStream, existsSync, writeFileSync } from 'node:fs'
import { appendFile } from 'node:fs/promises'
import { join, parse as pathParse } from 'node:path'
import { parse, stringify } from 'csv'
import { parse as parseSync } from 'csv/sync'
import { counting, omit } from 'radash'
import { emptyDirSync, ensureDirSync } from 'fs-extra'
import { camelCase, snakeCase } from 'lodash-es'
import Papa from 'papaparse'
import ora from 'ora'

let header: string[] = []
const indexObject: Record<string, number> = {}
const materialClassObject: Record<string, {
  bytes: number
  fileNum: number
}> = {}
const inputFilePath = '/Users/benkoplin/Desktop/Sample Data_Installed Product_20241001.csv'
const parsedInputFile = pathParse(inputFilePath)
const parsedOutputFile = omit(parsedInputFile, ['base'])
parsedOutputFile.dir = join(parsedOutputFile.dir, `${camelCase(parsedInputFile.name)} PARSE JOB`)
parsedOutputFile.name += ' USA ONLY'
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
    if (UltimateConsigneeCountryName === 'USA' && record[indexObject.MaterialClassTypeName] === 'System') {
      stringifier.write(record)
    }
  }
//   }
})
void run()
async function run() {
  let rowCount = 0
  const fileCount = 1

  const files = []

  //   ensureFileSync(filePath)
  // Report start
  // Iterate through each records
  for await (const row of stringifier) {
    // Report current line
    // Fake asynchronous operation
    const theseRows = parseSync(row, { skip_empty_lines: true })

    for await (const thisRow of theseRows) {
      const thisMaterialClass = thisRow[indexObject.Level2ProductCategoryDescription]
      if (materialClassObject[thisMaterialClass] === undefined) {
        materialClassObject[thisMaterialClass] = {
          bytes: 0,
          fileNum: 1,
        }
      }
      let filePath = join(parsedOutputFile.dir, `${parsedOutputFile.name} ${snakeCase(thisMaterialClass)} ${materialClassObject[thisMaterialClass].fileNum}.csv`)
      if (!existsSync(filePath))
        writeFileSync(filePath, '', 'utf-8')
      rowCount += theseRows.length
      materialClassObject[thisMaterialClass].bytes += row.length
      if (materialClassObject[thisMaterialClass].bytes > (10 * 1024 * 1024)) {
        files.push({
          filePath,
          bytes: materialClassObject[thisMaterialClass].bytes,
        })
        materialClassObject[thisMaterialClass].fileNum += 1
        filePath = join(parsedOutputFile.dir, `${parsedOutputFile.name} ${snakeCase(thisMaterialClass)} ${materialClassObject[thisMaterialClass].fileNum}.csv`)
        // if (!existsSync(filePath))
        //   writeFileSync(filePath, '', 'utf-8')
        writeFileSync(filePath, '', 'utf-8')
        materialClassObject[thisMaterialClass].bytes = row.length
        //   byteLength = row.length
      }
      const csvOutput = Papa.unparse([thisRow])
      await appendFile(filePath, `${csvOutput}\n`, 'utf-8')
    }
  }
  // Report end
  ora().succeed(`Processed ${rowCount} rows into ${files.length} files`)
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
parser.on('error', (err) => {
  console.error(err.message)
})

// reader.pipe(parser).pipe(stringifier)
