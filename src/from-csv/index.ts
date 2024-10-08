import { createReadStream, createWriteStream, writeFileSync } from 'node:fs'
import { appendFile } from 'node:fs/promises'
import { join, parse as pathParse } from 'node:path'
import { parse, stringify } from 'csv'
import { parse as parseSync } from 'csv/sync'
import { counting, omit } from 'radash'
import { emptyDirSync, ensureDirSync } from 'fs-extra'
import { camelCase } from 'lodash-es'
import Papa from 'papaparse'
import ora from 'ora'

let header: string[] = []
const inputFilePath = '/Users/benkoplin/Desktop/Sample Data_Installed Product_20241001.csv'
const parsedInputFile = pathParse(inputFilePath)
const parsedOutputFile = omit(parsedInputFile, ['base'])
parsedOutputFile.dir = join(parsedOutputFile.dir, `${camelCase(parsedInputFile.name)} PARSE JOB`)
parsedOutputFile.name += ' USA ONLY'
ensureDirSync(parsedOutputFile.dir)
emptyDirSync(parsedOutputFile.dir)
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
    headerFile.end(Papa.unparse([header]))
  }
  else if (record[55] === 'USA' && record[26] === 'System') {
    stringifier.write(record)
  }
//   }
})
void run()
async function run() {
  let rowCount = 0

  let fileCount = 1
  let filePath = join(parsedOutputFile.dir, `${parsedOutputFile.name} ${fileCount}.csv`)

  writeFileSync(filePath, '', 'utf-8')
  let byteLength = 0
  const files = []

  //   ensureFileSync(filePath)
  // Report start
  // Iterate through each records
  for await (const row of stringifier) {
    // Report current line
    // Fake asynchronous operation
    const theseRows = parseSync(row, { skip_empty_lines: true })
    rowCount += theseRows.length
    byteLength += row.length
    if (byteLength > (10 * 1024 * 1024)) {
      files.push({
        filePath,
        byteLength,
      })
      fileCount += 1
      filePath = join(parsedOutputFile.dir, `${parsedOutputFile.name} ${fileCount}.csv`)
      writeFileSync(filePath, '', 'utf-8')
      byteLength = row.length
    //   byteLength = row.length
    }
    for await (const thisRow of theseRows) {
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
