import type { WriteStream } from 'node:fs'
import { createReadStream, createWriteStream } from 'node:fs'
import { join, parse as pathParse } from 'node:path'
import type { Duplex } from 'node:stream'
import { parse, stringify } from 'csv'
import { counting, omit } from 'radash'
import { ensureDirSync } from 'fs-extra'
import { camelCase } from 'lodash-es'
import Papa from 'papaparse'
import fg from 'fast-glob'

const records = []
let header: string[] = []
const rowCount = 0
let fileCount = 1
const inputFilePath = '/Users/benkoplin/Desktop/Sample Data_Installed Product_20241001.csv'
const parsedInputFile = pathParse(inputFilePath)
const parsedOutputFile = omit(parsedInputFile, ['base'])
parsedOutputFile.dir = join(parsedOutputFile.dir, `${camelCase(parsedInputFile.name)} PARSE JOB`)
parsedOutputFile.name += ' USA ONLY'
ensureDirSync(parsedOutputFile.dir)
const reader = createReadStream(inputFilePath, 'utf-8')
let writer = createWriteStream(join(parsedOutputFile.dir, `${parsedOutputFile.name} ${fileCount}.csv`), 'utf-8')
writer.on('close', () => {
  const files = fg.sync(join(parsedOutputFile.dir, '*.csv'))
  console.log(files)
})
const headerFile = createWriteStream(join(parsedOutputFile.dir, 'header.csv'), 'utf-8')
const parser = parse({
  bom: true,
  trim: true,
//   cast: true,
//   cast_date: true,
})
const stringifier = stringify({
  quoted_empty: true,
})
reader.on('data', (chunk) => {
  writeData(chunk, parser)
})
function writeData(data: Buffer | string, writeStream: InstanceType<typeof WriteStream> | InstanceType<typeof Duplex>): void {
  reader.pause()
  parser.pause()
  stringifier.pause()
  if (!writeStream.write(data)) {
    writeStream.once('drain', () => {
      reader.resume()
      stringifier.resume()
      parser.resume()
    })
  }
  else {
    reader.resume()
    stringifier.resume()
    parser.resume()
  }
}
stringifier.on('readable', () => {
  let row

  while ((row = stringifier.read()) !== null) {
    if (writer.bytesWritten > 5 * 1024 * 1024) {
      writer.end()
      fileCount++
      writer = createWriteStream(join(parsedOutputFile.dir, `${parsedOutputFile.name} ${fileCount}.csv`), 'utf-8')
      writer.on('close', () => {
        const files = fg.sync(join(parsedOutputFile.dir, '*.csv'))
        console.log(files)
      })
      writeData(row, writer)
    }
    else {
      writeData(row, writer)
    }

    // records.push(row.toString())
  }
})
parser.on('readable', () => {
  let record
  while ((record = parser.read()) !== null) {
    if (header.length === 0) {
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
      writeData(Papa.unparse([header]), headerFile)
    }
    else if (record[55] === 'USA') {
      writeData(record, stringifier)
    }
  }
})
// Catch any error
parser.on('error', (err) => {
  console.error(err.message)
})

//   .pipe(writer)
