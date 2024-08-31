import * as fs from 'node:fs'
import { join, parse } from 'node:path'
import type { ParsedPath } from 'node:path/posix'
import { PassThrough } from 'node:stream'
import * as XLSX from 'xlsx'
import Papa from 'papaparse'
import { ceil, padStart, range } from 'lodash-es'
import type { JsonValue } from 'type-fest'
import type { Spinner } from 'yocto-spinner'
import dayjs from 'dayjs'
import colors from 'picocolors'
import { confirm, number } from '@inquirer/prompts'
import { emptyDirSync, ensureDirSync } from 'fs-extra'

XLSX.set_fs(fs)
export interface Arguments {
  filePath?: string
  sheetName?: string
  range?: string
}

export async function parseWorksheet(args: {
  filePath: string
  sheetName: string
  range: string
}, spinner: Spinner): Promise<void> {
  const { filePath, sheetName, range: inputRange } = args
  const parsedFile = parse(filePath)
  const workbook = XLSX.readFile(filePath, { raw: true, cellDates: true, dense: true, sheet: sheetName })
  let csvSize = fs.statSync(filePath).size
  let splitWorksheet = false
  let outputFilePath = `${parsedFile.dir}/${parsedFile.name}_${sheetName}_${dayjs().format('YYYY.MM.DD HH.mm.ss')}`
  if (csvSize > 5000000) {
    splitWorksheet = await confirm({ message: 'The file is large. Would you like to split the output into multiple CSVs?', default: false })
    if (splitWorksheet) {
      ensureDirSync(join(parsedFile.dir, dayjs().format('YYYY.MM.DD HH.mm.ss')))
      emptyDirSync(join(parsedFile.dir, dayjs().format('YYYY.MM.DD HH.mm.ss')))
      outputFilePath = join(parsedFile.dir, dayjs().format('YYYY.MM.DD HH.mm.ss'), `${parsedFile.name} ${sheetName}`)
      const tempCSVSize = await number({ message: 'Size of output CSV files (in Mb):', default: ceil(csvSize / 1000000, 2), min: 1, max: ceil(csvSize / 1000000, 2) * 4 })
      csvSize = tempCSVSize * 1000000
    }
    else {
      csvSize = csvSize * 10
    }
  }
  spinner.start()
  const parsingConfig: FileParserOptions = { workbook, sheetName, inputRange, parsedFile, spinner, outputFilePath, csvSize, splitWorksheet }
  processWorksheet(parsingConfig)
}
interface FileParserOptions {
  workbook?: XLSX.WorkBook
  sheetName: string
  inputRange: string
  parsedFile: ParsedPath
  spinner: Spinner
  outputFilePath: string
  csvSize: number
  splitWorksheet: boolean
}

async function processWorksheet({ workbook, sheetName, inputRange, parsedFile, spinner, outputFilePath, csvSize, splitWorksheet }: FileParserOptions): void {
  const rawSheet = workbook!.Sheets[sheetName]
  const decodedRange = XLSX.utils.decode_range(inputRange)
  /* let fields: string[] = []
  const data: JsonValue[] = [] */
  const columnIndices = range(decodedRange.s.c, decodedRange.e.c + 1)
  const rowIndices = range(decodedRange.s.r, decodedRange.e.r + 1)
  let fileNum = 1
  let writeStream = fs.createWriteStream(`${outputFilePath}_${padStart(`${fileNum}`, 3, '0')}.csv`, 'utf-8')
  writeStream.on('close', () => {
    fileNum += 1
  })
  const pass = new PassThrough()
  pass.on('data', (chunk) => {
    pass.pause()
    const streamWriteResult = writeStream.write(chunk)
    if (!splitWorksheet) {
      pass.resume()
    }
    else if (!streamWriteResult) {
      writeStream.once('drain', () => {
        ({ writeStream, fileNum } = updateWriteStream(writeStream, csvSize, pass, fileNum, outputFilePath))
      })
    }
    else {
      ({ writeStream, fileNum } = updateWriteStream(writeStream, csvSize, pass, fileNum, outputFilePath))
    }
  })
  pass.on('end', () => {
    spinner.success(
      colors.green('Parsed successfully'),
    )
  })
  rowIndices.forEach((rowIdx) => {
    const rowdata: JsonValue[] = []
    columnIndices.forEach((colIdx) => {
      const currentCell = rawSheet['!data']?.[rowIdx]?.[colIdx]
      rowdata.push((currentCell?.v ?? null) as string)
    })
    if (rowIdx === decodedRange.s.r) {
      rowdata.push('source_file', 'source_sheet', 'source_range')
      const csv = Papa.unparse([rowdata])
      if (splitWorksheet)
        fs.writeFileSync(`${outputFilePath}_HEADER.csv`, csv, 'utf-8')
      else pass.write(`${csv}\n`)
    }
    else {
      rowdata.push(parsedFile.base, sheetName, inputRange)
      const csv = Papa.unparse([rowdata])
      pass.write(`${csv}\n`)
    }
    if (rowIdx === decodedRange.e.r) {
      pass.end()
    }
    // dataForCSV.push(rowdata)
  })
  /* fs.writeFile(`${parsedFile.dir}/${parsedFile.name}_${sheetName}_${dayjs().format('YYYY.MM.DD HH.mm.ss')}.csv`, csv, {
       encoding: 'utf-8',
     }, (err) => {
       if (err)
         throw new Commander.InvalidArgumentError(`There was an error parsing the worksheet ${colors.bold(colors.cyan(`"${sheetName}"`))} from the Excel at ${colors.yellow(`"${filePath}"`)}`)
       spinner.success(colors.green('Parsed successfully'))
     }) */
}
function updateWriteStream(writeStream: fs.WriteStream, csvSize: number, pass: PassThrough, fileNum: number, outputFilePath: string): { writeStream: fs.WriteStream, fileNum: number } {
  if (writeStream.bytesWritten < csvSize) {
    pass.resume()
  }
  else {
    writeStream.destroy()
    fileNum += 1
    writeStream = fs.createWriteStream(`${outputFilePath}_${padStart(`${fileNum}`, 3, '0')}.csv`, 'utf-8')
    pass.resume()
  }
  return { writeStream, fileNum }
}
