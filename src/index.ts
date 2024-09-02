import * as fs from 'node:fs'
import { join, parse, relative } from 'node:path'
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
import fastGlob from 'fast-glob'

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
  // const { filePath, sheetName, range: inputRange } = args
  const parsedFile = parse(args.filePath)
  spinner.text = `Calculating size of output CSV files for ${colors.cyan(`"${parsedFile.base}"`)}...
`
  const workbook = XLSX.readFile(args.filePath, { raw: true, cellDates: true, dense: true, sheet: args.sheetName })
  let csvSize = fs.statSync(args.filePath).size
  let splitWorksheet = false
  let outputFilePath = `${parsedFile.dir}/${parsedFile.name}_${args.sheetName}_${dayjs().format('YYYY.MM.DD HH.mm.ss')}`
  const csvSizeInMegabytes = ceil(csvSize / 1000000, 2)
  if (csvSizeInMegabytes > 5) {
    splitWorksheet = await confirm({ message: `The size of the resulting CSV file could exceed ${colors.yellow(`${csvSizeInMegabytes * 4}Mb`)}. Would you like to split the output into multiple CSVs?`, default: false })
    if (splitWorksheet) {
      ensureDirSync(join(parsedFile.dir, dayjs().format('YYYY.MM.DD HH.mm.ss')))
      emptyDirSync(join(parsedFile.dir, dayjs().format('YYYY.MM.DD HH.mm.ss')))
      outputFilePath = join(parsedFile.dir, dayjs().format('YYYY.MM.DD HH.mm.ss'), `${parsedFile.name} ${args.sheetName}`)
      const tempCSVSize = await number({ message: 'Size of output CSV files (in Mb):', default: csvSizeInMegabytes, min: 1, max: csvSizeInMegabytes * 4 })
      csvSize = tempCSVSize * 1000000
    }
  }
  const rawSheet = workbook.Sheets[args.sheetName]
  const decodedRange = XLSX.utils.decode_range(args.range)

  const columnIndices = range(decodedRange.s.c, decodedRange.e.c + 1)
  const rowIndices = range(decodedRange.s.r, decodedRange.e.r + 1)
  let fileNum = 1
  let writeStream = splitWorksheet ? fs.createWriteStream(`${outputFilePath}_${padStart(`${fileNum}`, 3, '0')}.csv`, 'utf-8') : fs.createWriteStream(`${outputFilePath}.csv`, 'utf-8')
  const pass = new PassThrough()
  pass.on('end', () => {
    const outputFiles = fastGlob.sync(`${outputFilePath}*.csv`, { cwd: parsedFile.dir, onlyFiles: true }).map(file => `  ${colors.cyan(relative(parsedFile.dir, file))}`)
    spinner.success(
      `${colors.green('SUCCESS!')} The output file(s) have been saved to the following location(s):\n${outputFiles.join('\n')}`,
    )
  })
  writeStream.on('close', () => {
    fileNum += 1
  })
  pass.on('data', (chunk: Blob) => {
    pass.pause()
    const { text, isLastRow } = JSON.parse(chunk) as { text: string, isLastRow: boolean }
    if (!splitWorksheet) {
      if (!writeStream.write(text)) {
        writeStream.once('drain', () => {
          if (isLastRow)
            pass.end()
          else pass.resume()
        })
      }
      else {
        if (isLastRow)
          pass.end()
        else pass.resume()
      }
    }
    else if (!streamWriteResult) {
      writeStream.once('drain', () => {
        ({ writeStream, fileNum } = updateWriteStream(writeStream, csvSize, pass, fileNum, outputFilePath))
      })
    }
    else {
      if (!writeStream.write(text)) {
        writeStream.once('drain', () => {
          if (isLastRow) {
            pass.end()
          }
          else if (writeStream.bytesWritten < csvSize) {
            pass.resume()
          }
          else {
            writeStream.destroy()
            fileNum += 1
            writeStream = fs.createWriteStream(`${outputFilePath}_${padStart(`${fileNum}`, 3, '0')}.csv`, 'utf-8')
            pass.resume()
          }
        })
      }
      else {
        if (isLastRow) {
          pass.end()
        }
        else if (writeStream.bytesWritten < csvSize) {
          pass.resume()
        }
        else {
          writeStream.destroy()
          fileNum += 1
          writeStream = fs.createWriteStream(`${outputFilePath}_${padStart(`${fileNum}`, 3, '0')}.csv`, 'utf-8')
          pass.resume()
        }
      }
    }
  })

  rowIndices.forEach((rowIdx) => {
    const rowdata: JsonValue[] = []
    columnIndices.forEach((colIdx) => {
      const currentCell = rawSheet['!data']?.[rowIdx]?.[colIdx]
      rowdata.push((currentCell?.v ?? null) as string)
    })
    const isLastRow = rowIdx === decodedRange.e.r
    if (rowIdx === decodedRange.s.r) {
      rowdata.push('source_file', 'source_sheet', 'source_range')
      const csv = Papa.unparse([rowdata])
      if (splitWorksheet)
        fs.writeFileSync(`${outputFilePath}_HEADER.csv`, csv, 'utf-8')
      else pass.write(JSON.stringify({ text: `${csv}\n`, isLastRow }))
    }
    else {
      rowdata.push(parsedFile.base, args.sheetName, args.range)
      const csv = Papa.unparse([rowdata])
      pass.write(JSON.stringify({ text: `${csv}\n`, isLastRow }))
    }
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
