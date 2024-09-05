import * as os from 'node:os'
import * as fs from 'node:fs'
import { basename, join, parse, relative, sep } from 'node:path'
import { PassThrough } from 'node:stream'
import inquirerFileSelector from 'inquirer-file-selector'
import * as XLSX from 'xlsx'
import Papa from 'papaparse'
import { ceil, padStart, range } from 'lodash-es'
import type { JsonValue } from 'type-fest'
import dayjs from 'dayjs'
import colors from 'picocolors'
import { confirm, input, number, select } from '@inquirer/prompts'
import { Separator } from '@inquirer/core'
import { emptyDirSync, ensureDirSync } from 'fs-extra'
import fg from 'fast-glob'
import { isUndefined } from '@antfu/utils'
import yoctoSpinner from 'yocto-spinner'

XLSX.set_fs(fs)

const spinner = yoctoSpinner({ text: 'Parsing…' })

export interface Arguments {
  filePath?: string
  sheetName?: string
  range?: string
}
export async function parseArguments(args: { filePath?: string | undefined, sheetName?: string | undefined, range?: string | undefined }): Promise<void> {
  if (isUndefined(args.filePath)) {
    const filePath = await getExcelFilePath()
    args.filePath = filePath
  }
  spinner.text = `Parsing ${colors.cyan(`"${args.filePath}"`)}...\n`

  const { SheetNames, Sheets } = XLSX.readFile(args.filePath, { raw: true, cellDates: true, dense: true })
  let csvSize = fs.statSync(args.filePath).size
  if (isUndefined(args.sheetName) || !SheetNames.includes(args.sheetName)) {
    const answer = await chooseSheetToParse(SheetNames)
    args.sheetName = answer
  }
  if (isUndefined(args.range)) {
    const answer = await getWorksheetRange(Sheets, args)
    args.range = answer
  }
  const parsedFile = parse(args.filePath)
  const rawSheet = Sheets[args.sheetName]
  const decodedRange = XLSX.utils.decode_range(args.range)

  const columnIndices = range(decodedRange.s.c, decodedRange.e.c + 1)
  const rowIndices = range(decodedRange.s.r, decodedRange.e.r + 1)
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
  let fileNum = 1
  let writeStream = splitWorksheet ? fs.createWriteStream(`${outputFilePath}_${padStart(`${fileNum}`, 3, '0')}.csv`, 'utf-8') : fs.createWriteStream(`${outputFilePath}.csv`, 'utf-8')
  const pass = new PassThrough()
  pass.on('end', () => {
    const outputFiles = fg.sync(`${outputFilePath}*.csv`, { cwd: parsedFile.dir, onlyFiles: true }).map(file => `  ${colors.cyan(relative(parsedFile.dir, file))}`)
    spinner.success(
      `${colors.green('SUCCESS!')} The output file(s) have been saved to the following location(s):\n${outputFiles.join('\n')}`,
    )
  })
  pass.on('data', (chunk: Blob) => {
    pass.pause()
    const { text, isLastRow } = JSON.parse(chunk.toString()) as { text: string, isLastRow: boolean }
    const streamWriteResult = writeStream.write(text)
    if (splitWorksheet === false) {
      if (streamWriteResult === false) {
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
    else if (streamWriteResult === false) {
      writeStream.once('drain', () => {
        ({ writeStream, fileNum } = updateWriteStream(isLastRow, pass, writeStream, csvSize, fileNum, outputFilePath))
      })
    }
    else {
      ({ writeStream, fileNum } = updateWriteStream(isLastRow, pass, writeStream, csvSize, fileNum, outputFilePath))
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
      rowdata.push(parsedFile.base, args.sheetName!, args.range!)
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
      rowdata.push(parsedFile.base, args.sheetName!, args.range!)
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
function updateWriteStream(isLastRow: boolean, pass: PassThrough, writeStream: fs.WriteStream, csvSize: number, fileNum: number, outputFilePath: string): { writeStream: fs.WriteStream, fileNum: number } | { writeStream: fs.WriteStream, fileNum: number } {
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
    writeStream.on('error', (err) => {
      spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
      pass.destroy()
    })
    pass.resume()
  }
  return { writeStream, fileNum }
}

async function getWorksheetRange(Sheets: { [sheet: string]: XLSX.WorkSheet }, args: { filePath?: string | undefined, sheetName?: string | undefined, range?: string | undefined }): Promise<string> {
  return input({ name: 'range', message: 'Enter the range of the worksheet to parse', default: Sheets[args.sheetName!]['!ref'] })
}

async function chooseSheetToParse(SheetNames: string[]): Promise<string> {
  return select({ name: 'sheetName', message: 'Select the worksheet to parse', choices: SheetNames.map(value => ({ name: value, value })) })
}

async function getExcelFilePath(): Promise<string> {
  const cloudFolders = fg.sync(['Library/CloudStorage/**'], { onlyDirectories: true, absolute: true, cwd: os.homedir(), deep: 1 }).map(folder => ({ name: basename(folder).replace('OneDrive-SharedLibraries', 'SharePoint-'), value: folder }))
  const homeFolders = fg.sync(['Desktop', 'Documents', 'Downloads'], { onlyDirectories: true, absolute: true, cwd: os.homedir(), deep: 1 }).map(folder => ({ name: basename(folder), value: folder }))

  const dirName = await select({
    name: 'dirName',
    message: 'Where do you want to start looking for your Excel file?',
    pageSize: 20,
    choices: [new Separator('----HOME----'), ...homeFolders, new Separator('----ONEDRIVE----'), ...cloudFolders],
  })
  const filePath = await inquirerFileSelector({
    message: 'Navigate to the Excel file you want to parse',
    basePath: dirName,
    hideNonMatch: true,
    allowCancel: true,
    pageSize: 20,
    match(filePath) {
      const isValidFilePath = !filePath.path.split(sep).some(v => v.startsWith('.'))

      return isValidFilePath && (filePath.isDir || /\.xlsx?$/.test(filePath.name))
    },
  })
  return filePath
}
