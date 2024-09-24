/* eslint-disable node/prefer-global/buffer */
import * as os from 'node:os'
import * as fs from 'node:fs'
import { basename, join, parse, relative, sep } from 'node:path'
import type { PassThrough } from 'node:stream'
import { Writable } from 'node:stream'
import type { ParsedPath } from 'node:path/posix'
import inquirerFileSelector from 'inquirer-file-selector'
import * as XLSX from 'xlsx'
import Papa from 'papaparse'
import { ceil } from 'lodash-es'
import { inRange, isEmpty, range, snake } from 'radash'
import type { JsonPrimitive, JsonValue, SetRequired } from 'type-fest'
import dayjs from 'dayjs'
import colors from 'picocolors'
import { confirm, expand, input, number, select } from '@inquirer/prompts'
import { Separator } from '@inquirer/core'
import { emptyDirSync, ensureDirSync } from 'fs-extra'
import fg from 'fast-glob'
import { isUndefined } from '@antfu/utils'
import yoctoSpinner from 'yocto-spinner'

XLSX.set_fs(fs)

const spinner = yoctoSpinner({ text: 'Parsing…' })

class SizeTrackingWritable extends Writable {
  private byteSize: number = 0
  private maxSize: number
  private filePath: string
  private fileStream: fs.WriteStream

  constructor(filePath: string, maxSize: number) {
    super()
    this.filePath = filePath
    this.maxSize = maxSize
    this.fileStream = fs.createWriteStream(this.filePath)
    this.fileStream.on('error', (err) => {
      spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
    })
  }

  _write(chunk: string, encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    this.byteSize += chunk.length
    if (this.byteSize > this.maxSize) {
      this.fileStream.end()
      this.byteSize = chunk.length
      this.fileStream = fs.createWriteStream(this.filePath, { flags: 'a' })
    }
    this.fileStream.write(chunk, encoding, callback)
  }

  getByteSize(): number {
    return this.byteSize
  }
}

export interface Arguments {
  args: Pick<Arguments, 'filePath' | 'sheetName' | 'range'>
  bytesWritten?: number
  columnIndices?: Generator<number>
  csvFileSize?: number
  csvSizeInMb?: number
  decodedRange?: XLSX.Range
  dirName?: string
  fileNum?: number
  filePath?: string
  isLastRow?: boolean
  outputFilePath?: string
  outputFileDir?: string
  outputFiles?: string[]
  parsedFile?: ParsedPath
  pass?: PassThrough
  range?: string
  rangeIncludesHeader?: boolean
  rawSheet?: XLSX.WorkSheet
  rowCount?: number
  rowData?: JsonPrimitive[]
  rowIndices?: Generator<number>
  sheetName?: string
  headerRow?: string[]
  Sheets: { [sheet: string]: XLSX.WorkSheet }
  splitWorksheet?: boolean
  writeStream?: SizeTrackingWritable | Writable
}
export async function parseArguments(args: Arguments): Promise<void> {
  if (isUndefined(args.filePath)) {
    await getExcelFilePath(args)
    spinner.text = `Parsing ${colors.cyan(`"${buildFilePath(args, args.filePath!)}"`)}\n`
  }
  else {
    spinner.text = `Parsing ${colors.cyan(`"${args.filePath}"`)}...\n`
  }
  args.parsedFile = parse(args.filePath!)
  const { SheetNames, Sheets } = XLSX.readFile(args.filePath!, {
    raw: true,
    cellDates: true,
    dense: true,
  })
  args.rowCount = 0
  args.Sheets = Sheets

  if (isUndefined(args.sheetName) || !SheetNames.includes(args.sheetName)) {
    args.sheetName = await chooseSheetToParse({ SheetNames })
  }
  if (isUndefined(args.range)) {
    await getWorksheetRange(args)
  }
  args.rawSheet = Sheets[args.sheetName]
  args.decodedRange = XLSX.utils.decode_range(args.range!)
  args.rangeIncludesHeader = await confirm({
    message: `Does range ${colors.cyan(`"${args.range}"`)} include the header row?`,
    default: true,
  })
  args.columnIndices = range(args.decodedRange.s.c, args.decodedRange.e.c + 1)
  args.rowIndices = range(args.decodedRange.s.r, args.decodedRange.e.r + 1)
  const firstDataRowIndex = args.decodedRange.s.r + (args.rangeIncludesHeader ? 1 : 0)
  const firstDataRow = []
  args.headerRow = []
  for (const colIdx of args.columnIndices) {
    if (args.rangeIncludesHeader) {
      args.headerRow.push((args.rawSheet?.['!data']?.[args.decodedRange.s.r]?.[colIdx].v ?? '') as string)
    }
    const currentCell = args.rawSheet['!data']?.[firstDataRowIndex]?.[colIdx].v ?? null
    firstDataRow.push((currentCell) as string)
  }
  if (args.rangeIncludesHeader) {
    args.headerRow.push('source_file', 'source_sheet', 'source_range')
  }
  firstDataRow.push(args.parsedFile.base, args.sheetName, args.range!)
  const headerRowString = Papa.unparse([args.headerRow])
  const firstDataRowString = Papa.unparse([firstDataRow])
  args.outputFiles = []
  args.csvFileSize = (Buffer.from(firstDataRowString).length * (args.decodedRange.e.r - args.decodedRange.s.r + 1)) + (Buffer.from(headerRowString).length)
  args.csvSizeInMb = args.csvFileSize * 1.5 / (1024 * 1024)
  args.splitWorksheet = false
  args.outputFileDir = join(args.parsedFile.dir)
  args.outputFilePath = join(args.outputFileDir, `${snake(`${args.parsedFile.name} ${args.sheetName}`, { splitOnNumber: true })}_${dayjs().format('YYYY.MM.DD HH.mm.ss')}`)
  if (args.csvSizeInMb > 5) {
    args.splitWorksheet = await confirm({
      message: `The size of the resulting CSV file could exceed ${colors.yellow(`${args.csvSizeInMb * 4}Mb`)}. Would you like to split the output into multiple CSVs?`,
      default: false,
    })
    if (args.splitWorksheet) {
      args.outputFileDir = join(args.parsedFile.dir, `PARSE ${dayjs().format('YYYY.MM.DD HH.mm.ss')}`)
      ensureDirSync(args.outputFileDir)
      emptyDirSync(args.outputFileDir)
      args.outputFilePath = join(args.outputFileDir, `${args.parsedFile.name} ${args.sheetName}`)
      const tempCSVSize = await number({
        message: 'Size of output CSV files (in Mb):',
        default: 5,
        min: 1,
        max: ceil(args.csvSizeInMb),
        theme: {
          style: {
            answer: (text: string) => isEmpty(text) ? '' : colors.cyan(`${text}Mb`),
            defaultAnswer: (text: string) => colors.cyan(`${text}Mb`),
          },
        },
      })
      args.csvFileSize = tempCSVSize! * 1024 * 1024
    }
  }
  args.fileNum = 1
  if (args.splitWorksheet) {
    if (args.rangeIncludesHeader) {
      const headerFilePath = `${args.outputFilePath}_HEADER.csv`
      args.outputFiles.push(headerFilePath)
      fs.writeFile(headerFilePath, headerRowString, 'utf-8', (err) => {
        if (err) {
          spinner.error(`There was an error writing the CSV file header: ${colors.red(err.message)}`)
          process.exit(1)
        }
        else {
          spinner.text = `Writing header to ${colors.cyan(`"${headerFilePath}"`)}\n`
        }
      })
    }

    args.writeStream = new SizeTrackingWritable(`${args.outputFilePath}_${args.fileNum}.csv`, args.csvFileSize)
  }
  else {
    args.writeStream = fs.createWriteStream(`${args.outputFilePath}.csv`, 'utf-8')
    if (args.rangeIncludesHeader) {
      args.writeStream.write(Buffer.from(`${headerRowString}\n`))
    }
  }
  args.rowData = []
  for (const rowIdx of args.rowIndices) {
    args.isLastRow = rowIdx === args.decodedRange.e.r
    if (rowIdx === args.decodedRange.s.r) {
      return
    }
    const rowdata: JsonValue[] = []
    for (const colIdx of args.columnIndices) {
      const currentCell = args.rawSheet['!data']?.[rowIdx]?.[colIdx]
      rowdata.push((currentCell?.v ?? null) as string)
    }

    rowdata.push(args.parsedFile.base, args.sheetName, args.range!)
    const rowString = Papa.unparse([rowdata])
    args.writeStream.write(Buffer.from(`${rowString}\n`))
  }
}

// args.pass = new PassThrough()
/* args.pass.on('data', (text: Blob) => {
       args.pass.pause()
       const streamWriteResult = args.writeStream.write(text)
       args.rowCount += 1
       if (args.splitWorksheet === false) {
         if (streamWriteResult === false) {
           args.writeStream.once('drain', () => {
             if (args.isLastRow) {
               args.outputFiles.push(`${args.outputFilePath}.csv`)
               finishParsing(args)
             }
             else {
               args.pass.resume()
             }
           })
         }
         else {
           if (args.isLastRow) {
             args.outputFiles.push(`${args.outputFilePath}.csv`)
             finishParsing(args)
           }
           else {
             args.pass.resume()
           }
         }
       }
       else if (streamWriteResult === false) {
         args.writeStream.once('drain', () => {
           if (args.isLastRow) {
             args.outputFiles.push(`${args.outputFilePath}.csv`)
             finishParsing(args)
           }
           else if (args.writeStream.bytesWritten < args.excelFileSize) {
             args.pass.resume()
           }
           else {
             args.writeStream.destroy()
             args.outputFiles.push(`${args.outputFilePath}.csv`)
             args.fileNum += 1
             args.writeStream = fs.createWriteStream(`${args.outputFilePath}.csv`, 'utf-8')
             args.writeStream.on('error', (err) => {
               spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
               args.pass.destroy()
             })
             args.pass.resume()
           }
         })
       }
       else {
         if (args.isLastRow) {
           args.outputFiles.push(`${args.outputFilePath}.csv`)
           finishParsing(args)
         }
         else if (args.writeStream.bytesWritten < args.excelFileSize) {
           args.pass.resume()
         }
         else {
           args.writeStream.destroy()
           args.outputFiles.push(`${args.outputFilePath}.csv`)
           args.fileNum += 1
           args.writeStream = fs.createWriteStream(`${args.outputFilePath}.csv`, 'utf-8')
           args.writeStream.on('error', (err) => {
             spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
             args.pass.destroy()
           })
           args.pass.resume()
         }
       }
     }) */

function finishParsing(args: Arguments): void {
  const formattedFiles = args.outputFiles!.map(file => `\t${colors.cyan(`"${buildFilePath(args, file)}"`)}`)
  const successMessagePrefix = `SUCCESS! ${colors.yellow(colors.underline(`${args.rowCount} rows written`))}. The output file(s) have been saved to the following location(s):`
  let successMessage = `${colors.green(successMessagePrefix)}\n${formattedFiles.join('\n')}`
  if (args.rangeIncludesHeader) {
    if (args.splitWorksheet)
      successMessage += `\n\n${colors.yellow('NOTE: The header row was included in the output as a separate file. You will have to copy its contents into the Data Loader.\n\n')}`
    else successMessage += `\n\n${colors.yellow('NOTE: The header row was included in the output.\n\n')}`
  }
  else {
    successMessage += `\n\n${colors.yellow('NOTE: The header row was not included in the output. You will have to copy it from the source file into the Data Loader.\n\n')}`
  }
  spinner.start()
  spinner.success(
    successMessage,
  )
}

async function getWorksheetRange(args: Arguments): Promise<void> {
  const worksheetRange = args.Sheets[args.sheetName!]['!ref']
  const parsedRange = XLSX.utils.decode_range(worksheetRange!)
  const isRowInRange = (input: number): boolean => inRange(input, parsedRange.s.r, parsedRange.e.r + 1)
  const isColumnInRange = (input: number): boolean => inRange(input, parsedRange.s.c, parsedRange.e.c + 1)
  const isRangeInDefaultRange = (r: XLSX.Range): boolean => isRowInRange(r.s.r) && isColumnInRange(r.s.c) && isRowInRange(r.e.r) && isColumnInRange(r.e.c)
  const rangeType = await expand({
    message: 'How do you want to specify the range of the worksheet to parse?',
    default: 'e',
    expanded: true,
    choices: [
      {
        name: 'Excel Format (e.g. A1:B10)',
        value: 'Excel Format',
        key: 'e',
      },
      {
        name: 'By specifying the start/end row numbers and the start/end column letters',
        value: 'Numbers and Letters',
        key: 'n',
      },
    ],
  })
  if (rangeType === 'Excel Format') {
    const userRangeInput = await input({
      message: 'Enter the range of the worksheet to parse',
      default: worksheetRange,
      validate: (value: string) => {
        const isValidInput = isRangeInDefaultRange(XLSX.utils.decode_range(value))
        if (!isValidInput)
          return `The range must be within the worksheet's default range (${XLSX.utils.encode_range(parsedRange)})`
        return true
      },
    }, {
      clearPromptOnDone: false,
      signal: AbortSignal.timeout(5000),
    }).catch((error: string | { name: string }) => {
      if (error.name === 'AbortPromptError') {
        return worksheetRange
      }

      throw error
    })
    args.range = userRangeInput
  }
  else {
    const startRow = await number({
      name: 'startRow',
      message: 'Enter the starting row number',
      default: parsedRange.s.r + 1,
      min: parsedRange.s.r + 1,
      max: parsedRange.e.r + 1,
      step: 1,
    })
    const endRow = await number({
      name: 'endRow',
      message: 'Enter the ending row number',
      default: parsedRange.e.r + 1,
      min: startRow,
      max: parsedRange.e.r + 1,
      step: 1,
    })
    const startCol = await input({
      name: 'startCol',
      message: 'Enter the starting column reference (e.g., A)',
      default: XLSX.utils.encode_col(parsedRange.s.c),
      // transformer: (value: number) => `Column "${value}"`,
      validate: (value: string) => {
        const valueIsValid = /^[A-Z]+$/.test(value)
        if (!valueIsValid) {
          return `Invalid column reference. Column references are uppercase letters. The worksheet has data in columns "${XLSX.utils.encode_col(parsedRange.s.c)}" to "${XLSX.utils.encode_col(parsedRange.e.c)}"`
        }
        return true
      },
    })
    const endCol = await input({
      name: 'endCol',
      message: 'Enter the ending column reference (e.g., AB)',
      default: XLSX.utils.encode_col(parsedRange.e.c),
      // transformer: (value: number) => `Column "${value}"`,
      validate: (value: string) => {
        const isGreaterThanOrEqualToStartColumn = XLSX.utils.decode_col(value) >= XLSX.utils.decode_col(startCol)
        const isValidReference = /^[A-Z]+$/.test(value)
        if (!isValidReference) {
          return `Invalid column reference. Column references are uppercase letters. The worksheet has data in columns "${XLSX.utils.encode_col(parsedRange.s.c)}" to "${XLSX.utils.encode_col(parsedRange.e.c)}"`
        }
        else if (!isGreaterThanOrEqualToStartColumn) {
          return `The ending column reference must be greater than or equal to the starting column reference ("${startCol}")`
        }
        return true
      },
    })

    args.range = `${startCol}${startRow}:${endCol}${endRow}`
    spinner.text = `Will parse ${colors.cyan(`"${args.range}"`)} from worksheet ${colors.cyan(`"${args.sheetName}"`)}.\n`
  }
}

async function chooseSheetToParse({ SheetNames }: { SheetNames: string[] }): Promise<string> {
  return select({
    name: 'sheetName',
    message: 'Select the worksheet to parse',
    choices: SheetNames.map((value, i) => ({
      name: `${i + 1}) ${value}`,
      value,
      short: value,
    })),
  }, {
    clearPromptOnDone: false,
  })
}

async function getExcelFilePath(args: Arguments): Promise<SetRequired<Arguments, 'filePath' | 'dirName'>> {
  const cloudFolders = fg.sync(['Library/CloudStorage/**'], {
    onlyDirectories: true,
    absolute: true,
    cwd: os.homedir(),
    deep: 1,
  }).map(folder => ({
    name: basename(folder).replace('OneDrive-SharedLibraries', 'SharePoint-'),
    value: folder,
  }))
  const homeFolders = fg.sync(['Desktop', 'Documents', 'Downloads'], {
    onlyDirectories: true,
    absolute: true,
    cwd: os.homedir(),
    deep: 1,
  }).map(folder => ({
    name: basename(folder),
    value: folder,
  }))

  args.dirName = await select({
    name: 'dirName',
    message: 'Where do you want to start looking for your Excel file?',
    pageSize: 20,
    choices: [new Separator('----HOME----'), ...homeFolders, new Separator('----ONEDRIVE----'), ...cloudFolders],
  }, {
    clearPromptOnDone: false,
  })
  args.filePath = await inquirerFileSelector({
    message: 'Navigate to the Excel file you want to parse (only files with the .xls or .xlsx extension will be shown, and the file names must start with an alphanumeric character)',
    basePath: args.dirName,
    hideNonMatch: true,
    allowCancel: true,
    pageSize: 20,
    theme: {
      style: {
        answer: (text: string) => colors.cyan(buildFilePath(args, text)),
        currentDir: (text: string) => colors.magenta(`./${basename(args.dirName)}/${relative(args.dirName, text)}`),
      },
    },
    match(filePath) {
      if (filePath.isDir) {
        return !filePath.path.split(sep).some(v => /^[^A-Z0-9]/i.test(v))
      }

      return !/^[^A-Z0-9]/i.test(filePath.name) && /\.xlsx?$/.test(filePath.name)
    },
  }).catch((error: string | { name: string }) => {
    if (error.name === 'AbortPromptError') {
      return 'canceled'
    }
  })
  if (args.filePath === 'canceled') {
    spinner.error(`Cancelled selection`)
    process.exit(1)
  }
  return args
}
function buildFilePath(args: Arguments, text: string): string {
  return `./${basename(args.dirName)}/${relative(args.dirName, text)}`
}
