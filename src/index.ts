import * as os from 'node:os'
import * as fs from 'node:fs'
import { basename, format, join, parse, relative, sep } from 'node:path'
import type { PassThrough } from 'node:stream'
import { Writable } from 'node:stream'
import type { ParsedPath } from 'node:path/posix'
import Papa from 'papaparse'
import inquirerFileSelector from 'inquirer-file-selector'
import * as XLSX from 'xlsx'
import { counting, inRange, isEmpty, isString, omit, range } from 'radash'
import type { JsonPrimitive, Merge, SetRequired } from 'type-fest'
import colors from 'picocolors'
import { confirm, expand, input, number, select } from '@inquirer/prompts'
import { Separator } from '@inquirer/core'
import fg from 'fast-glob'
import yoctoSpinner from 'yocto-spinner'
import dayjs from 'dayjs'
import { ensureDirSync } from 'fs-extra'

XLSX.set_fs(fs)

const spinner = yoctoSpinner({ text: 'Parsing…' })
export interface Arguments<T extends boolean = false> {
  bytesWritten?: number
  columnIndices?: Generator<number>
  csvFileSize?: number
  csvSizeInMb?: number
  decodedRange?: XLSX.Range
  dirName?: string
  fileNum?: number
  filePath?: string
  isLastRow?: boolean
  outputFile?: ParsedPath
  outputFilePath?: string
  outputFileName?: string
  outputFileDir?: string
  outputFiles?: string[]
  parsedFile?: ParsedPath
  pass?: PassThrough
  range?: string
  rangeIncludesHeader?: boolean
  rawSheet?: XLSX.WorkSheet
  rowCount?: number
  rowIndices?: Generator<number>
  sheetName?: string
  headerRow?: string[]
  Sheets?: { [sheet: string]: XLSX.WorkSheet }
  splitWorksheet?: T
}

class SizeTrackingWritable extends Writable {
  private byteSize: number = 0
  private maxSize: number = 0
  // private _filePath: string
  _fileStream?: fs.WriteStream
  _bytesWritten: number = 0
  _csvFileSize: number = 5 * 1024 * 1024
  _csvSizeInMb: number = 5
  _currentRow: number = 0
  _currentRowData: JsonPrimitive[] = []

  _fileNum: number = 0
  _headerRow: string[] = []
  _isFirstRow: boolean = true
  _isLastRow: boolean = false
  _outputFiles: Omit<ParsedPath, 'base'>[] = []
  _inputFile?: ParsedPath
  _inputFilePath?: string
  _inputRange?: string
  _inputSheetName?: string
  _rangeIncludesHeader?: boolean
  _writeRowCount: number = 0
  _Sheets?: { [sheet: string]: XLSX.WorkSheet }
  _SheetNames?: string[]
  _splitWorksheet: boolean = false
  formattedTimestamp: string = dayjs().format('YYYY.MM.DD HH.mm.ss')
  spinner = yoctoSpinner()

  constructor(args: {
    filePath?: string
    range?: string
    sheetName?: string
  }) {
  // constructor() {
    super()
    // if (!isUndefined(args.filePath))
    this._inputFilePath = args.filePath
    // if (!isUndefined(args.range))
    this._inputRange = args.range
    // if (!isUndefined(args.sheetName))
    this._inputSheetName = args.sheetName
    // this._filePath = ''
    // this.fileStream = fs.createWriteStream(this.filePath)
    // this.fileStream.on('error', (err) => {
    //   spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
    // })
    // this.fileStream.on('close', () => {
    //   spinner.success(`Finished writing ${colors.cyan(`"${this.filePath}"`)}`)
    // })
  }

  // _write(chunk: any, encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
  //   this.byteSize += (chunk as Buffer).length
  //   if (this.isFirstRow && this.splitWorksheet === true && this.args.rangeIncludesHeader === true) {
  //     const outputFilePathClone = omit(clone(this.args.outputFile), ['base'])
  //     outputFilePathClone.name += `_HEADER`

  //     const formattedFilePath = format(outputFilePathClone)
  //     fs.writeFile(formattedFilePath, `${chunk}`, 'utf-8', (err) => {
  //       if (err) {
  //         spinner.error(`There was an error writing the CSV file header: ${colors.red(err.message)}`)
  //         process.exit(1)
  //       }
  //       else {
  //         const summaryPath = format(omit(outputFilePathClone, ['dir']))
  //         this.outputFiles.push(summaryPath)
  //         spinner.text = `Writing header to ${colors.cyan(`"${summaryPath}"`)}\n`
  //         this.incrementRowCount()
  //       }
  //     })
  //   }
  //   else {
  //     if (this.splitWorksheet === true && this.byteSize > (this.maxSize ?? 0)) {
  //       this.fileStream.end()
  //       this.byteSize = (chunk as Buffer).length
  //       this.incrementFileCount()
  //       this.fileStream = fs.createWriteStream(this.filePath, { flags: 'a' })
  //     }
  //     this.fileStream.write(`${chunk}\n`, encoding, callback)
  //     this.incrementRowCount()
  //   }
  // }

  private writeRow(rowData: JsonPrimitive[]): void {
    const rowString = `${Papa.unparse([rowData])}\n`
    const buff = Buffer.from(rowString)

    if (this.rowIsHeaderRow && this._splitWorksheet === true) {
      fs.writeFileSync(format(this.outputFile), buff, 'utf-8')
      this._outputFiles.push({
        file: this.outputFile,
        size: buff.length,
      })
    }
    else {
      if (!this.hasWriteStream()) {
        // ensureDirSync(this.outputFile.dir)
        this._fileStream = this.makeFileStream()
        this.byteSize = 0
      }
      else if (this._splitWorksheet && (this.byteSize + buff.length) > this.maxSize) {
        this._fileStream.end(rowString)
        this._fileStream = this.makeFileStream()
        this.byteSize = 0
      }
      this.byteSize += buff.length
      this._fileStream!.write(rowString)
    }
  }

  private makeFileStream(): fs.WriteStream {
    this.incrementFileCount()
    const currentOutputFile = this.outputFile
    const _fileStream = fs.createWriteStream(format(currentOutputFile))
    _fileStream.on('error', (err) => {
      this.spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
    })
    _fileStream.on('finish', () => {
      this._outputFiles.push({
        file: currentOutputFile,
        size: _fileStream.bytesWritten,
      })
      this.spinner.text = `Finished writing ${colors.cyan(`"${relative(this.inputFile.dir, format(currentOutputFile))}"`)}`
    })
    this.spinner.text = `Writing ${colors.cyan(`"${relative(this.inputFile.dir, format(currentOutputFile))}"`)}\n`
    this.spinner.start()
    return _fileStream
  }

  get inputFile(): ParsedPath {
    return parse(this._inputFilePath ?? '')
  }

  get rowIsHeaderRow(): boolean {
    return this._isFirstRow && this._rangeIncludesHeader!
  }

  get outputFile(): Omit<ParsedPath, 'base'> {
    let outputFileName = `${this.formattedTimestamp} SHEET ${this._inputSheetName}`

    if (this._splitWorksheet === true) {
      if (this.rowIsHeaderRow)
        outputFileName += ' HEADER'
      else
        outputFileName += ` ${this._fileNum}`
    }
    else {
      outputFileName += ' FULL'
    }

    const parsedJobDir = join(this.inputFile.dir, `${this.inputFile.name} PARSE JOBS`)
    ensureDirSync(parsedJobDir)
    return {
      ...omit(parse(this._inputFilePath!), ['base']),
      ext: '.csv',
      name: outputFileName,
      dir: parsedJobDir,
    }
  }

  get relativeOuputFile(): string {
    return relative(this.inputFile.dir, format(this.outputFile))
  }

  iterate() {
    const rawSheet = this._Sheets![this._inputSheetName!]
    for (const rowIdx of this.rowInidices) {
      const unprocessedRowData: JsonPrimitive[] = []
      this._isFirstRow = rowIdx === this.decodedRange.s.r
      this._isLastRow = rowIdx === this.decodedRange.e.r
      for (const colIdx of this.columnIndices) {
        const currentCell = rawSheet['!data']?.[rowIdx]?.[colIdx]
        unprocessedRowData.push((currentCell?.v ?? null) as string)
      }
      const rowData = this.processRowData(unprocessedRowData)
      this.writeRow(rowData)
    }
  }

  private processRowData(rowData: JsonPrimitive[]): JsonPrimitive[] {
    if (this.rowIsHeaderRow) {
      const groupedColumnNames = counting(rowData as string[], v => v)
      const headerRowData = (rowData as string[]).reverse().map((v) => {
        if (groupedColumnNames[v] > 1) {
          const count = groupedColumnNames[v]
          groupedColumnNames[v] -= 1
          return `${v} ${count - 1}`
        }
        return v
      })
        .reverse()
      rowData = headerRowData
      rowData.push('source_file', 'source_sheet', 'source_range')
    }

    else {
      rowData.push(this.inputFile.base, this._inputSheetName!, this._inputRange!)
    }
    return rowData
  }

  // get filePath(): string {
  //   const outputFilePathClone = omit(clone(this.args.outputFile), ['base'])
  //   if (this.args.splitWorksheet === true) {
  //     outputFilePathClone.name = `${outputFilePathClone?.name}_${this.args.fileNum}`
  //     return format(outputFilePathClone)
  //   }
  //   else {
  //     return format(outputFilePathClone)
  //   }
  // }

  // get isLastRow(): boolean {
  //   return this._isLastRow ?? false
  // }

  // set isLastRow(val: boolean) {
  //   this._isLastRow = val
  // }

  // get isFirstRow(): boolean {
  //   return this._isFirstRow ?? false
  // }

  // set isFirstRow(val: boolean) {
  //   this._isFirstRow = val
  // }

  get rowData(): JsonPrimitive[] {
    return this._currentRowData
  }

  set rowData(val: JsonPrimitive[]) {
    this._currentRowData = val
  }

  get decodedRange(): XLSX.Range {
    return XLSX.utils.decode_range(this._inputRange!)
  }

  get rowInidices(): Generator<number> {
    return range(this.decodedRange.s.r, this.decodedRange.e.r)
  }

  get columnIndices(): Generator<number> {
    return range(this.decodedRange.s.c, this.decodedRange.e.c)
  }

  incrementFileCount(): void {
    this._fileNum += 1
  }

  incrementRowCount(): void {
    this._writeRowCount += 1
  }

  getByteSize(): number {
    return this.byteSize
  }

  hasWriteStream(): this is Merge<this, SetRequired<SizeTrackingWritable, '_fileStream'>> {
    return this._fileStream !== undefined
  }

  hasInputFilePath(): this is Merge<this, SetRequired<SizeTrackingWritable, '_inputFilePath'>> {
    return this._inputFilePath !== undefined
  }

  hasInputRange(): this is Merge<this, SetRequired<SizeTrackingWritable, '_inputRange'>> {
    return this._inputRange !== undefined
  }

  hasInputSheetName(): this is Merge<this, SetRequired<SizeTrackingWritable, '_inputSheetName' >> {
    return this._inputSheetName !== undefined
  }

  hasSheetProperties(): this is Merge<this, SetRequired<SizeTrackingWritable, '_Sheets' | '_SheetNames'>> {
    return this._Sheets !== undefined && this._SheetNames !== undefined
  }

  hasRangeIncludesHeaderAnswer(): this is Merge<this, SetRequired<SizeTrackingWritable, '_rangeIncludesHeader' >> {
    return this._rangeIncludesHeader !== undefined
  }

  async setInputFile(): Promise<void> {
    if (this.hasInputFilePath()) {
      // this.spinner.start()
      // this._inputFilePath = resolve(this._inputFilePath)
      if (!fs.existsSync(this._inputFilePath)) {
        this.spinner.text = colors.red(`FILE ${colors.cyan(`"${this._inputFilePath}"`)} NOT FOUND\n`)
        this._inputFilePath = undefined as unknown as string
        await this.setInputFile()
      }
      else {
        this.spinner.text = `Parsing ${colors.cyan(`"${this._inputFilePath}"`)}...\n`
        this.spinner.start()
      }
    }
    else {
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

      const dirName = await select({
        message: 'Where do you want to start looking for your Excel file?',
        pageSize: 20,
        choices: [new Separator('----HOME----'), ...homeFolders, new Separator('----ONEDRIVE----'), ...cloudFolders],
      }, {
        // clearPromptOnDone: false,
      })
      const filePath = await inquirerFileSelector({
        message: 'Navigate to the Excel file you want to parse (only files with the .xls or .xlsx extension will be shown, and the file names must start with an alphanumeric character)',
        basePath: dirName,
        hideNonMatch: true,
        allowCancel: true,
        pageSize: 20,
        theme: {
          style: {
            // answer: (text: string) => colors.cyan(buildFilePath(dirName, text)),
            currentDir: (text: string) => colors.magenta(join(`.`, basename(dirName), relative(dirName, text))),
          },
        },
        match(filePath) {
          if (filePath.isDir) {
            return !filePath.path.split(sep).some(v => /^[^A-Z0-9]/i.test(v))
          }

          return !/^[^A-Z0-9]/i.test(filePath.name) && /\.xlsx?$/.test(filePath.name)
        },
      }).catch((error: string | { name: string }) => {
        if (!isString(error) && error.name === 'AbortPromptError') {
          return 'canceled'
        }
      })
      if (filePath === 'canceled') {
        spinner.error(`Cancelled selection`)
        process.exit(1)
      }
      this._inputFilePath = filePath!
      // this.spinner.start()
      this.spinner.text = `Parsing ${colors.cyan(`"${buildFilePath(dirName, filePath!)}"`)}\n`
      this.spinner.start()
    }
  }

  async setRange(): Promise<void> {
    if (!this.hasInputRange()) {
      const worksheetRange = this._Sheets![this._inputSheetName!]['!ref']!
      const parsedRange = XLSX.utils.decode_range(worksheetRange)
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
        this._inputRange = await input({
          message: 'Enter the range of the worksheet to parse',
          default: worksheetRange,
          validate: (value: string) => {
            const isValidInput = isRangeInDefaultRange(XLSX.utils.decode_range(value))
            if (!isValidInput)
              return `The range must be within the worksheet's default range (${XLSX.utils.encode_range(parsedRange)})`
            return true
          },
        }, {
          // clearPromptOnDone: false,
          signal: AbortSignal.timeout(5000),
        }).catch((error: string | { name: string }) => {
          if (!isString(error) && error.name === 'AbortPromptError') {
            return worksheetRange
          }

          throw error
        })
      }
      else {
        const startRow = await number({
          message: 'Enter the starting row number',
          default: parsedRange.s.r + 1,
          min: parsedRange.s.r + 1,
          max: parsedRange.e.r + 1,
          step: 1,
        })
        const endRow = await number({
          message: 'Enter the ending row number',
          default: parsedRange.e.r + 1,
          min: startRow,
          max: parsedRange.e.r + 1,
          step: 1,
        })
        const startCol = await input({
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

        this._inputRange = `${startCol}${startRow}:${endCol}${endRow}`
      }
      // spinner.text = `Will parse ${colors.cyan(`"${this._inputRange}"`)} from worksheet ${colors.cyan(`"${this._inputSheetName}"`)}.\n`
    }
  }

  setSheetProperties(): this is Merge<this, SetRequired<SizeTrackingWritable, '_Sheets' | '_SheetNames'>> {
    if (!this.hasSheetProperties()) {
      const { SheetNames, Sheets } = XLSX.readFile(this._inputFilePath!, {
        raw: true,
        cellDates: true,
        dense: true,
      })
      this._Sheets = Sheets
      this._SheetNames = SheetNames
      this.spinner.success(`Parsed ${colors.cyan(`"${this.inputFile.base}"`)}\n`)
      return true
    }
    else {
      this.spinner.success(`Parsed ${colors.cyan(`"${this.inputFile.base}"`)}\n`)
      return true
    }
  }

  async setSheetName(): Promise<void> {
    if (!this.hasInputSheetName()) {
      this._inputSheetName = await select({
        message: 'Select the worksheet to parse',
        choices: this._SheetNames!.map((value, i) => ({
          name: `${i + 1}) ${value}`,
          value,
          short: value,
        })),
      }, {
        // clearPromptOnDone: false,
      })
    }
    // spinner.text = `Will parse worksheet ${colors.cyan(`"${this._inputSheetName}"`)} from ${colors.cyan(`"${this.inputFile.base}"`)}.\n`
  }

  async setRangeIncludesHeader(): Promise<void> {
    this._rangeIncludesHeader = await confirm({
      message: `Does range ${colors.cyan(`"${this._inputRange}"`)} include the header row?`,
      default: true,
    })
  }

  async setSplitWorksheet(): Promise<void> {
    this._splitWorksheet = await confirm({
      message: `Would you like to split the output into multiple CSVs of a certain size?`,
      default: false,
    })
    if (this._splitWorksheet) {
      this.maxSize = ((await number({
        message: 'Size of output CSV files (in Mb):',
        default: 5,
        min: 0.5,
        step: 0.5,
        theme: {
          style: {
            answer: (text: string) => isEmpty(text) ? '' : colors.cyan(`${text}Mb`),
            defaultAnswer: (text: string) => colors.cyan(`${text}Mb`),
          },
        },
      })) ?? 5) * 1024 * 1024
    }
  }
}

export async function parseArguments(inputArgs: Pick<Arguments<boolean>, 'filePath' | 'range' | 'sheetName'>): Promise<void> {
  const streamer = new SizeTrackingWritable(inputArgs)
  await streamer.setInputFile()
  streamer.setSheetProperties()
  await streamer.setSheetName()
  await streamer.setRange()
  if (!streamer.hasRangeIncludesHeaderAnswer()) {
    await streamer.setRangeIncludesHeader()
  }
  await streamer.setSplitWorksheet()
  streamer.iterate()
  // args.headerRow = []
  // for (const colIdx of range(args.decodedRange.s.c, args.decodedRange.e.c + 1)) {
  //   if (args.rangeIncludesHeader) {
  //     const cellValue = get(args.rawSheet, `!data[${args.decodedRange.s.r}][${colIdx}].v`, '')
  //     args.headerRow.push(cellValue)
  //   }
  //   const currentCell = get(args.rawSheet, `!data[${firstDataRowIndex}][${colIdx}].v`, null)
  //   firstDataRow.push(currentCell)
  // }
  // if (args.rangeIncludesHeader) {
  //   args.headerRow.push('source_file', 'source_sheet', 'source_range')
  // }
  // firstDataRow.push(args.parsedFile.base, args.sheetName, args.range)
  // const headerRowString = Papa.unparse([args.headerRow])
  // const firstDataRowString = Papa.unparse([firstDataRow])
  // const outputFiles: string[] = []
  // let csvFileSize = (Buffer.from(firstDataRowString).length * (args.decodedRange.e.r - args.decodedRange.s.r + 1)) + (Buffer.from(headerRowString).length)
  // args.csvSizeInMb = csvFileSize * 1.5 / (1024 * 1024)
  // let splitWorksheet = false
  // args.outputFile.name = `${snake(`${args.parsedFile.name} ${args.sheetName}`, { splitOnNumber: true })}_${dayjs().format('YYYY.MM.DD HH.mm.ss')}`
  // if (args.csvSizeInMb > 5) {
  //   splitWorksheet = await confirm({
  //     message: `The size of the resulting CSV file could exceed ${colors.yellow(`${ceil(args.csvSizeInMb)}Mb`)}. Would you like to split the output into multiple CSVs?`,
  //     default: false,
  //   })
  //   if (splitWorksheet) {
  //     args.outputFile.dir = join(args.parsedFile.dir, `PARSE ${dayjs().format('YYYY.MM.DD HH.mm.ss')}`)
  //     ensureDirSync(args.outputFile.dir)
  //     emptyDirSync(args.outputFile.dir)
  //     args.outputFile.name = `${args.parsedFile.name} ${args.sheetName}`
  //     const tempCSVSize = await number({
  //       message: 'Size of output CSV files (in Mb):',
  //       default: 5,
  //       min: 1,
  //       max: ceil(args.csvSizeInMb),
  //       theme: {
  //         style: {
  //           answer: (text: string) => isEmpty(text) ? '' : colors.cyan(`${text}Mb`),
  //           defaultAnswer: (text: string) => colors.cyan(`${text}Mb`),
  //         },
  //       },
  //     })
  //     csvFileSize = tempCSVSize! * 1024 * 1024
  //   }
  // }
  // const writeStream = new SizeTrackingWritable({
  //   ...args,
  //   splitWorksheet,
  //   csvFileSize,
  //   outputFiles,
  // })
  // for (const rowIdx of range(args.decodedRange.s.r, args.decodedRange.e.r + 1)) {
  //   writeStream.isLastRow = rowIdx === args.decodedRange.e.r
  //   writeStream.isFirstRow = rowIdx === args.decodedRange.s.r
  //   const rowData = []
  //   for (const colIdx of range(args.decodedRange.s.c, args.decodedRange.e.c + 1)) {
  //     const currentCell = args.rawSheet['!data']?.[rowIdx]?.[colIdx]
  //     rowData.push((currentCell?.v ?? null) as string)
  //   }
  //   writeStream.rowData = rowData
  //   writeStream.writeRow()
  // }
}

// args.pass = new PassThrough()
// args.pass.on('data', (text: Blob) => {
//     args.pass.pause()
//     const streamWriteResult = args.writeStream.write(text)
//     args.rowCount += 1
//     if (args.splitWorksheet === false) {
//       if (streamWriteResult === false) {
//         args.writeStream.once('drain', () => {
//           if (args.isLastRow) {
//             args.outputFiles.push(`${args.outputFilePath}.csv`)
//             finishParsing(args)
//           }
//           else {
//             args.pass.resume()
//           }
//         })
//       }
//       else {
//         if (args.isLastRow) {
//           args.outputFiles.push(`${args.outputFilePath}.csv`)
//           finishParsing(args)
//         }
//         else {
//           args.pass.resume()
//         }
//       }
//     }
//     else if (streamWriteResult === false) {
//       args.writeStream.once('drain', () => {
//         if (args.isLastRow) {
//           args.outputFiles.push(`${args.outputFilePath}.csv`)
//           finishParsing(args)
//         }
//         else if (args.writeStream.bytesWritten < args.excelFileSize) {
//           args.pass.resume()
//         }
//         else {
//           args.writeStream.destroy()
//           args.outputFiles.push(`${args.outputFilePath}.csv`)
//           args.fileNum += 1
//           args.writeStream = fs.createWriteStream(`${args.outputFilePath}.csv`, 'utf-8')
//           args.writeStream.on('error', (err) => {
//             spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
//             args.pass.destroy()
//           })
//           args.pass.resume()
//         }
//       })
//     }
//     else {
//       if (args.isLastRow) {
//         args.outputFiles.push(`${args.outputFilePath}.csv`)
//         finishParsing(args)
//       }
//       else if (args.writeStream.bytesWritten < args.excelFileSize) {
//         args.pass.resume()
//       }
//       else {
//         args.writeStream.destroy()
//         args.outputFiles.push(`${args.outputFilePath}.csv`)
//         args.fileNum += 1
//         args.writeStream = fs.createWriteStream(`${args.outputFilePath}.csv`, 'utf-8')
//         args.writeStream.on('error', (err) => {
//           spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
//           args.pass.destroy()
//         })
//         args.pass.resume()
//       }
//     }
//   })

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
  const parsedRange = XLSX.utils.decode_range(worksheetRange)
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
      // clearPromptOnDone: false,
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
    // spinner.text = `Will parse ${colors.cyan(`"${args.range}"`)} from worksheet ${colors.cyan(`"${args.sheetName}"`)}.\n`
  }
}

async function chooseSheetToParse({ SheetNames }: { SheetNames: string[] }): Promise<string> {
  return select({
    message: 'Select the worksheet to parse',
    choices: SheetNames.map((value, i) => ({
      name: `${i + 1}) ${value}`,
      value,
      short: value,
    })),
  }, {
    // clearPromptOnDone: false,
  })
}

async function getExcelFilePath(): Promise<SetRequired<Arguments, 'filePath' | 'dirName'>> {
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

  const dirName = await select({
    message: 'Where do you want to start looking for your Excel file?',
    pageSize: 20,
    choices: [new Separator('----HOME----'), ...homeFolders, new Separator('----ONEDRIVE----'), ...cloudFolders],
  }, {
    // clearPromptOnDone: false,
  })
  const filePath = await inquirerFileSelector({
    message: 'Navigate to the Excel file you want to parse (only files with the .xls or .xlsx extension will be shown, and the file names must start with an alphanumeric character)',
    basePath: dirName,
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
  }, { clearPromptOnDone: false }).catch((error: string | { name: string }) => {
    if (error.name === 'AbortPromptError') {
      return 'canceled'
    }
  })
  if (filePath === 'canceled') {
    spinner.error(`Cancelled selection`)
    process.exit(1)
  }
  return args
}
function buildFilePath(dirName: string, text: string): string {
  return `./${basename(dirName)}/${relative(dirName, text)}`
}
