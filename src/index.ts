import * as os from 'node:os'
import * as fs from 'node:fs'
import { basename, format, join, parse, relative, sep } from 'node:path'
import type { PassThrough } from 'node:stream'
import { Writable } from 'node:stream'
import type { ParsedPath } from 'node:path/posix'
import Papa from 'papaparse'
import inquirerFileSelector from 'inquirer-file-selector'
import * as XLSX from 'xlsx'
import { clone, counting, inRange, isEmpty, isString, omit, range } from 'radash'
import type { JsonPrimitive, Merge, SetRequired } from 'type-fest'
import colors from 'picocolors'
import { confirm, expand, input, number, select } from '@inquirer/prompts'
import { Separator } from '@inquirer/core'
import fg from 'fast-glob'
import type { Spinner } from 'yocto-spinner'
import yoctoSpinner from 'yocto-spinner'
import dayjs from 'dayjs'
import { ensureDirSync } from 'fs-extra'
import { round } from 'lodash-es'
import { Subject, last, reduce, tap } from 'rxjs'

XLSX.set_fs(fs)

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
  writer$ = new Subject<string>()

  outputFiles$ = this.writer$.pipe(
    reduce<string, {
      file: Omit<ParsedPath, 'base'>
      size: number
      stream: fs.WriteStream
      isHeaderFile: boolean
      writeResult: boolean
      fileNum: number
      rows: number
    }[]>((files, curr, rowIndex) => {
      const outputFileName = `${this.formattedTimestamp} SHEET ${this._inputSheetName}`
      const isHeader = rowIndex === 0 && this._rangeIncludesHeader === true
      // if (this._splitWorksheet === true) {
      //   if (isHeader)
      //     outputFileName += ' HEADER'
      //   else
      //     outputFileName += ` ${files.length}`
      // }
      // else {
      //   outputFileName += ' FULL'
      // }

      const inputFileObject = omit(parse(this._inputFilePath!), ['base'])
      if (isHeader && this._splitWorksheet) {
        const outputFileObject = clone(inputFileObject)
        outputFileObject.ext = '.csv'
        outputFileObject.name = `${outputFileName} HEADER`
        outputFileObject.dir = join(inputFileObject.dir, `${inputFileObject.name} PARSE JOBS`)
        const outputFile = fs.createWriteStream(format(outputFileObject), 'utf-8')

        files.push({
          file: outputFileObject,
          size: Buffer.from(curr).length,
          stream: outputFile,
          isHeaderFile: true,
          writeResult: outputFile.write(curr),
          fileNum: 0,
          rows: 1,
        })
      }
      else {
        const byteSize = Buffer.from(`${curr}\n`).length
        const last = files[files.length - 1]
        if (!last.isHeaderFile && (byteSize + last.size) < this.maxSize) {
          last.writeResult = last.stream.write(`${curr}\n`)
          last.size += byteSize
          last.rows += 1
        }
        else {
          const outputFileObject = clone(inputFileObject)
          outputFileObject.ext = '.csv'
          outputFileObject.name = `${outputFileName} ${last.fileNum + 1}`
          outputFileObject.dir = join(inputFileObject.dir, `${inputFileObject.name} PARSE JOBS`)
          if (!last.writeResult) {
            last.stream.once('drain', () => {
              last.stream.end()
            })
          }
          else {
            last.stream.end()
          }
          const stream = fs.createWriteStream(format(outputFileObject), 'utf-8')
          const nextFile = {
            file: outputFileObject,
            size: byteSize,
            stream,
            isHeaderFile: false,
            writeResult: stream.write(`${curr}\n`),
            fileNum: last.fileNum + 1,
            rows: 1,
          }

          this.spinnerObservable.next({ text: `Writing ${colors.cyan(`"${format(nextFile.file)}"`)}\n` })
          nextFile.stream.on('error', (err) => {
            this.spinner = this.spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
          })

          // stream.on('ready', () => {

          files.push(nextFile)
        }
      }
      return files
    }, []),
  )

  finalFiles$ = this.outputFiles$.pipe(
    tap((files) => {
      files
    }),
    last(),
  )

  _bytesWritten: number = 0
  _csvFileSize: number = 5 * 1024 * 1024
  _csvSizeInMb: number = 5
  _currentRowIndex: number = 0
  _currentRowData: JsonPrimitive[] = []

  _fileNum: number = 0
  _headerRow: string[] = []
  _isFirstRow: boolean = true
  _isLastRow: boolean = false
  _outputFiles: {
    file: Omit<ParsedPath, 'base'>
    size: number
  }[] = []

  _inputFile?: ParsedPath
  _inputFilePath?: string
  _inputRange?: string
  _inputSheetName?: string
  _rangeIncludesHeader?: boolean
  _rowData: JsonPrimitive[][] = []
  _writeRowCount: number = 0
  _Sheets?: { [sheet: string]: XLSX.WorkSheet }
  _SheetNames?: string[]
  _splitWorksheet: boolean = false
  formattedTimestamp: string = dayjs().format('YYYY.MM.DD HH.mm.ss')
  spinnerObservable = new Subject<{
    text: string
    method?: keyof Spinner
  }>()

  spinner = yoctoSpinner()

  constructor(args: {
    filePath?: string
    range?: string
    sheetName?: string
  }) {
    super()
    this._inputFilePath = args.filePath
    this._inputRange = args.range
    this._inputSheetName = args.sheetName
    this.spinnerObservable.subscribe({
      next: ({ text, method = 'start' }) => {
        if (typeof method !== 'undefined' && method in this.spinner && typeof this.spinner[method] === 'function') {
          this.spinner[method](text)
        }
        else {
          this.spinner.start(text)
        }
      },
    })
  }

  get currentRowIndex(): number {
    return this._currentRowIndex
  }

  incrementReadRowCount(): void {
    this._currentRowIndex += 1
  }

  finishParsing(): void {
    // const formattedFiles = this._outputFiles.map(({ file, size }) => `\t${colors.cyan(`"${relative(this.inputFile.dir, format(file))}, ${colors.yellow(`${round(size! / 1024 / 1024, 2)} Mb`)}`)}`)
    let successMessage = `SUCCESS! ${colors.yellow(colors.underline(`${this._writeRowCount} rows written`))}.`
    // let successMessage = `${colors.green(successMessagePrefix)}\n${formattedFiles.join('\n')}`
    if (this.rangeIncludesHeader) {
      if (this._splitWorksheet)
        successMessage += `\n${colors.yellow('NOTE: The header row was included in the output as a separate file. You will have to copy its contents into the Data Loader.\n\n')}`
      else successMessage += `\n${colors.yellow('NOTE: The header row was included in the output.\n\n')}`
    }
    else {
      successMessage += `\n\n${colors.yellow('NOTE: The header row was not included in the output. You will have to copy it from the source file into the Data Loader.\n\n')}`
    }
    this.spinnerObservable.next({
      text: successMessage,
      method: 'success',
    })
  }

  private makeFileStream(): fs.WriteStream {
    this.incrementFileCount()
    const currentOutputFile = this.outputFile
    const _fileStream = fs.createWriteStream(format(currentOutputFile))

    _fileStream.on('close', () => {
      // this._outputFiles.push({
      //   file: currentOutputFile,
      //   size: _fileStream.bytesWritten,
      // })
      this.spinnerObservable.next({
        text: `Finished writing ${colors.yellow(`${round(_fileStream.bytesWritten / 1024 / 1024, 2)} Mb`)} to ${colors.cyan(`"${relative(this.inputFile.dir, format(currentOutputFile))}`)}\n`,
        method: 'success',
      })
    })
    _fileStream.on('error', (err) => {
      this.spinner = this.spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
    })
    // _fileStream.on('ready', () => {
    this.spinnerObservable.next({ text: `Writing ${colors.cyan(`"${relative(this.inputFile.dir, format(currentOutputFile))}"`)}\n` })
    // })

    return _fileStream
  }

  get inputFile(): ParsedPath {
    return parse(this._inputFilePath ?? '')
  }

  get rangeIncludesHeader(): boolean {
    return this._rangeIncludesHeader ?? false
  }

  get isLastRow(): boolean {
    return (this._currentRowIndex + this.decodedRange.s.r) === this.decodedRange.e.r
  }

  get isSeparateHeaderRow(): boolean {
    return this._currentRowIndex === 0 && this._rangeIncludesHeader === true && this._splitWorksheet === true
  }

  get rowIsHeaderRow(): boolean {
    return this._currentRowIndex === 0 && this._rangeIncludesHeader === true
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

  iterate(): void {
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
      this._rowData.push(rowData)
      this.writer$.next(Papa.unparse([rowData]))
    }
    this.writer$.complete()
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

  incrementWriteRowCount(): void {
    this._writeRowCount += 1
  }

  getByteSize(): number {
    return this.byteSize
  }

  hasWriteStream(): this is Merge<this, SetRequired<SizeTrackingWritable, '_outputStream'>> {
    return this._outputStream !== undefined
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
      if (!fs.existsSync(this._inputFilePath)) {
        this.spinnerObservable.next({
          text: colors.red(`FILE ${colors.cyan(`"${this._inputFilePath}"`)} NOT FOUND\n`),
          method: 'error',
        })
        this._inputFilePath = undefined as unknown as string
        await this.setInputFile()
      }
      else {
        this.spinnerObservable.next({ text: `Parsing ${colors.cyan(`"${this._inputFilePath}"`)}...\n` })
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

      })
      const filePath = await inquirerFileSelector({
        message: 'Navigate to the Excel file you want to parse (only files with the .xls or .xlsx extension will be shown, and the file names must start with an alphanumeric character)',
        basePath: dirName,
        hideNonMatch: true,
        allowCancel: true,
        pageSize: 20,
        theme: {
          style: {

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
        this.spinnerObservable.next({
          text: `Cancelled selection`,
          method: 'error',
        })
        setTimeout(() => {
          process.exit(1)
        }, 1500)
      }
      this._inputFilePath = filePath!
      const parsedJobDir = join(this.inputFile.dir, `${this.inputFile.name} PARSE JOBS`)
      ensureDirSync(parsedJobDir)
      this.spinnerObservable.next({ text: `Parsing ${colors.cyan(`"${buildFilePath(dirName, filePath!)}"`)}\n` })
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
      this.spinnerObservable.next({
        text: `Parsed ${colors.cyan(`"${this.inputFile.base}"`)}\n`,
        method: 'success',
      })
      return true
    }
    else {
      this.spinnerObservable.next({
        text: `Parsed ${colors.cyan(`"${this.inputFile.base}"`)}\n`,
        method: 'success',
      })
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

      })
    }
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
        min: 0.25,
        step: 0.25,
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
  streamer.finalFiles$.subscribe({
    next: (files) => {
      // files.forEach((file) => {
      //   file.stream.on('finish', () => {
      //     // this._outputFiles.push({
      //     //   file: currentOutputFile,
      //     //   size: stream.bytesWritten,
      //     // })
      //     // this.finalFiles$.next(nextFile)
      //     streamer.spinner = yoctoSpinner()
      //     streamer.spinner.text = `Finished writing ${colors.yellow(`${round(file.size / 1024 / 1024, 2)} Mb`)} to ${colors.cyan(`"${format(file.file)}`)}\n`
      //   })
      // })
      streamer.finishParsing()
    },
    error: (err) => {
      streamer.spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
    },
    complete: () => {
      streamer.spinner.stop()
    },
  })
  streamer.iterate()
}

function buildFilePath(dirName: string, text: string): string {
  return `./${basename(dirName)}/${relative(dirName, text)}`
}
