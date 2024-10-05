import * as os from 'node:os'
import * as fs from 'node:fs'
import { readFile } from 'node:fs/promises'
import { basename, join, parse, relative, sep } from 'node:path'
import { Writable } from 'node:stream'
import type { ParsedPath } from 'node:path/posix'
import Papa from 'papaparse'
import inquirerFileSelector from 'inquirer-file-selector'
import * as XLSX from 'xlsx'
import { counting, inRange, isEmpty, range } from 'radash'
import type { JsonPrimitive, Merge, SetRequired } from 'type-fest'
import colors from 'picocolors'
import { confirm, expand, input, number, select } from '@inquirer/prompts'
import { Separator } from '@inquirer/core'
import fg from 'fast-glob'
import ora from 'ora'
import dayjs from 'dayjs'
import { BehaviorSubject, Subject, concatMap, filter, forkJoin, from, last, map, of, range as range$, reduce, switchMap, take, tap, withLatestFrom } from 'rxjs'
import { round } from 'lodash-es'
/* async_RS reads a stream and returns a Promise resolving to a workbook */
const spinner = ora({ text: 'PARSING' })
spinner.stop()
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
const fileSelector$ = from(select({
  message: 'Where do you want to start looking for your Excel file?',
  pageSize: 20,
  choices: [new Separator('----HOME----'), ...homeFolders, new Separator('----ONEDRIVE----'), ...cloudFolders],
})).pipe(
  concatMap(basePath => from(inquirerFileSelector({
    message: 'Navigate to the Excel file you want to parse (only files with the .xls or .xlsx extension will be shown, and the file names must start with an alphanumeric character)',
    basePath,
    hideNonMatch: true,
    allowCancel: true,
    pageSize: 20,
    theme: {
      style: {

        currentDir: (text: string) => colors.magenta(join(`.`, basename(basePath), relative(basePath, text))),
      },
    },
    match(filePath) {
      if (filePath.isDir) {
        return !filePath.path.split(sep).some(v => /^[^A-Z0-9]/i.test(v))
      }

      return !/^[^A-Z0-9]/i.test(filePath.name) && /\.xlsx?$/.test(filePath.name)
    },
  }))),
  last(),
)
// const dirName = select({
//   message: 'Where do you want to start looking for your Excel file?',
//   pageSize: 20,
//   choices: [new Separator('----HOME----'), ...homeFolders, new Separator('----ONEDRIVE----'), ...cloudFolders],
// })
export class SizeTrackingWritable extends Writable {
  private byteSize: number = 0
  private maxSize: number = 0

  inputPath$ = new BehaviorSubject<string>('')
  inputStream$ = this.inputPath$.pipe(
    filter(value => typeof value !== 'undefined' && value !== '' && fs.existsSync(value)),
    // switchMap(value => iif(() => typeof value === 'undefined' || !fs.existsSync(value), fileSelector$, of(value))),
    tap((value) => {
      value
    }),
    tap((value) => {
      this._inputFilePath = value
      const parsedInputPath = parse(value)
      spinner.text = `Parsing ${colors.cyan(`"${parsedInputPath.base}"`)}...`
    }),
    switchMap(value => from(readFile(value))),
  )

  inputWb$ = this.inputStream$.pipe(
    concatMap((buffer) => {
      return of(XLSX.read(buffer, {
        raw: true,
        cellDates: true,
        dense: true,
      }))
    }),
    tap((wb) => {
      const parsedInputPath = parse(this._inputFilePath!)
      spinner.text = `Parsed ${colors.cyan(`"${parsedInputPath.base}"`)}`
      spinner.succeed()
      const { SheetNames, Sheets } = wb
    }),
  )

  inputSheet$ = this.inputWb$.pipe(
    filter(wb => typeof wb !== 'undefined'),
    take(1),
    concatMap(sheet => from(this.setSheetName(sheet))),
    filter(value => typeof value !== 'undefined'),
    tap((val) => {
      val
    }),
  )

  inputRange$ = forkJoin([this.inputWb$, this.inputSheet$]).pipe(
    filter(([wb, sheetName]) => typeof wb !== 'undefined' && typeof sheetName !== 'undefined' && sheetName in wb.Sheets),
    take(1),
    concatMap(([workbook, sheetName]) => from(this.setRange(workbook, sheetName))),
    tap((range) => {
      range
    }),
  )

  rowIterator = this.inputRange$.pipe(
    concatMap((range) => {
      const decodedRange = XLSX.utils.decode_range(range)
      return range$(decodedRange.s.r, decodedRange.e.r)
    }),
  )

  columnIterator = this.inputRange$.pipe(
    concatMap((range) => {
      const decodedRange = XLSX.utils.decode_range(range)
      return range$(decodedRange.s.c, decodedRange.e.c)
    }),
  )

  reader$ = new Subject<string>()
  transformer$ = this.reader$.pipe(
    reduce((acc, curr, index) => {
      const bufferedData = Buffer.from(`${curr}\n`)
      const byteSize = bufferedData.length
      const last = acc[acc.length - 1]
      if (typeof last === 'undefined') {
        acc.push({
          buff: bufferedData,
          isHeader: index === 0 && this._rangeIncludesHeader === true,
        })
      }
      else if ((byteSize + last.buff.length) >= this.maxSize || last.isHeader) {
        const next = {
          buff: bufferedData,
          isHeader: false,
        }
        acc.push(next)
      }
      else {
        last.buff = Buffer.concat([last.buff, bufferedData])
      }
      return acc
    }, [] as {
      buff: Buffer
      isHeader: boolean
    }[]),
  )

  iterator$ = forkJoin<[XLSX.WorkBook, string, string]>([this.inputWb$, this.inputSheet$, this.inputRange$]).pipe(
    map(([inputWb, inputSheet, inputRange]) => {
      spinner.text = `Iterating through ${colors.cyan(`"${inputRange}"`)} in ${colors.cyan(`"${inputSheet}"`)}...`
      spinner.start()
      const rawSheet = inputWb.Sheets[inputSheet]['!data']
      if (typeof rawSheet === 'undefined') {
        ora().fail(colors.magenta(`Sheet ${colors.cyan(`"${inputSheet}"`)} not found in ${colors.cyan(`"${this.inputFile.base}"`)}\n`))
      }
      return {
        rawSheet,
        sheet: inputSheet,
      }
    }),
    withLatestFrom(this.rowIterator, this.columnIterator),
    map(([{ rawSheet, sheet }, rowIdx, colIdx]) => {
      spinner.text = `Iterating through rows in ${colors.cyan(`"${sheet}"`)}...`
      spinner.start()
      const unprocessedRowData: JsonPrimitive[] = []
      const isFirstRow = rowIdx === this.decodedRange.s.r
      const currentCell = rawSheet?.[rowIdx]?.[colIdx]
      unprocessedRowData.push((currentCell?.v ?? null) as string)
      if (isFirstRow && this._rangeIncludesHeader === true) {
        const groupedColumnNames = counting(unprocessedRowData as string[], v => v)
        const headerRowData = (unprocessedRowData as string[]).reverse().map((v) => {
          if (groupedColumnNames[v] > 1) {
            const count = groupedColumnNames[v]
            groupedColumnNames[v] -= 1
            return `${v} ${count - 1}`
          }
          return v
        })
          .reverse()
        headerRowData.push('source_file', 'source_sheet', 'source_range')
        return Papa.unparse([unprocessedRowData])
      }

      else {
        unprocessedRowData.push(this.inputFile.base, sheet, this._inputRange!)
        return Papa.unparse([unprocessedRowData])
      }
    }),
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
  _inputFileBuffer: Buffer[] = []
  _inputRange?: string
  _inputSheetName?: string
  _rangeIncludesHeader?: boolean
  _rowData: JsonPrimitive[][] = []
  _writeRowCount: number = 0
  _Sheets?: { [sheet: string]: XLSX.WorkSheet }
  _SheetNames?: string[]
  _splitWorksheet: boolean = false
  formattedTimestamp: string = dayjs().format('YYYY.MM.DD HH.mm.ss')

  constructor(args: {
    filePath?: string
    range?: string
    sheetName?: string
  }) {
    super()
    this._inputFilePath = args.filePath
    this._inputRange = args.range
    this._inputSheetName = args.sheetName
    if (!this._inputFilePath || !fs.existsSync(this._inputFilePath)) {
      fileSelector$.pipe(tap(value =>
        this._inputFilePath = value)).subscribe(this.inputPath$)
    }
    else {
      this.inputPath$.next(this._inputFilePath)
    }
    // this.inputPath$.complete()
    // this.inputPath$.pipe(
    //   // filter((value): value is string => typeof value !== 'undefined' && fs.existsSync(value)),
    //   // filter(),
    // take(1),// ).sub
    // scribe((value) => {
    //   const parsedInputPath = parse(value)
    //   const buff = oraPromise(readFile(value), {
    //     text: `Parsing ${colors.cyan(`"${parsedInputPath.base}"`)}`,
    //     successText: `Parsed ${colors.cyan(`"${parsedInputPath.base}"`)}`,
    //   })
    //   void buff.then((buffer) => {
    //     this.inputStream$.next(buffer)
    //     this.inputStream$.complete()
    //   })
    // })
    this.iterator$.subscribe(this.reader$)
    // this.inputPath$.next(args.filePath)
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
    if (this._rangeIncludesHeader === true) {
      if (this._splitWorksheet)
        successMessage += `\n${colors.yellow('NOTE: The header row was included in the output as a separate file. You will have to copy its contents into the Data Loader.\n\n')}`
      else successMessage += `\n${colors.yellow('NOTE: The header row was included in the output.\n\n')}`
    }
    else {
      successMessage += `\n\n${colors.yellow('NOTE: The header row was not included in the output. You will have to copy it from the source file into the Data Loader.\n\n')}`
    }
    spinner.text = successMessage
    spinner.succeed()
  }

  get inputFile(): ParsedPath {
    return parse(this._inputFilePath ?? '')
  }

  iterate(wb: XLSX.WorkBook, sheet: string, range: string): void {
    spinner.text = `Iterating through rows in ${colors.cyan(`"${sheet}"`)}...`
    spinner.start()
    let rawSheet = this._Sheets![sheet]
    if (typeof rawSheet === 'undefined') {
      const { Sheets } = XLSX.readFile(this._inputFilePath!, {
        raw: true,
        cellDates: true,
        dense: true,
        sheet,
      })
      rawSheet = Sheets[sheet]
    }
    if (typeof rawSheet === 'undefined') {
      spinner.text = colors.magenta(`Sheet ${colors.cyan(`"${sheet}"`)} not found in ${colors.cyan(`"${this.inputFile.base}"`)}\n`)
      spinner.fail()
    }
    else {
      for (const rowIdx of this.rowInidices) {
        const unprocessedRowData: JsonPrimitive[] = []
        const isFirstRow = rowIdx === this.decodedRange.s.r
        for (const colIdx of this.columnIndices) {
          const currentCell = rawSheet['!data']?.[rowIdx]?.[colIdx]
          unprocessedRowData.push((currentCell?.v ?? null) as string)
        }
        if (isFirstRow && this._rangeIncludesHeader === true) {
          const groupedColumnNames = counting(unprocessedRowData as string[], v => v)
          const headerRowData = (unprocessedRowData as string[]).reverse().map((v) => {
            if (groupedColumnNames[v] > 1) {
              const count = groupedColumnNames[v]
              groupedColumnNames[v] -= 1
              return `${v} ${count - 1}`
            }
            return v
          })
            .reverse()
          headerRowData.push('source_file', 'source_sheet', 'source_range')
          this.reader$.next(Papa.unparse([unprocessedRowData]))
        }

        else {
          unprocessedRowData.push(this.inputFile.base, sheet, this._inputRange!)
          this.reader$.next(Papa.unparse([unprocessedRowData]))
        }
      }
      this.reader$.complete()
    }
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

  async setRange(wb: XLSX.WorkBook, sheetName: string): Promise<string> {
    const worksheetRange = wb.Sheets[sheetName]['!ref']!
    const parsedRange = XLSX.utils.decode_range(this._inputRange ?? worksheetRange)
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
      return input({
        message: 'Enter the range of the worksheet to parse',
        default: this._inputRange ?? worksheetRange,
        validate: (value: string) => {
          const isValidInput = isRangeInDefaultRange(XLSX.utils.decode_range(value))
          if (!isValidInput)
            return `The range must be within the worksheet's default range (${XLSX.utils.encode_range(parsedRange)})`
          return true
        },
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

      return `${startCol}${startRow}:${endCol}${endRow}`
    }
  }

  // async setSheetProperties(): Promise<void> {
  //   const wb = await oraPromise(async_RS(fs.createReadStream(this._inputFilePath!)), {
  //     text: `Parsing ${colors.cyan(`"${this.inputFile.base}"`)}`,
  //     successText: `Parsed ${colors.cyan(`"${this.inputFile.base}"`)}`,
  //   })

  //   const { SheetNames, Sheets } = wb
  //   this._Sheets = Sheets
  //   this._SheetNames = SheetNames
  // }

  async setSheetName(wb: XLSX.WorkBook): Promise<string | undefined> {
    return select({
      message: 'Select the worksheet to parse',
      choices: wb.SheetNames.map((value, i) => ({
        name: `${i + 1}) ${value}`,
        value,
        short: value,
      })),
    })
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
  // await streamer.setInputFile()
  // streamer.spinner = this.spinner.start(`Parsing ${colors.cyan(`"${parse(streamer._inputFilePath!).base}"`)}...`)
  // await streamer.setSheetProperties()
  // if (!streamer.hasRangeIncludesHeaderAnswer()) {
  //   await streamer.setRangeIncludesHeader()
  // }
  // await streamer.setSplitWorksheet()
  // streamer.transformer$.pipe(
  //   mergeAll(),
  //   withLatestFrom(streamer.transformer$),
  //   map(([file, files], fileIndex) => {
  //     const headerIncluded = files.some(file => file.isHeader)
  //     const fileCount = headerIncluded ? files.length - 1 : files.length
  //     const getFileNum = (input: number): number => headerIncluded ? input : (input - 1)
  //     const fileCountText = (input: number): string => padStart(`${input}`, `${fileCount}`.length, '0')
  //     const inputFileObject = omit(parse(streamer._inputFilePath!), ['base'])
  //     const outputFileObject = clone(inputFileObject)
  //     const outputDirectoryWithTimestamp = join(inputFileObject.dir, `${inputFileObject.name} PARSE JOBS ${streamer.formattedTimestamp}`)
  //     outputFileObject.dir = outputDirectoryWithTimestamp
  //     ensureDirSync(outputDirectoryWithTimestamp)
  //     const outputFileName = `SHEET ${streamer._inputSheetName} FILE`
  //     const isHeader = file.isHeader
  //     const dataLength = file.buff.length
  //     const writeSize = getFileSizeString(dataLength)
  //     if (isHeader && streamer._splitWorksheet) {
  //       outputFileObject.ext = '.csv'
  //       outputFileObject.name = `${outputFileName} HEADER`
  //       // const outputFile = writeFile(format(outputFileObject), file.buff, { encoding: 'utf8' })
  //       const logString = `"${relative(inputFileObject.dir, format(outputFileObject))}"`
  //       return of(oraPromise(writeFile(format(outputFileObject), file.buff, { encoding: 'utf8' }), {
  //         text: colors.cyan(logString),
  //         successText: colors.green(`${logString}: ${colors.yellow(writeSize)}`),
  //         failText: colors.magenta(logString),
  //       }))
  //     }
  //     else {
  //       const fileNum = getFileNum(fileIndex)
  //       outputFileObject.ext = '.csv'
  //       outputFileObject.name = `${outputFileName} ${fileCountText(fileNum)} of ${fileCountText(fileCount)}`
  //       const logString = `"${relative(inputFileObject.dir, format(outputFileObject))}"`
  //       // const outputFile = writeFile(format(outputFileObject), file.buff, { encoding: 'utf8' })
  //       return of(oraPromise(writeFile(format(outputFileObject), file.buff, { encoding: 'utf8' }), {
  //         text: colors.cyan(logString),
  //         successText: colors.green(`${logString}: ${colors.yellow(writeSize)}`),
  //         failText: colors.magenta(logString),
  //       }))
  //     }
  //   }),
  //   toArray(),
  // )
  // streamer.iterate()
}

function getFileSizeString(dataLength: number): string {
  return dataLength > (1024 * 1.1) ? dataLength > ((1024 ^ 2) * 1.1) ? `${round(dataLength / (1024 ^ 2), 2)} Mb` : `${round(dataLength / 1024, 2)} Kb` : `${dataLength} bytes`
}

function buildFilePath(dirName: string, text: string): string {
  return `./${basename(dirName)}/${relative(dirName, text)}`
}
