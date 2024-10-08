import { inspect } from 'node:util'
import * as fs from 'node:fs'
import { readFile } from 'node:fs/promises'
import type { ParsedPath } from 'node:path'
import { basename, format, join, parse, relative, sep } from 'node:path'
import { homedir } from 'node:os'
import { log } from 'node:console'
import fg from 'fast-glob'
import Papa from 'papaparse'
import inquirerFileSelector from 'inquirer-file-selector'
import * as XLSX from 'xlsx'
import { counting, inRange, isEmpty, omit } from 'radash'
import type { JsonPrimitive } from 'type-fest'
import colors from 'picocolors'
import { confirm, expand, input, number, select } from '@inquirer/prompts'
import { Separator } from '@inquirer/core'
import ora from 'ora'
import { BehaviorSubject, Subject, concatMap, forkJoin, from, map, range as range$, reduce, switchMap, tap, withLatestFrom } from 'rxjs'
import { isUndefined, round } from 'lodash-es'
import type { Arguments } from './arguments'
/* async_RS reads a stream and returns a Promise resolving to a workbook */

XLSX.set_fs(fs)

const fileSelector$ = from(selectStartingFolder()).pipe(
  switchMap(basePath => from(selectExcelFile(basePath))),
)
const inputPath$ = new BehaviorSubject<string>('')

const inputSheet$ = new Subject<string | undefined>()
const inputRange$ = new Subject<string | undefined>()
const rangeIncludesHeader$ = new BehaviorSubject<boolean>(true)
const splitWorksheet$ = new BehaviorSubject<boolean>(false)
const maxSize$ = new BehaviorSubject<number>(5 * 1024 * 1024)
const outputStream$ = new Subject<Buffer[]>()
const parsedInputPath$ = new Subject<ParsedPath>()
// const spinner = new Subject<['start' | 'succeed' | 'fail' | 'info', Exclude<Parameters<typeof ora>[0], string | undefined>]>()
// spinner.subscribe({
//   next([method, value]) {
//     ora({
//       ...value,
//       discardStdin: false,
//     })[method]()
//   },
// })

const inputWb$ = parsedInputPath$.pipe(
  // tap((value) => {
  //   spinner.next(['start', {
  //     text: `Parsing ${colors.cyan(`"${value.base}"`)}...`,
  //   }])
  // }),
  switchMap(value => from(readFile(format(value))).pipe(
    map((buffer) => {
      return XLSX.read(buffer, {
        raw: true,
        cellDates: true,
        dense: true,
      })
    }),
    tap((wb) => {
      return wb
    }),
  )),
)

const parsedOutputFilePath$ = parsedInputPath$.pipe(
  map((value) => {
    const outputFile = omit(value, ['base'])
    outputFile.ext = '.csv'
    outputFile.dir = join(outputFile.dir, `${outputFile.name} PARSE JOBS ${new Date().toISOString()
      .replace(/:/g, '-')}`)
    return outputFile
  }),
)

// const inputSheet$ = inputWb$.pipe(
//   filter(wb => typeof wb !== 'undefined'),
//   take(1),
//   concatMap(sheet => from(setSheetName(sheet))),
//   filter(value => typeof value !== 'undefined'),
//   tap((val) => {
//     val
//   }),
// )

// const inputRange$ = forkJoin([inputWb$, inputSheet$]).pipe(
//   filter(([wb, sheetName]) => typeof wb !== 'undefined' && typeof sheetName !== 'undefined' && sheetName in wb.Sheets),
//   take(1),
//   concatMap(([workbook, sheetName]) => from(setRange(workbook, sheetName))),
//   tap((range) => {
//     range
//   }),
// )

const rowIterator = inputRange$.pipe(
  concatMap((range) => {
    const decodedRange = XLSX.utils.decode_range(range)
    return range$(decodedRange.s.r, decodedRange.e.r)
  }),
)

const columnIterator = inputRange$.pipe(
  concatMap((range) => {
    const decodedRange = XLSX.utils.decode_range(range)
    return range$(decodedRange.s.c, decodedRange.e.c)
  }),
)

const reader$ = new Subject<string>()
const transformer$ = reader$.pipe(
  reduce((acc, curr, index) => {
    const bufferedData = Buffer.from(`${curr}\n`)
    const byteSize = bufferedData.length
    const last = acc[acc.length - 1]
    if (typeof last === 'undefined') {
      acc.push({
        buff: bufferedData,
        isHeader: index === 0 && _rangeIncludesHeader === true,
      })
    }
    else if ((byteSize + last.buff.length) >= maxSize || last.isHeader) {
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

const iterator$ = forkJoin<[XLSX.WorkBook, string, string]>([inputWb$, inputSheet$, inputRange$]).pipe(
  map(([inputWb, inputSheet, inputRange]) => {
    spinner.text = `Iterating through ${colors.cyan(`"${inputRange}"`)} in ${colors.cyan(`"${inputSheet}"`)}...`
    spinner.start()
    const rawSheet = inputWb.Sheets[inputSheet]['!data']
    if (typeof rawSheet === 'undefined') {
      ora().fail(colors.magenta(`Sheet ${colors.cyan(`"${inputSheet}"`)} not found in ${colors.cyan(`"${inputFile.base}"`)}\n`))
    }
    return {
      rawSheet,
      sheet: inputSheet,
    }
  }),
  withLatestFrom(rowIterator, columnIterator),
  map(([{ rawSheet, sheet }, rowIdx, colIdx]) => {
    spinner.text = `Iterating through rows in ${colors.cyan(`"${sheet}"`)}...`
    spinner.start()
    const unprocessedRowData: JsonPrimitive[] = []
    const isFirstRow = rowIdx === decodedRange.s.r
    const currentCell = rawSheet?.[rowIdx]?.[colIdx]
    unprocessedRowData.push((currentCell?.v ?? null) as string)
    if (isFirstRow && _rangeIncludesHeader === true) {
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
      unprocessedRowData.push(inputFile.base, sheet, _inputRange)
      return Papa.unparse([unprocessedRowData])
    }
  }),
)

function selectExcelFile(basePath: string): Promise<string> {
  return inquirerFileSelector({
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
  })
}

export function selectStartingFolder(): Promise<string> {
  const cloudFolders = fg.sync(['Library/CloudStorage/**'], {
    onlyDirectories: true,
    absolute: true,
    cwd: homedir(),
    deep: 1,
  }).map(folder => ({
    name: basename(folder).replace('OneDrive-SharedLibraries', 'SharePoint-'),
    value: folder,
  }))
  const homeFolders = fg.sync(['Desktop', 'Documents', 'Downloads'], {
    onlyDirectories: true,
    absolute: true,
    cwd: homedir(),
    deep: 1,
  }).map(folder => ({
    name: basename(folder),
    value: folder,
  }))
  return select({
    message: 'Where do you want to start looking for your Excel file?',
    pageSize: 20,
    choices: [new Separator('----HOME----'), ...homeFolders, new Separator('----ONEDRIVE----'), ...cloudFolders],
  })
}

function iterate(wb: XLSX.WorkBook, sheet: string, range: string): void {
  spinner.text = `Iterating through rows in ${colors.cyan(`"${sheet}"`)}...`
  spinner.start()
  let rawSheet = _Sheets![sheet]
  if (typeof rawSheet === 'undefined') {
    const { Sheets } = XLSX.readFile(_inputFilePath, {
      raw: true,
      cellDates: true,
      dense: true,
      sheet,
    })
    rawSheet = Sheets[sheet]
  }
  if (typeof rawSheet === 'undefined') {
    spinner.text = colors.magenta(`Sheet ${colors.cyan(`"${sheet}"`)} not found in ${colors.cyan(`"${inputFile.base}"`)}\n`)
    spinner.fail()
  }
  else {
    for (const rowIdx of rowInidices) {
      const unprocessedRowData: JsonPrimitive[] = []
      const isFirstRow = rowIdx === decodedRange.s.r
      for (const colIdx of columnIndices) {
        const currentCell = rawSheet['!data']?.[rowIdx]?.[colIdx]
        unprocessedRowData.push((currentCell?.v ?? null) as string)
      }
      if (isFirstRow && _rangeIncludesHeader === true) {
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
        reader$.next(Papa.unparse([unprocessedRowData]))
      }

      else {
        unprocessedRowData.push(inputFile.base, sheet, _inputRange)
        reader$.next(Papa.unparse([unprocessedRowData]))
      }
    }
    reader$.complete()
  }
}

async function setRange(wb: XLSX.WorkBook, sheetName: string): Promise<string> {
  const worksheetRange = wb.Sheets[sheetName]['!ref']!
  const parsedRange = XLSX.utils.decode_range(_inputRange ?? worksheetRange)
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
      default: _inputRange ?? worksheetRange,
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

async function setSheetName(wb: XLSX.WorkBook): Promise<string | undefined> {
  return select({
    message: 'Select the worksheet to parse',
    choices: wb.SheetNames.map((value, i) => ({
      name: `${i + 1}) ${value}`,
      value,
      short: value,
    })),
  })
}

async function setRangeIncludesHeader(): Promise<void> {
  _rangeIncludesHeader = await confirm({
    message: `Does range ${colors.cyan(`"${_inputRange}"`)} include the header row?`,
    default: true,
  })
}

async function setSplitWorksheet(): Promise<void> {
  _splitWorksheet = await confirm({
    message: `Would you like to split the output into multiple CSVs of a certain size?`,
    default: false,
  })
  if (_splitWorksheet) {
    maxSize = ((await number({
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

export async function parseArguments(inputArgs: Pick<Arguments<boolean>, 'filePath' | 'range' | 'sheetName'>): Promise<void> {
  if (isUndefined(inputArgs.filePath)) {
    const startingPath = await selectStartingFolder()
    inputArgs.filePath = await selectExcelFile(startingPath)
  }

  inputWb$.subscribe((wb) => {
    log(inspect(wb, {
      colors: true,
      depth: 1,
    }))
  })
  parsedInputPath$.next(parse(inputArgs.filePath))
}

function getFileSizeString(dataLength: number): string {
  return dataLength > (1024 * 1.1) ? dataLength > ((1024 ^ 2) * 1.1) ? `${round(dataLength / (1024 ^ 2), 2)} Mb` : `${round(dataLength / 1024, 2)} Kb` : `${dataLength} bytes`
}

function buildFilePath(dirName: string, text: string): string {
  return `./${basename(dirName)}/${relative(dirName, text)}`
}
