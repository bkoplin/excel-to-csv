import * as fs from 'node:fs'
import { parse } from 'node:path'
import type { ParsedPath } from 'node:path/posix'
import * as Commander from '@commander-js/extra-typings'
import colors from 'picocolors'
import * as XLSX from 'xlsx'
import Papa from 'papaparse'
import { times } from 'lodash-es'
import type { JsonValue } from 'type-fest'
import type { Spinner } from 'yocto-spinner'

XLSX.set_fs(fs)
export interface Arguments {
  filePath?: string
  sheetName?: string
  range?: string
}

export async function parseWorksheet(args: {
  filePath?: string
  sheetName?: string
  range?: string
}, spinner: Spinner): Promise<void> {
  const { filePath, sheetName, range } = args
  spinner.start()
  if (typeof filePath !== 'string')
    return
  const parsedFile = parse(filePath)
  if (typeof sheetName === 'string') {
    const workbook = XLSX.readFile(filePath, { raw: false, cellDates: true, dense: true, sheet: sheetName })
    const worksheets = workbook.SheetNames
    if (worksheets.includes(sheetName)) {
      processWorksheet(workbook, sheetName, range, parsedFile, filePath, spinner)
    }
    else {
      throw new Commander.InvalidArgumentError(`The worksheet ${colors.bold(colors.cyan(`"${sheetName}"`))} does not exist in the Excel at ${colors.yellow(`"${filePath}"`)}`)
    }
  }
  else {
    const workbook = XLSX.readFile(filePath, { raw: false, cellDates: true, dense: true, sheet: sheetName })
    const worksheets = workbook.SheetNames
    processWorksheet(workbook, worksheets[0], range, parsedFile, filePath, spinner)
  }
}
function processWorksheet(workbook: XLSX.WorkBook, sheetName: string, inputRange: string | undefined, parsedFile: ParsedPath, filePath: string, spinner: Spinner): void {
  const rawSheet = workbook.Sheets[sheetName]
  const range = (inputRange || rawSheet['!ref']) as string
  const decodedRange = XLSX.utils.decode_range(range)
  let fields: string[] = []
  const data: JsonValue[] = []
  times(decodedRange.e.r - decodedRange.s.r, (i) => {
    const rowIdx = i + decodedRange.s.r
    const rowdata = rawSheet['!data']?.[rowIdx].slice(decodedRange.s.c, decodedRange.e.c + 1).map(cell => cell.v) as JsonValue[]
    if (i === 0) {
      fields = rowdata as string[]
      fields[decodedRange.e.c + 1] = 'source_file'
      fields[decodedRange.e.c + 2] = 'source_sheet'
      fields[decodedRange.e.c + 3] = 'source_range'
    }
    else {
      rowdata[decodedRange.e.c + 1] = parsedFile.base
      rowdata[decodedRange.e.c + 2] = sheetName
      rowdata[decodedRange.e.c + 3] = range
      data.push(rowdata)
    }
  })
  // const worksheet = XLSX.utils.sheet_to_json(rawSheet, { range, raw: true, UTC: true, header: 1 }) as Array<JsonPrimitive>[]
  // const [fields, ...data] = map(worksheet, (row, i) => {
  //   if (i === 0)
  //     return [...row, 'source_file', 'source_range']
  //   else return [...row, parsedFile.base, range || workbook.Sheets[sheetName]['!ref']]
  // })
  const csv = Papa.unparse({ fields, data })
  fs.writeFile(`${parsedFile.dir}/${parsedFile.name}_${sheetName}.csv`, csv, {
    encoding: 'utf-8',
  }, (err) => {
    if (err)
      throw new Commander.InvalidArgumentError(`There was an error parsing the worksheet ${colors.bold(colors.cyan(`"${sheetName}"`))} from the Excel at ${colors.yellow(`"${filePath}"`)}`)
    spinner.success(colors.green('Parsed successfully'))
  })
}
