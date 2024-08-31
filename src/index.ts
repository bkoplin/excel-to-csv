import * as fs from 'node:fs'
import { parse } from 'node:path'
import type { ParsedPath } from 'node:path/posix'
import * as Commander from '@commander-js/extra-typings'
import colors from 'picocolors'
import * as XLSX from 'xlsx'
import Papa from 'papaparse'
import { range } from 'lodash-es'
import type { JsonValue } from 'type-fest'
import type { Spinner } from 'yocto-spinner'
import dayjs from 'dayjs'
import { isUndefined } from '@antfu/utils'

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
  spinner.start()
  const parsedFile = parse(filePath)
  const workbook = XLSX.readFile(filePath, { raw: true, cellDates: true, dense: true, sheet: sheetName })
  const parsingConfig: FileParserOptions = { workbook, sheetName, inputRange, parsedFile, filePath, spinner }
  processWorksheet(parsingConfig)
}
interface FileParserOptions {
  workbook?: XLSX.WorkBook
  sheetName: string
  inputRange: string
  parsedFile: ParsedPath
  filePath: string
  spinner: Spinner
}

function processWorksheet({ workbook, sheetName, inputRange, parsedFile, filePath, spinner }: FileParserOptions): void {
  const rawSheet = workbook!.Sheets[sheetName]
  const decodedRange = XLSX.utils.decode_range(inputRange)
  const dataForCSV: Papa.UnparseObject<JsonValue> = { fields: [], data: [] }
  /* let fields: string[] = []
     const data: JsonValue[] = [] */
  const columnIndices = range(decodedRange.s.c, decodedRange.e.c + 1)
  const rowIndices = range(decodedRange.s.r, decodedRange.e.r + 1)
  rowIndices.forEach((rowIdx) => {
    if (rowIdx === decodedRange.s.r) {
      columnIndices.forEach((colIdx) => {
        const currentCell = rawSheet['!data'][rowIdx][colIdx]
        if (isUndefined(currentCell)) {
          dataForCSV.fields.push(null)
        }
        else {
          dataForCSV.fields.push(currentCell.v as string)
        }
      })
      dataForCSV.fields.push('source_file', 'source_sheet', 'source_range')
    }
    else {
      const rowdata: JsonValue[] = []
      columnIndices.forEach((colIdx) => {
        const currentCell = rawSheet['!data'][rowIdx][colIdx]
        if (isUndefined(currentCell)) {
          rowdata.push(null)
        }
        else {
          rowdata.push(currentCell.v as string)
        }
      })
      rowdata.push(parsedFile.base, sheetName, inputRange)
      dataForCSV.data.push(rowdata)
    }
  })
  const csv = Papa.unparse(dataForCSV)
  fs.writeFile(`${parsedFile.dir}/${parsedFile.name}_${sheetName}_${dayjs().format('YYYY.MM.DD HH.mm.ss')}.csv`, csv, {
    encoding: 'utf-8',
  }, (err) => {
    if (err)
      throw new Commander.InvalidArgumentError(`There was an error parsing the worksheet ${colors.bold(colors.cyan(`"${sheetName}"`))} from the Excel at ${colors.yellow(`"${filePath}"`)}`)
    spinner.success(colors.green('Parsed successfully'))
  })
}
