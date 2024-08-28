import * as fs from 'node:fs'
import { dirname, parse } from 'node:path'
import type { ParsedPath } from 'node:path/posix'
import * as Commander from '@commander-js/extra-typings'
import colors from 'picocolors'
import { stringify } from 'yaml'
import * as XLSX from 'xlsx'
import yoctoSpinner from 'yocto-spinner'
import Papa from 'papaparse'

const spinner = yoctoSpinner({ text: 'Parsing…' })
XLSX.set_fs(fs)
export interface Arguments {
  filePath: string
  sheetName?: string
  range?: string
}

export async function parseWorksheet({ filePath, sheetName, range }: Arguments): void {
  spinner.start()
  const parsedFile = parse(filePath)
  const workbook = XLSX.readFile(filePath, { raw: true, cellDates: true })
  const worksheets = workbook.SheetNames
  if (typeof sheetName === 'string') {
    if (worksheets.includes(sheetName)) {
      processWorksheet(workbook, sheetName, range, parsedFile, filePath)
    }
    else {
      throw new Commander.InvalidArgumentError(`The worksheet ${colors.bold(colors.cyan(`"${sheetName}"`))} does not exist in the Excel at ${colors.yellow(`"${filePath}"`)}`)
    }
  }
  else {
    processWorksheet(workbook, worksheets[0], range, parsedFile, filePath)
  }
}
function processWorksheet(workbook: XLSX.WorkBook, sheetName: string, range: string | undefined, parsedFile: ParsedPath, filePath: string): void {
  const rawSheet = workbook.Sheets[sheetName]
  const worksheet = XLSX.utils.sheet_to_json(rawSheet, { range, raw: true, UTC: true })
  const data = worksheet.map(row => ({ ...row, source: parsedFile.base, range: range || workbook.Sheets[sheetName]['!ref'] }))
  const csv = Papa.unparse(data)
  fs.writeFile(`${parsedFile.dir}/${parsedFile.name}_${sheetName}.csv`, csv, (err) => {
    if (err)
      throw new Commander.InvalidArgumentError(`There was an error parsing the worksheet ${colors.bold(colors.cyan(`"${sheetName}"`))} from the Excel at ${colors.yellow(`"${filePath}"`)}`)
    spinner.success(colors.green('Parsed successfully'))
  })
}
