import { readFile } from 'node:fs/promises'
import chalk from 'chalk'
import ora from 'ora'
import { inRange } from 'radash'
import XLSX from 'xlsx'

export async function getWorkbook(inputPath: string): Promise<XLSX.WorkBook> {
  const buffer = await readFile(inputPath)
  return XLSX.read(buffer, {
    type: 'buffer',
    cellDates: true,
  })
}

export function isOverlappingRange(ws: XLSX.WorkSheet, opts: { range?: string | true }): opts is { range: string } {
  const range = opts.range
  const sheetRange = ws?.['!ref']
  if (typeof range === 'undefined' || typeof sheetRange === 'boolean') {
    ora(`Your input range is not a valid range; you will need to select a range`).warn()
    return false
  }
  else if (typeof sheetRange === 'undefined') {
    ora(`The worksheet does not exist in the Excel file or does not have a range`).fail()
    process.exit(1)
    return false
  }
  else {
    const decodedSheetRange = XLSX.utils.decode_range(sheetRange)
    const decodedInputRange = XLSX.utils.decode_range(range)
    const rowsStartInSheetRange = inRange(decodedInputRange.s.r, decodedSheetRange.s.r, decodedSheetRange.e.r + 1)
    const rowsEndInSheetRange = inRange(decodedInputRange.e.r, decodedSheetRange.s.r, decodedSheetRange.e.r + 1)
    const colsStartInSheetRange = inRange(decodedInputRange.s.c, decodedSheetRange.s.c, decodedSheetRange.e.c + 1)
    const colsEndInSheetRange = inRange(decodedInputRange.e.c, decodedSheetRange.s.c, decodedSheetRange.e.c + 1)
    // if (rowsStartInSheetRange && rowsEndInSheetRange && colsStartInSheetRange && colsEndInSheetRange) {
    //   return true
    // }
    // else
    if (!rowsEndInSheetRange && !rowsStartInSheetRange && !colsEndInSheetRange && !colsStartInSheetRange) {
      ora(`You have selected a range (${chalk.cyanBright(`${range}`)}) that is completely outside the worksheet data range (${chalk.cyanBright(`${sheetRange}`)})`).fail()
      return false
    }
    else {
      const warningStrings = []
      //   if (!rowsStartInSheetRange) {
      if (decodedInputRange.s.c < decodedSheetRange.s.c)
        warningStrings.push(`\n\tstarts at column ${chalk.redBright(XLSX.utils.encode_col(decodedInputRange.s.c))}, which is ${chalk.redBright(decodedSheetRange.s.c - decodedInputRange.s.c)} columns before the worksheet data range starts`)
      else if (decodedInputRange.s.c > decodedSheetRange.s.c)
        warningStrings.push(`\n\tstarts at column ${chalk.redBright(XLSX.utils.encode_col(decodedInputRange.s.c))}, which is ${chalk.redBright(decodedInputRange.s.c - decodedSheetRange.s.c)} columns after the worksheet data range starts`)
        //   }
        //   if (!colsEndInSheetRange) {
      if (decodedInputRange.e.c < decodedSheetRange.s.c)
        warningStrings.push(`\n\tends at column ${chalk.redBright(XLSX.utils.encode_col(decodedInputRange.e.c))}, which is ${chalk.redBright(decodedSheetRange.s.c - decodedInputRange.e.c)} columns before the worksheet data range ends`)
      else if (decodedInputRange.e.c > decodedSheetRange.s.c)
        warningStrings.push(`\n\tends at column ${chalk.redBright(XLSX.utils.encode_col(decodedInputRange.e.c))}, which is ${chalk.redBright(decodedInputRange.e.c - decodedSheetRange.s.c)} columns after the worksheet data range ends`)
      if (decodedInputRange.s.r < decodedSheetRange.s.r)
        warningStrings.push(`\n\tstarts at row ${chalk.redBright(decodedInputRange.s.r)}, which is ${chalk.redBright(decodedSheetRange.s.r - decodedInputRange.s.r)} rows before the worksheet data range starts`)
      else if (decodedInputRange.s.r > decodedSheetRange.s.r)
        warningStrings.push(`\n\tstarts at row ${chalk.redBright(decodedInputRange.s.r)}, which is ${chalk.redBright(decodedInputRange.s.r - decodedSheetRange.s.r)} rows after the worksheet data range starts`)
      //   }
      //   if (!rowsEndInSheetRange) {
      if (decodedInputRange.e.r < decodedSheetRange.s.r)
        warningStrings.push(`\n\tends at row ${chalk.redBright(decodedInputRange.e.r)}, which is ${chalk.redBright(decodedSheetRange.s.r - decodedInputRange.e.r)} rows before the worksheet data range ends`)
      else if (decodedInputRange.e.r > decodedSheetRange.s.r)
        warningStrings.push(`\n\tends at row ${chalk.redBright(decodedInputRange.e.r)}, which is ${chalk.redBright(decodedInputRange.e.r - decodedSheetRange.s.r)} rows after the worksheet data range ends`)
      //   }
      //   if (!colsStartInSheetRange) {
      //   }
      ora(`You have input a range (${chalk.redBright(`${range}`)}) that includes less data than the worksheet data range (${`${chalk.redBright(sheetRange)}`}).\n\nYour input range:${warningStrings.join('')}`).warn()
      return true
    }
  }
}
