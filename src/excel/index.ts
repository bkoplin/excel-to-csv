import { readFile } from 'node:fs/promises'
import chalk from 'chalk'
import ora from 'ora'
import { inRange } from 'radash'
import XLSX from 'xlsx'
import {
  confirm,
  expand,
  input,
  number,
  select,
} from '@inquirer/prompts'

export async function getWorkbook(inputPath: string): Promise<XLSX.WorkBook> {
  const buffer = await readFile(inputPath)
  return XLSX.read(buffer, {
    type: 'buffer',
    cellDates: true,
    raw: true,
    dense: true,
  })
}

export function isOverlappingRange(ws: XLSX.WorkSheet, range: string | undefined): range is string {
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
    const {
      isColumnInRange,
      isRangeInDefaultRange,
      isRowInRange,
      parsedRange: decodedSheetRange,
    } = extractRangeInfo(ws, sheetRange)
    const { parsedRange: decodedInputRange } = extractRangeInfo(ws, range)
    const rowsStartInSheetRange = isRowInRange(decodedInputRange.s.r)
    const rowsEndInSheetRange = isRowInRange(decodedInputRange.e.r)
    const colsStartInSheetRange = isColumnInRange(decodedInputRange.s.c)
    const colsEndInSheetRange = isColumnInRange(decodedInputRange.e.c)
    // if (rowsStartInSheetRange && rowsEndInSheetRange && colsStartInSheetRange && colsEndInSheetRange) {
    //   return true
    // }
    // else
    if (!isRangeInDefaultRange(decodedInputRange)) {
      ora(`You have selected a range (${chalk.yellowBright(`${range}`)}) that is completely outside the worksheet data range (${chalk.yellowBright(`${sheetRange}`)})`).fail()
      return false
    }
    else {
      const warningStrings = []
      for (const termType of [['s', 'starts'], ['e', 'ends']] as const) {
        for (const objType of [['r', 'row', 'encode_row'], ['c', 'column', 'encode_col']] as const) {
          const inputVal = decodedInputRange[termType[0]][objType[0]]
          const sheetVal = decodedSheetRange[termType[0]][objType[0]]
          const encodedInputVal = XLSX.utils[objType[2]](inputVal)
          const diffType = inputVal < sheetVal ? 'before' : 'after'
          if (inputVal !== sheetVal) {
            warningStrings.push(`\n  ${termType[1]} at ${objType[1]} ${chalk.yellowBright(encodedInputVal)}, which is ${chalk.yellowBright(Math.abs(inputVal - sheetVal))} ${objType[1]}(s) ${diffType} the worksheet data range ${termType[1]}`)
          }
        }
      }
      if (warningStrings.length)
        ora(`You have input a range (${chalk.yellowBright(`${range}`)}) that includes less data than the worksheet data range (${`${chalk.yellowBright(sheetRange)}`}).\n\nYour input range:${warningStrings.join('')}\n\n`).warn()
      //   if (!colsStartInSheetRange) {
      //   }
      return true
    }
  }
}

export async function setRange(wb: XLSX.WorkBook, sheetName: string, _inputRange?: string): Promise<string> {
  const {
    worksheetRange,
    isRangeInDefaultRange,
    parsedRange,
  } = extractRangeInfo(wb.Sheets[sheetName], _inputRange)
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

export function extractRangeInfo(ws: XLSX.WorkSheet, _inputRange: string | undefined): {
  worksheetRange: string
  isRangeInDefaultRange: (r: XLSX.Range) => boolean
  parsedRange: XLSX.Range
  isRowInRange: (input: number) => boolean
  isColumnInRange: (input: number) => boolean
} {
  const worksheetRange = ws['!ref']!
  const parsedRange = XLSX.utils.decode_range(_inputRange ?? worksheetRange)
  const isRowInRange = (input: number): boolean => inRange(input, parsedRange.s.r, parsedRange.e.r + 1)
  const isColumnInRange = (input: number): boolean => inRange(input, parsedRange.s.c, parsedRange.e.c + 1)
  const isRangeInDefaultRange = (r: XLSX.Range): boolean => isRowInRange(r.s.r) && isColumnInRange(r.s.c) && isRowInRange(r.e.r) && isColumnInRange(r.e.c)
  return {
    worksheetRange,
    isRangeInDefaultRange,
    parsedRange,
    isRowInRange,
    isColumnInRange,
  }
}

export async function setSheetName(wb: XLSX.WorkBook, sheetName?: string): Promise<string> {
  return select({
    message: 'Select the worksheet to parse',
    default: sheetName ?? wb.SheetNames[1],
    choices: wb.SheetNames.map((value, i) => ({
      name: `${i + 1}) ${value}`,
      value,
      short: value,
    })),
  })
}

export async function setRangeIncludesHeader(_inputRange: string): Promise<boolean> {
  return await confirm({
    message: `Does range ${chalk.cyanBright(`"${_inputRange}"`)} include the header row?`,
    default: true,
  })
}
