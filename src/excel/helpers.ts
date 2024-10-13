import { homedir } from 'node:os'
import fs from 'fs-extra'
import {
  basename,
  join,
  relative,
  sep,
} from 'pathe'
import fg from 'fast-glob'
import inquirerFileSelector from 'inquirer-file-selector'
import * as XLSX from 'xlsx'
import { inRange } from 'radash'
import colors from 'chalk'
import {
  confirm,
  expand,
  input,
  number,
  select,
} from '@inquirer/prompts'
import { Separator } from '@inquirer/core'
import ora from 'ora'
import {
  createRegExp,
  exactly,
  maybe,
} from 'magic-regexp'

/* async_RS reads a stream and returns a Promise resolving to a workbook */

export async function checkAndResolveFilePath(fileType: 'Excel' | 'CSV', argFilePath: string | undefined): Promise<string> {
  if (typeof argFilePath === 'undefined' || !fs.existsSync(argFilePath)) {
    ora().warn(colors.yellowBright(`No ${colors.yellowBright(fileType)} exists at path ${colors.cyanBright(`"${argFilePath}"`)}!`))
    const startingFolder = await selectStartingFolder(fileType)
    const selectedFile = await selectFile(fileType, startingFolder)
    return selectedFile
  }
  return Promise.resolve(argFilePath)
}

export function selectFile(fileType: 'Excel' | 'CSV', basePath: string): Promise<string> {
  const fileExtString = fileType === 'Excel' ? `${colors.cyanBright('.xls')} or ${colors.cyanBright('.xlsx')}` : colors.cyanBright('csv')
  const pathRegexp = fileType === 'Excel' ? createRegExp(exactly('.'), exactly('xlsx').before(maybe('x')).at.lineEnd(), ['i']) : createRegExp(exactly('.'), exactly('csv').at.lineEnd(), ['i'])
  return inquirerFileSelector({
    message: `Navigate to the ${colors.yellowBright(fileType)} file you want to parse (only files with an ${fileExtString} extension will be shown, and the file names must start with an alphanumeric character)`,
    basePath,
    hideNonMatch: true,
    allowCancel: true,
    pageSize: 20,
    theme: { style: { currentDir: (text: string) => colors.magentaBright(join(`.`, basename(basePath), relative(basePath, text))) } },
    match(filePath) {
      if (filePath.isDir) {
        return !filePath.path.split(sep).some(v => /^[^A-Z0-9]/i.test(v))
      }

      return !/^[^A-Z0-9]/i.test(filePath.name) && pathRegexp.test(filePath.name)
    },
  })
}

export function selectStartingFolder(fileType: 'Excel' | 'CSV'): Promise<string> {
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
    message: `Where do you want to start looking for your ${colors.yellowBright(fileType)} file?`,
    pageSize: 20,
    choices: [new Separator('----HOME----'), ...homeFolders, new Separator('----ONEDRIVE----'), ...cloudFolders],
  })
}

export async function setRange(wb: XLSX.WorkBook, sheetName: string, _inputRange?: string): Promise<string> {
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

export async function setSheetName(wb: XLSX.WorkBook): Promise<string> {
  return select({
    message: 'Select the worksheet to parse',
    choices: wb.SheetNames.map((value, i) => ({
      name: `${i + 1}) ${value}`,
      value,
      short: value,
    })),
  })
}

export async function setRangeIncludesHeader(_inputRange: string): Promise<boolean> {
  return await confirm({
    message: `Does range ${colors.cyanBright(`"${_inputRange}"`)} include the header row?`,
    default: true,
  })
}
