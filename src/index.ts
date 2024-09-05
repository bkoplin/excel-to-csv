import * as os from 'node:os'
import * as fs from 'node:fs'
import { basename, join, parse, relative, sep } from 'node:path'
import { PassThrough } from 'node:stream'
import inquirerFileSelector from 'inquirer-file-selector'
import * as XLSX from 'xlsx'
import Papa from 'papaparse'
import { ceil, curryRight, inRange, padStart, range } from 'lodash-es'
import type { JsonValue } from 'type-fest'
import dayjs from 'dayjs'
import colors from 'picocolors'
import { confirm, expand, input, number, select } from '@inquirer/prompts'
import { Separator } from '@inquirer/core'
import { emptyDirSync, ensureDirSync } from 'fs-extra'
import fg from 'fast-glob'
import { isUndefined } from '@antfu/utils'
import yoctoSpinner from 'yocto-spinner'

XLSX.set_fs(fs)

const spinner = yoctoSpinner({ text: 'Parsing…' })

export interface Arguments {
  filePath?: string
  sheetName?: string
  range?: string
}
export async function parseArguments(args: {
  filePath?: string | undefined
  sheetName?: string | undefined
  range?: string | undefined
}): Promise<void> {
  if (isUndefined(args.filePath)) {
    const { filePath, dirName } = await getExcelFilePath()
    args.filePath = filePath
    spinner.text = `Parsing ${colors.cyan(`"./${basename(dirName)}/${relative(dirName, filePath)}"`)}\n`
  }
  else {
    spinner.text = `Parsing ${colors.cyan(`"${args.filePath}"`)}...\n`
  }

  const { SheetNames, Sheets } = XLSX.readFile(args.filePath, {
    raw: true,
    cellDates: true,
    dense: true,
  })
  let csvSize = fs.statSync(args.filePath).size
  if (isUndefined(args.sheetName) || !SheetNames.includes(args.sheetName)) {
    const answer = await chooseSheetToParse(SheetNames)
    args.sheetName = answer
  }
  if (isUndefined(args.range)) {
    const answer = await getWorksheetRange(Sheets, args)
    args.range = answer
  }
  const parsedFile = parse(args.filePath)
  const rawSheet = Sheets[args.sheetName]
  const decodedRange = XLSX.utils.decode_range(args.range)

  const columnIndices = range(decodedRange.s.c, decodedRange.e.c + 1)
  const rowIndices = range(decodedRange.s.r, decodedRange.e.r + 1)
  let splitWorksheet = false
  let outputFilePath = `${parsedFile.dir}/${parsedFile.name}_${args.sheetName}_${dayjs().format('YYYY.MM.DD HH.mm.ss')}`
  const csvSizeInMegabytes = ceil(csvSize / 1000000, 2)
  if (csvSizeInMegabytes > 5) {
    splitWorksheet = await confirm({
      message: `The size of the resulting CSV file could exceed ${colors.yellow(`${csvSizeInMegabytes * 4}Mb`)}. Would you like to split the output into multiple CSVs?`,
      default: false,
    })
    if (splitWorksheet) {
      ensureDirSync(join(parsedFile.dir, dayjs().format('YYYY.MM.DD HH.mm.ss')))
      emptyDirSync(join(parsedFile.dir, dayjs().format('YYYY.MM.DD HH.mm.ss')))
      outputFilePath = join(parsedFile.dir, dayjs().format('YYYY.MM.DD HH.mm.ss'), `${parsedFile.name} ${args.sheetName}`)
      const tempCSVSize = await number({
        message: 'Size of output CSV files (in Mb):',
        default: csvSizeInMegabytes,
        min: 1,
        max: csvSizeInMegabytes * 4,
      })
      csvSize = tempCSVSize * 1000000
    }
  }
  let fileNum = 1
  let writeStream = splitWorksheet ? fs.createWriteStream(`${outputFilePath}_${padStart(`${fileNum}`, 3, '0')}.csv`, 'utf-8') : fs.createWriteStream(`${outputFilePath}.csv`, 'utf-8')
  const pass = new PassThrough()
  pass.on('end', () => {
    const outputFiles = fg.sync(`${outputFilePath}*.csv`, {
      cwd: parsedFile.dir,
      onlyFiles: true,
    }).map(file => `  ${colors.cyan(relative(parsedFile.dir, file))}`)
    spinner.success(
      `${colors.green('SUCCESS!')} The output file(s) have been saved to the following location(s):\n${outputFiles.join('\n')}`,
    )
  })
  pass.on('data', (chunk: Blob) => {
    pass.pause()
    const { text, isLastRow } = JSON.parse(chunk.toString()) as {
      text: string
      isLastRow: boolean
    }
    const streamWriteResult = writeStream.write(text)
    if (splitWorksheet === false) {
      if (streamWriteResult === false) {
        writeStream.once('drain', () => {
          if (isLastRow)
            pass.end()
          else pass.resume()
        })
      }
      else {
        if (isLastRow)
          pass.end()
        else pass.resume()
      }
    }
    else if (streamWriteResult === false) {
      writeStream.once('drain', () => {
        ({ writeStream, fileNum } = updateWriteStream(isLastRow, pass, writeStream, csvSize, fileNum, outputFilePath))
      })
    }
    else {
      ({ writeStream, fileNum } = updateWriteStream(isLastRow, pass, writeStream, csvSize, fileNum, outputFilePath))
    }
  })

  rowIndices.forEach((rowIdx) => {
    const rowdata: JsonValue[] = []
    columnIndices.forEach((colIdx) => {
      const currentCell = rawSheet['!data']?.[rowIdx]?.[colIdx]
      rowdata.push((currentCell?.v ?? null) as string)
    })
    const isLastRow = rowIdx === decodedRange.e.r
    if (rowIdx === decodedRange.s.r) {
      rowdata.push('source_file', 'source_sheet', 'source_range')
      const csv = Papa.unparse([rowdata])
      if (splitWorksheet) {
        fs.writeFileSync(`${outputFilePath}_HEADER.csv`, csv, 'utf-8')
      }
      else {
        pass.write(JSON.stringify({
          text: `${csv}\n`,
          isLastRow,
        }))
      }
    }
    else {
      rowdata.push(parsedFile.base, args.sheetName!, args.range!)
      const csv = Papa.unparse([rowdata])
      pass.write(JSON.stringify({
        text: `${csv}\n`,
        isLastRow,
      }))
    }
  })
  rowIndices.forEach((rowIdx) => {
    const rowdata: JsonValue[] = []
    columnIndices.forEach((colIdx) => {
      const currentCell = rawSheet['!data']?.[rowIdx]?.[colIdx]
      rowdata.push((currentCell?.v ?? null) as string)
    })
    if (rowIdx === decodedRange.s.r) {
      rowdata.push('source_file', 'source_sheet', 'source_range')
      const csv = Papa.unparse([rowdata])
      if (splitWorksheet)
        fs.writeFileSync(`${outputFilePath}_HEADER.csv`, csv, 'utf-8')
      else pass.write(`${csv}\n`)
    }
    else {
      rowdata.push(parsedFile.base, args.sheetName!, args.range!)
      const csv = Papa.unparse([rowdata])
      pass.write(`${csv}\n`)
    }
    if (rowIdx === decodedRange.e.r) {
      pass.end()
    }
    // dataForCSV.push(rowdata)
  })
  /* fs.writeFile(`${parsedFile.dir}/${parsedFile.name}_${sheetName}_${dayjs().format('YYYY.MM.DD HH.mm.ss')}.csv`, csv, {
       encoding: 'utf-8',
     }, (err) => {
       if (err)
         throw new Commander.InvalidArgumentError(`There was an error parsing the worksheet ${colors.bold(colors.cyan(`"${sheetName}"`))} from the Excel at ${colors.yellow(`"${filePath}"`)}`)
       spinner.success(colors.green('Parsed successfully'))
     }) */
}
function updateWriteStream(isLastRow: boolean, pass: PassThrough, writeStream: fs.WriteStream, csvSize: number, fileNum: number, outputFilePath: string): {
  writeStream: fs.WriteStream
  fileNum: number
} {
  if (isLastRow) {
    pass.end()
  }
  else if (writeStream.bytesWritten < csvSize) {
    pass.resume()
  }
  else {
    writeStream.destroy()
    fileNum += 1
    writeStream = fs.createWriteStream(`${outputFilePath}_${padStart(`${fileNum}`, 3, '0')}.csv`, 'utf-8')
    writeStream.on('error', (err) => {
      spinner.error(`There was an error writing the CSV file: ${colors.red(err.message)}`)
      pass.destroy()
    })
    pass.resume()
  }
  return {
    writeStream,
    fileNum,
  }
}

async function getWorksheetRange(Sheets: { [sheet: string]: XLSX.WorkSheet }, args: {
  filePath?: string | undefined
  sheetName?: string | undefined
  range?: string | undefined
}): Promise<string> {
  const worksheetRange = Sheets[args.sheetName!]['!ref']
  const parsedRange = XLSX.utils.decode_range(worksheetRange)
  const isRowInRange = curryRight(inRange, 3)(parsedRange.e.r + 1)(parsedRange.s.r)
  const isColumnInRange = curryRight(inRange, 3)(parsedRange.e.c + 1)(parsedRange.s.c)
  const isRangeInDefaultRange = (r: XLSX.Range): boolean => isRowInRange(r.s.r) === true && isColumnInRange(r.s.c) === true && isRowInRange(r.e.r) === true && isColumnInRange(r.e.c) === true
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
    const userRangeInput = input({
      name: 'range',
      message: 'Enter the range of the worksheet to parse',
      default: worksheetRange,
      validate: (value: string) => {
        const isValidInput = isRangeInDefaultRange(XLSX.utils.decode_range(value))
        if (!isValidInput)
          return `The range must be within the worksheet's default range (${XLSX.utils.encode_range(parsedRange)})`
        return true
      },
    }, {
      clearPromptOnDone: false,
      signal: AbortSignal.timeout(5000),
    }).catch((error: string | { name: string }) => {
      if (error.name === 'AbortPromptError') {
        return worksheetRange
      }

      throw error
    })
    return userRangeInput
  }
  else {
    const startRow = await number({
      name: 'startRow',
      message: 'Enter the starting row number',
      default: parsedRange.s.r + 1,
      min: parsedRange.s.r + 1,
      max: parsedRange.e.r + 1,
      step: 1,
      /* theme: {
           style: {
             answer: (text: string) => colors.cyan(`Row "${text}"`),
             defaultAnswer: (text: string) => colors.cyan(`Row "${text}"`),
           },
         }, */
    })
    const endRow = await number({
      name: 'endRow',
      message: 'Enter the ending row number',
      default: parsedRange.e.r + 1,
      min: startRow,
      max: parsedRange.e.r + 1,
      step: 1,
      /* theme: {
           style: {
             answer: (text: string) => colors.cyan(`Row "${text}"`),
             defaultAnswer: (text: string) => colors.cyan(`Row "${text}"`),
           },
         }, */
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
    return `${startCol}${startRow}:${endCol}${endRow}`
  }
}

async function chooseSheetToParse(SheetNames: string[]): Promise<string> {
  return select({
    name: 'sheetName',
    message: 'Select the worksheet to parse',
    choices: SheetNames.map(value => ({
      name: `1) ${value}`,
      value,
      short: value,
    })),
  }, {
    clearPromptOnDone: false,
  })
}

async function getExcelFilePath(): Promise<{
  dirName: string
  filePath: string
}> {
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
    name: 'dirName',
    message: 'Where do you want to start looking for your Excel file?',
    pageSize: 20,
    choices: [new Separator('----HOME----'), ...homeFolders, new Separator('----ONEDRIVE----'), ...cloudFolders],
  }, {
    clearPromptOnDone: false,
  })
  const filePath = await inquirerFileSelector({
    message: 'Navigate to the Excel file you want to parse (only files with the .xls or .xlsx extension will be shown, and the file names must start with an alphanumeric character)',
    basePath: dirName,
    hideNonMatch: true,
    allowCancel: true,
    pageSize: 20,
    theme: {
      style: {
        answer: (text: string) => colors.cyan(`./${basename(dirName)}/${relative(dirName, text)}`),
        currentDir: (text: string) => colors.magenta(`./${basename(dirName)}/${relative(dirName, text)}`),
      },
    },
    match(filePath) {
      if (filePath.isDir) {
        return !filePath.path.split(sep).some(v => /^[^A-Z0-9]/i.test(v))
      }

      return !/^[^A-Z0-9]/i.test(filePath.name) && /\.xlsx?$/.test(filePath.name)
    },
  }).catch((error: string | { name: string }) => {
    if (error.name === 'AbortPromptError') {
      return 'canceled'
    }
  })
  if (filePath === 'canceled') {
    spinner.error(`Cancelled selection`)
    process.exit(1)
  }
  return {
    filePath,
    dirName,
  }
}
