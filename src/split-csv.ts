
import { homedir } from 'node:os'
import { readFile } from 'node:fs/promises'

import { Readable } from 'node:stream'
import fg from 'fast-glob'
import * as Commander from '@commander-js/extra-typings'
import chalk from 'chalk'
import {
  join,
  relative,
  resolve,
} from 'pathe'
import fs from 'fs-extra'
import type { Merge } from 'type-fest'
import {
  delay,
  isNaN,
  isNil,
  noop,
  toNumber,
} from 'lodash-es'
import ora from 'ora'
import {
  expand,
  input,
  number,
  select,
} from '@inquirer/prompts'
import XLSX from 'xlsx'
import { inRange } from 'radash'
import Papa from 'papaparse'
import pkg from '../package.json'
import { splitCSV } from './from-csv'

const filePath = new Commander.Argument('[path]', 'the full path to the CSV file')
const filterValuesOption = new Commander.Option(
  '-f, --filters [operators...]',
  `one or more pairs of colum headers and values to apply as a filter to each row as ${chalk.bgGrey(`${chalk.cyan('ColumnName')}${chalk.whiteBright(':')}${chalk.yellow('FilterValue')}`)}`,
).preset([] as string[][])
  .implies({ matchType: `all` })
const categoryOption = new Commander.Option(
  '-c, --category-field [column title]',
  'the name of a column whose value will be used to create each separate file',
)
  .preset('')
  .default(undefined)
  .default(true)
const maxFileSizeOption = new Commander.Option(
  '-s, --max-file-size-in-mb [number]',
  'the maximum size of each file in MB',
)
  .preset('')
  .default(undefined)
  .argParser((val): number | undefined => typeof val === 'undefined' || val === null || isNaN(toNumber(val)) ? undefined : toNumber(val))
const filterCondition = new Commander.Option('-m, --match-type', 'the type of match to use when filtering rows').choices([`all`, `any`, `none`])
  .preset(`all`)
const excelCommand = new Commander.Command('excel').addArgument(filePath)
  .option('--sheet [sheet name]', 'the sheet containing the data to parse to CSV')
  .option('--range [range]', 'the range of cells to parse in the Excel file')
  .addOption(filterValuesOption)
  .addOption(categoryOption)
  .addOption(filterCondition)
  .addOption(maxFileSizeOption)
  .option('-h, --header [write header]', 'enable writing the CSV header to each file (if you do not include this flag, the header will be written separately, even if there is only one file)')
  .action(async (argFilePath, options) => {
    if (isNil(argFilePath)) {
      ora().fail(chalk.redBright(`No argument provided; you must provide a file path as the command argument`))
      process.exit()
    }
    let inputFilePath = resolve(argFilePath)
    if (!argFilePath.toLowerCase().endsWith('.xlsx')) {
      ora().fail(`The file path provided (${chalk.cyanBright(`"${argFilePath}"`)}) does not end with ".xlsx." This program can only parse .xlsx files. ${chalk.yellowBright('You might need to quote the path if it contains spaces')}.`)
      process.exit()
    }
    inputFilePath = await checkAndResolveFilePath(inputFilePath)
    // let sheet: XLSX.WorkSheet
    const file = await readFile(inputFilePath)
    const wb = XLSX.read(file, {
      type: 'buffer',
      cellDates: true,
    })
    if (typeof options.sheet === 'undefined' || typeof options.sheet === 'boolean' || !wb.SheetNames.includes(options.sheet)) {
      options.sheet = await setSheetName(wb)
    }
    if (typeof options.range === 'undefined' || typeof options.range === 'boolean') {
      options.range = await setRange(wb, options.sheet!)
    }
    if (options.filters) {
      const validFilters: string[][] = []
      for (const filter of options.filters) {
        const splitFilter = (filter as string).split(':').map(v => v.trim())
        if (splitFilter.length !== 2) {
          ora().info(`Ignoring filter: ${chalk.cyanBright(`"${filter}"`)}. Fields/values must be separated with a colon.`)
          await delay(noop, 1000)
          break
        }
        validFilters.push(splitFilter)
      }
      options.filters = validFilters
    }
    const rows = XLSX.utils.sheet_to_json(wb.Sheets[options.sheet!], {
      range: options.range,
      header: 1,
    })
    const csv = Papa.unparse(rows)
    const reader = Readable.from(csv)
    splitCSV(reader, {
      ...options,
      inputFilePath,
    })
  })
  // .action(async (argFilePath, options) => {})
const program = new Commander.Command()
  .version(pkg.version)
  .name('csv-xlsx')
  .description('A CLI tool to parse Excel Files and split CSV files, includeing filtering and grouping into smaller files based on a column value')
  .addArgument(filePath)
  .addOption(filterValuesOption)
  .addOption(categoryOption)
  .addOption(filterCondition)
  .addOption(maxFileSizeOption)
  .option('-h, --header [include]', 'enable writing the CSV header to each file (if you do not include this flag, the header will be written separately, even if there is only one file)', false)
  .addCommand(excelCommand)
  .action(async (argFilePath, options) => {
    if (isNil(argFilePath)) {
      ora().fail(chalk.redBright(`No argument provided; you must provide a file path as the command argument`))
      process.exit()
    }
    let inputFilePath = resolve(argFilePath)
    if (!argFilePath.toLowerCase().endsWith('.csv')) {
      ora().fail(`The file path provided (${chalk.cyanBright(`"${argFilePath}"`)}) does not end with ".csv." This program can only parse .csv files. ${chalk.yellowBright('You might need to quote the path if it contains spaces')}.`)
      process.exit()
    }
    inputFilePath = await checkAndResolveFilePath(argFilePath)
    if (options.filters) {
      const validFilters: string[][] = []
      for (const filter of options.filters) {
        const splitFilter = (filter as string).split(':').map(v => v.trim())
        if (splitFilter.length !== 2) {
          ora().info(`Ignoring filter: ${chalk.cyanBright(`"${filter}"`)}. Fields/values must be separated with a colon.`)
          await delay(noop, 1000)
          break
        }
        validFilters.push(splitFilter)
      }
      options.filters = validFilters
    }
    splitCSV(fs.createReadStream(inputFilePath), {
      ...options,
      inputFilePath,
    })
  })

program.parse(process.argv)
export type CommandOptions = Merge<ReturnType<typeof program.opts>, { inputFilePath: string }>

async function checkAndResolveFilePath(argFilePath: string): Promise<string> {
  let inputFilePath = resolve(argFilePath)
  if (!fs.existsSync(inputFilePath)) {
    ora().warn(chalk.yellowBright(`The file path at ${chalk.cyanBright(`"${argFilePath}"`)} provided does not exist.`))
    const combinedPaths = ['Library/CloudStorage', 'Desktop', 'Documents', 'Downloads'].map(v => join(v, '**', argFilePath))
    const files = fg.sync(combinedPaths, {
      cwd: homedir(),
      onlyFiles: true,
      absolute: true,
      objectMode: true,
      deep: 3,
    })
    if (files.length === 0) {
      ora().fail(`No files found matching "${chalk.cyanBright(`"${argFilePath}"`)}"`)
      process.exit()
    }
    else if (files.length === 1) {
      inputFilePath = files[0].path
    }
    else {
      ora().info(`Multiple files found matching ${chalk.cyanBright(`"${argFilePath}"`)}`)
      const fileChoices = files.map(file => ({
        name: relative(homedir(), file.path),
        value: file.path,
      }))
      inputFilePath = await select({
        message: 'Select the file to split/parse',
        choices: fileChoices,
        loop: true,
      })
    }
  }
  return inputFilePath
}

async function setRange(wb: XLSX.WorkBook, inputSheetName: string, inputRange?: string): Promise<string> {
  const worksheetRange = wb.Sheets[inputSheetName]['!ref']!
  const parsedRange = XLSX.utils.decode_range(inputRange ?? worksheetRange)
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
      default: inputRange ?? worksheetRange,
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
