#!/usr/bin/env esno
import { inspect } from 'node:util'
import { Command } from '@commander-js/extra-typings'
import type { Merge } from 'type-fest'

import pkg from '../package.json'
import {
  categoryOption,
  filterTypeOption,
  filterValuesOption,
  makeFilePathOption,
  maxFileSizeOption,
  writeHeaderOption,
} from './arguments'
import { checkAndResolveFilePath } from './excel/helpers'
// .passThroughOptions()

// .action(async (argFilePath, options) => {})
const program = new Command('parse').version(pkg.version)
  // .enablePositionalOptions()
  .description('A CLI tool to parse and split Excel Files and split CSV files, includes the ability to filter and group into smaller files based on a column value and/or file size')
  // .addCommand(csvCommand, { isDefault: true })
  // .addCommand(excelCommand)
  .showSuggestionAfterError(true)
  .configureHelp({
    sortOptions: true,
    sortSubcommands: true,
    showGlobalOptions: true,
  })
  // .enablePositionalOptions()
  .addOption(filterValuesOption)
  .addOption(categoryOption)
  .addOption(filterTypeOption)
  .addOption(maxFileSizeOption)
  .addOption(writeHeaderOption)

program.command('excel')
  .description('Parse an Excel file')
  .addOption(makeFilePathOption('Excel'))
  .option('--sheet [sheet name]', 'the sheet containing the data to parse to CSV')
  .option('--range [range]', 'the range of cells to parse in the Excel file')
  .action(async (options, command) => {
    const filePath = await checkAndResolveFilePath('Excel', options.filePath as string)
    const programOptions = {
      ...command.opts(),
      ...command.optsWithGlobals(),
      filePath,
    }
    console.log(inspect({ excelOptions: programOptions }, { colors: true }))
  })

program.command('csv')
  .description('Parse a CSV file')
  .addOption(makeFilePathOption('CSV'))
  .action(async (options, command) => {
    const filePath = await checkAndResolveFilePath('CSV', options.filePath as string)
    const programOptions = {
      ...command.opts(),
      ...command.optsWithGlobals(),
      filePath,
    }
    console.log(inspect({ csvOptions: programOptions }, { colors: true }))
  })
// .passThroughOptions()

// .passThroughOptions()
// program.command('parse-csv', { isDefault: true })

// program.command('parse-excel')
//   .description('Parse an Excel file')
//   .option('--sheet [sheet name]', 'the sheet containing the data to parse to CSV')
//   .option('--range [range]', 'the range of cells to parse in the Excel file')
//   .addOption(makeFilePathOption('Excel'))
for (const cmd of program.commands) {
  cmd.option('-d, --debug')
}
program.parse(process.argv)
export type CommandOptions = Merge<ReturnType<typeof program.opts>, { inputFilePath: string }>

// async function setRange(wb: XLSX.WorkBook, inputSheetName: string, inputRange?: string): Promise<string> {
//   const worksheetRange = wb.Sheets[inputSheetName]['!ref']!
//   const parsedRange = XLSX.utils.decode_range(inputRange ?? worksheetRange)
//   const isRowInRange = (input: number): boolean => inRange(input, parsedRange.s.r, parsedRange.e.r + 1)
//   const isColumnInRange = (input: number): boolean => inRange(input, parsedRange.s.c, parsedRange.e.c + 1)
//   const isRangeInDefaultRange = (r: XLSX.Range): boolean => isRowInRange(r.s.r) && isColumnInRange(r.s.c) && isRowInRange(r.e.r) && isColumnInRange(r.e.c)
//   const rangeType = await expand({
//     message: 'How do you want to specify the range of the worksheet to parse?',
//     default: 'e',
//     expanded: true,
//     choices: [
//       {
//         name: 'Excel Format (e.g. A1:B10)',
//         value: 'Excel Format',
//         key: 'e',
//       },
//       {
//         name: 'By specifying the start/end row numbers and the start/end column letters',
//         value: 'Numbers and Letters',
//         key: 'n',
//       },
//     ],
//   })
//   if (rangeType === 'Excel Format') {
//     return input({
//       message: 'Enter the range of the worksheet to parse',
//       default: inputRange ?? worksheetRange,
//       validate: (value: string) => {
//         const isValidInput = isRangeInDefaultRange(XLSX.utils.decode_range(value))
//         if (!isValidInput)
//           return `The range must be within the worksheet's default range (${XLSX.utils.encode_range(parsedRange)})`
//         return true
//       },
//     })
//   }
//   else {
//     const startRow = await number({
//       message: 'Enter the starting row number',
//       default: parsedRange.s.r + 1,
//       min: parsedRange.s.r + 1,
//       max: parsedRange.e.r + 1,
//       step: 1,
//     })
//     const endRow = await number({
//       message: 'Enter the ending row number',
//       default: parsedRange.e.r + 1,
//       min: startRow,
//       max: parsedRange.e.r + 1,
//       step: 1,
//     })
//     const startCol = await input({
//       message: 'Enter the starting column reference (e.g., A)',
//       default: XLSX.utils.encode_col(parsedRange.s.c),

//       validate: (value: string) => {
//         const valueIsValid = /^[A-Z]+$/.test(value)
//         if (!valueIsValid) {
//           return `Invalid column reference. Column references are uppercase letters. The worksheet has data in columns "${XLSX.utils.encode_col(parsedRange.s.c)}" to "${XLSX.utils.encode_col(parsedRange.e.c)}"`
//         }
//         return true
//       },
//     })
//     const endCol = await input({
//       message: 'Enter the ending column reference (e.g., AB)',
//       default: XLSX.utils.encode_col(parsedRange.e.c),

//       validate: (value: string) => {
//         const isGreaterThanOrEqualToStartColumn = XLSX.utils.decode_col(value) >= XLSX.utils.decode_col(startCol)
//         const isValidReference = /^[A-Z]+$/.test(value)
//         if (!isValidReference) {
//           return `Invalid column reference. Column references are uppercase letters. The worksheet has data in columns "${XLSX.utils.encode_col(parsedRange.s.c)}" to "${XLSX.utils.encode_col(parsedRange.e.c)}"`
//         }
//         else if (!isGreaterThanOrEqualToStartColumn) {
//           return `The ending column reference must be greater than or equal to the starting column reference ("${startCol}")`
//         }
//         return true
//       },
//     })

//     return `${startCol}${startRow}:${endCol}${endRow}`
//   }
// }

// async function setSheetName(wb: XLSX.WorkBook): Promise<string | undefined> {
//   return select({
//     message: 'Select the worksheet to parse',
//     choices: wb.SheetNames.map((value, i) => ({
//       name: `${i + 1}) ${value}`,
//       value,
//       short: value,
//     })),
//   })
// }
