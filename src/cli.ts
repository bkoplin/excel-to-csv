import fs, { existsSync } from 'node:fs'
import process from 'node:process'
import * as Commander from '@commander-js/extra-typings'
import colors from 'picocolors'
import yoctoSpinner from 'yocto-spinner'
import inquirer from 'inquirer'

import * as XLSX from 'xlsx'

import type { SetRequired } from 'type-fest'
import { parseWorksheet } from './index'

XLSX.set_fs(fs)
const filePathOption = new Commander.Option('-f, --file-path <STRING>', 'path to Excel file')
// .makeOptionMandatory()
//   .argParser((filePath) => {
//     if (existsSync(filePath))
//       return filePath
//     else throw new Commander.InvalidArgumentError(`${colors.yellow(`"${filePath}"`)} does not exist`)
//   })
const spinner = yoctoSpinner({ text: 'Parsing…' })
const program = new Commander.Command()
  .version('0.1.0')
  .addOption(filePathOption)
  // .option('-f, --file-path <STRING>', 'path to Excel file', (filePath: string) => {
  //   if (existsSync(filePath))
  //     return filePath
  //   else throw new Commander.InvalidArgumentError(`${colors.yellow(`"${filePath}"`)} does not exist`)
  // })
  .option('-s, --sheet-name <STRING>', 'name of source worksheet')
  .option('-r, --range <STRING>', 'range of worksheet to parse')
  .action(async (args) => {
    type FinalAnswers = SetRequired<typeof args, 'filePath' | 'range' | 'sheetName'>
    if (!args.filePath) {
      const answer = await inquirer.prompt<[FinalAnswers]>([{ type: 'input', name: 'filePath', message: 'Enter the path to the Excel file', when: !args.filePath, required: true, validate: (filePath: string) => {
        if (existsSync(filePath))
          return true
        else return `${colors.yellow(`"${filePath}"`)} does not exist`
      } }])
      args.filePath = answer.filePath
    }
    if (!args.sheetName) {
      const { SheetNames } = XLSX.readFile(args.filePath, { bookSheets: true })
      const answer = await inquirer.prompt<[FinalAnswers]>([{ type: 'list', name: 'sheetName', message: 'Select the worksheet to parse', choices: SheetNames }])
      args.sheetName = answer.sheetName
    }
    if (!args.range) {
      const wb = XLSX.readFile(args.filePath, { sheet: args.sheetName })
      const answer = await inquirer.prompt<[FinalAnswers]>([{ type: 'input', name: 'range', message: 'Enter the range of the worksheet to parse', default: wb.Sheets[args.sheetName]['!ref'] }])
      args.range = answer.range
    }
    parseWorksheet(args, spinner)
  })

program.parse(process.argv)
