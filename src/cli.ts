import fs from 'node:fs'
import process from 'node:process'
import os from 'node:os'
import { basename, resolve, sep } from 'node:path'
import * as Commander from '@commander-js/extra-typings'
import yoctoSpinner from 'yocto-spinner'
import fg from 'fast-glob'
import * as XLSX from 'xlsx'
import { input, select } from '@inquirer/prompts'
import inquirerFileSelector from 'inquirer-file-selector'

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
    if (!args.filePath) {
      const sourceFolders = fg.sync(['Library/CloudStorage/**'], { onlyDirectories: true, absolute: true, cwd: os.homedir(), deep: 1 })
      const dirName = await select({
        name: 'dirName',
        message: 'Select the folder containing the Excel file you want to parse',
        pageSize: 20,
        choices: [...['Desktop', 'Downloads', 'Documents'].map(v => resolve(v)), ...sourceFolders].map(folder => ({ name: basename(folder), value: folder })),
      })
      const filePath = await inquirerFileSelector({
        message: 'Where do you want to start looking for your Excel file?',
        basePath: dirName,
        hideNonMatch: true,
        allowCancel: true,
        pageSize: 20,
        match(filePath) {
          const isValidFilePath = !filePath.path.split(sep).some(v => v.startsWith('.'))
          // console.log({ filePath, isValidFilePath })
          return isValidFilePath && (filePath.isDir || /\.xlsx?$/.test(filePath.name))
        },
      })
      // // const dirName = await input({
      // //   type: 'input',
      // //   name: 'dirName',
      // //   message: 'What folder contains the Excel file you want to parse',
      // //   default: os.homedir(),
      // //   validate: (dirName: string) => {
      // //     const directory = fg.sync(dirName, { onlyDirectories: true, absolute: true, cwd: os.homedir(), })
      // //     if (directory.length)
      // //       return true
      // //     else return `${colors.yellow(`"${dirName}"`)} is not a valid directory`
      // //   },
      // // })
      // const filePath = await inquirerFileSelector({
      //   message: 'Select the Excel file you want to parse',
      //   basePath: dirName,
      //   hideNonMatch: true,
      //   match(filePath) {
      //     if (!/^[A-Z0-9]/i.test(filePath.name))
      //       return false
      //     else if (!statSync(filePath.path).isDirectory())
      //       return false
      //     const subFiles = fg.sync(['**/*.xls', '**/*.xlsx'], { cwd: filePath.path, onlyFiles: true })
      //     return subFiles.length > 0
      //   },
      // })
      args.filePath = filePath
    }
    if (!args.sheetName) {
      const { SheetNames } = XLSX.readFile(args.filePath, { bookSheets: true })
      const answer = await select<string>({ name: 'sheetName', message: 'Select the worksheet to parse', choices: SheetNames.map(value => ({ name: value, value })) })
      args.sheetName = answer
    }
    if (!args.range) {
      const wb = XLSX.readFile(args.filePath, { sheet: args.sheetName })
      const answer = await input<string>({ name: 'range', message: 'Enter the range of the worksheet to parse', default: wb.Sheets[args.sheetName]['!ref'] })
      args.range = answer
    }
    parseWorksheet(args, spinner)
  })

program.parse(process.argv)
