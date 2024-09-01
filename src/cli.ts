import fs from 'node:fs'
import process from 'node:process'
import os from 'node:os'
import { basename, sep } from 'node:path'
import * as Commander from '@commander-js/extra-typings'
import yoctoSpinner from 'yocto-spinner'
import fg from 'fast-glob'
import * as XLSX from 'xlsx'
import { input, select } from '@inquirer/prompts'
import inquirerFileSelector from 'inquirer-file-selector'
import colors from 'picocolors'
import { Separator } from '@inquirer/core'

import { isUndefined } from '@antfu/utils'
import { findIndex } from 'lodash-es'
import { parseWorksheet } from './index'

XLSX.set_fs(fs)
const filePathOption = new Commander.Option('-f, --file-path <STRING>', 'path to Excel file')

const spinner = yoctoSpinner({ text: 'Parsing…' })
const program = new Commander.Command()
  .version('0.1.0')
  .addOption(filePathOption)
  .option('-s, --sheet-name <STRING>', 'name of source worksheet')
  .option('-r, --range <STRING>', 'range of worksheet to parse')
  .action(async (args) => {
    if (isUndefined(args.filePath)) {
      const choices = fg.sync(['Library/CloudStorage/**', 'Desktop', 'Documents', 'Downloads'], { onlyDirectories: true, absolute: true, cwd: os.homedir(), deep: 1 }).sort()
        .map(folder => ({ name: basename(folder), value: folder }))
      const cloudStorageIndex = findIndex(choices, ({ value }) => value.includes('CloudStorage'))
      if (cloudStorageIndex !== -1) {
        choices.splice(cloudStorageIndex, 0, new Separator('---'))
        choices.push(new Separator('---'))
      }
      const dirName = await select({
        name: 'dirName',
        message: 'Select the folder containing the Excel file you want to parse',

        choices,
      })
      const filePath = await inquirerFileSelector({
        message: 'Where do you want to start looking for your Excel file?',
        basePath: dirName,
        hideNonMatch: true,
        allowCancel: true,
        pageSize: 20,
        match(filePath) {
          const isValidFilePath = !filePath.path.split(sep).some(v => v.startsWith('.'))

          return isValidFilePath && (filePath.isDir || /\.xlsx?$/.test(filePath.name))
        },
      })

      args.filePath = filePath
    }
    spinner.text = `Checking for worksheets in ${colors.cyan(`"${args.filePath}"`)}\n`

    const { SheetNames } = XLSX.readFile(args.filePath, { bookSheets: true })
    if (isUndefined(args.sheetName) || !SheetNames.includes(args.sheetName)) {
      const answer = await select<string>({ name: 'sheetName', message: 'Select the worksheet to parse', choices: SheetNames.map(value => ({ name: value, value })) })
      args.sheetName = answer
    }
    if (isUndefined(args.range)) {
      spinner.text = `Checking for data range of worksheet ${colors.cyan(`"${args.sheetName}"`)}  in ${colors.cyan(`"${args.filePath}"`)}\n`
      const wb = XLSX.readFile(args.filePath, { sheet: args.sheetName })
      const answer = await input<string>({ name: 'range', message: 'Enter the range of the worksheet to parse', default: wb.Sheets[args.sheetName]['!ref'] })
      args.range = answer
    }
    await parseWorksheet({ sheetName: args.sheetName, filePath: args.filePath, range: args.range }, spinner)
  })

program.parse(process.argv)
