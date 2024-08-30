import { existsSync } from 'node:fs'
import process from 'node:process'
import * as Commander from '@commander-js/extra-typings'
import colors from 'picocolors'
import yoctoSpinner from 'yocto-spinner'
import { parseWorksheet } from './index'

const spinner = yoctoSpinner({ text: 'Parsing…' })
const program = new Commander.Command()
  .version('0.1.0')
  .option('-f, --file-path <STRING>', 'path to Excel file', (filePath: string) => {
    if (existsSync(filePath))
      return filePath
    else throw new Commander.InvalidArgumentError(`${colors.yellow(`"${filePath}"`)} does not exist`)
  })
  .option('-s, --sheet-name <STRING>', 'name of source worksheet')
  .option('-r, --range <STRING>', 'range of worksheet to parse')
  .action(args => parseWorksheet(args, spinner))

program.parse(process.argv)
