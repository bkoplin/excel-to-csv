import * as Commander from '@commander-js/extra-typings'
import { parseArguments } from './index'

const program = new Commander.Command()
  .version('0.1.0')
  .option('-f, --file-path <STRING>', 'path to Excel file')
  .option('-s, --sheet-name <STRING>', 'name of source worksheet')
  .option('-r, --range <STRING>', 'range of worksheet to parse')
  .action(parseArguments)

program.parse(process.argv)
