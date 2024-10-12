
import { homedir } from 'node:os'
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
import { select } from '@inquirer/prompts'
import { splitCSV } from './from-csv'

const filePath = new Commander.Argument('<path>', 'the full path to the CSV file')
const filterValuesOption = new Commander.Option(
  '-f, --filters [operators...]',
  `one or more pairs of colum headers and values to apply as a filter to each row as ${chalk.bgGrey(`${chalk.cyan('ColumnName')}${chalk.whiteBright(':')}${chalk.yellow('FilterValue')}`)}`,
).preset([] as string[][])
  .implies({ matchType: `all` })
const categoryOption = new Commander.Option(
  '-c, --category-field [columnTitle]',
  'the name of a column whose value will be used to create each separate file',
)
  .preset('')
  .default(undefined)
const filterCondition = new Commander.Option('-m, --match-type', 'the type of match to use when filtering rows').choices([`all`, `any`, `none`])
  .preset(`all`)
const program = new Commander.Command()
  .version('0.1.0')
  .addArgument(filePath)
  .addOption(filterValuesOption)
  .addOption(categoryOption)
  .addOption(filterCondition)

  .option('-s, --max-file-size-in-mb [NUMBER]', 'the maximum size of each file in MB', (val): number | undefined => typeof val === 'undefined' || val === null || isNaN(toNumber(val)) ? undefined : toNumber(val), undefined)
  .option('-h, --header', 'whether to write the CSV header on each file (if not set to true, the header will be written separately, even if there is only one file)', false)
  .action(async (argFilePath, options) => {
    let inputFilePath = resolve(argFilePath)
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
    if (isNil(argFilePath)) {
      ora().fail(chalk.redBright(`No argument provided; you must provide a file path as the command argument`))
      process.exit()
    }
    else if (!argFilePath.endsWith('.csv')) {
      ora().fail(`The file path provided (${chalk.cyanBright(`"${argFilePath}"`)}) does not end with ".csv." This program can only parse .csv files. ${chalk.yellowBright('You might need to quote the path if it contains spaces')}.`)
      process.exit()
    }
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
    splitCSV({
      ...options,
      inputFilePath,
    })
  })

program.parse(process.argv)
export type CommandOptions = Merge<ReturnType<typeof program.opts>, { inputFilePath: string }>
