import type * as csv from 'csv'
import type { Info } from 'csv-parse'
import type { Primitive } from 'type-fest'
import type { CSVOptionsWithGlobals } from './types'
import { createReadStream } from 'node:fs'
import { basename } from 'node:path'
import { PassThrough } from 'node:stream'
import { Command } from '@commander-js/extra-typings'
import * as Prompts from '@inquirer/prompts'
import chalk from 'chalk'
import { parse as parser } from 'csv-parse'
import { stringify as stringifier } from 'csv-stringify'
import fs from 'fs-extra'
import {
  isArray,
  isNull,
  isUndefined,
} from 'lodash-es'
import ora from 'ora'
import {
  join,
  parse,
} from 'pathe'
import yaml from 'yaml'
import pkg from '../package.json'
import {
  checkAndResolveFilePath,
  formatHeaderValues,
  generateCommandLineString,
  generateParsedCsvFilePath,

  selectGroupingField,
  tryPrompt,
} from './helpers'
import categoryOption from './options/categoryField'
import delimiterOption from './options/delimiter'
import fileSizeOption from './options/fileSize'
import fromLineOption from './options/fromLine'
import makeFilePathOption from './options/makeFilePath'
import includesHeaderOption from './options/rangeIncludesHeader'
import toLineOption from './options/rowCount'
import filterValuesOption from './options/rowFilters'
import writeHeaderOption from './options/writeHeader'
import { excelCommamd } from './subcommands/excelCommand'
import writeCsv from './writeCsv'

export const spinner = ora({
  hideCursor: false,
  discardStdin: false,
})

export const program = new Command(pkg.name).version(pkg.version)
.description('A CLI tool to parse, filter and split Excel and CSV files and write the results to new CSV files of a specified size')
.showSuggestionAfterError(true)
.configureHelp({ sortSubcommands: true })
.addCommand(excelCommamd)
.hook('postAction', async (thisCommand, actionCommand) => {

})

export const _csvCommands = program.command('csv')
  .description('Parse a CSV file')
  .addOption(makeFilePathOption('CSV'))
  .addOption(fromLineOption)
  .addOption(toLineOption)
  .addOption(fileSizeOption)
  .addOption(includesHeaderOption)
  .addOption(writeHeaderOption)
  .addOption(filterValuesOption)
  .addOption(categoryOption)
  .addOption(delimiterOption)
  .action(async (options, command) => {
    const globalOptions = command.optsWithGlobals<CSVOptionsWithGlobals>()

    globalOptions.command = 'CSV'
    globalOptions.filePath = await checkAndResolveFilePath({
      fileType: 'CSV',
      argFilePath: globalOptions.filePath,
    })
    command.setOptionValueWithSource('filePath', globalOptions.filePath, 'env')
    if (isUndefined(globalOptions.rangeIncludesHeader)) {
      globalOptions.rangeIncludesHeader = await Prompts.confirm({
        message: `Does ${basename(globalOptions.filePath)} include a header row?`,
        default: true,
      })
      command.setOptionValueWithSource('rangeIncludesHeader', globalOptions.rangeIncludesHeader, 'env')
    }
    if (globalOptions.rangeIncludesHeader === false && globalOptions.writeHeader === true) {
      globalOptions.writeHeader = false
      command.parent?.setOptionValueWithSource('writeHeader', false, 'env')
    }

    const parsedOutputFile = generateParsedCsvFilePath({
      parsedInputFile: parse(globalOptions.filePath),
      filters: globalOptions.rowFilters,
    })

    fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), yaml.stringify({
      parsedCommandOptions: globalOptions,
      commandLineString: generateCommandLineString(globalOptions, command),
    }, { lineWidth: 1000 }))
    parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
    fs.ensureDirSync(parsedOutputFile.dir)

    let recordCount = 0

    let skippedLines = 1 - globalOptions.fromLine

    let header: Exclude<csv.parser.ColumnOption, Primitive>[] = []

    let tryToSetCategory = true

    let isWriting = false

    let bytesRead = 0

    let inputStreamReader: csv.stringifier.Stringifier
    // const inputStreamReader = stringifier({
    //   bom: true,
    //   columns: globalOptions.rangeIncludesHeader && header.length > 0 ? header : undefined,
    //   header: globalOptions.rangeIncludesHeader ? header.length > 0 : undefined,
    // })

    const sourceStream = createReadStream(globalOptions.filePath, 'utf-8')

    const outputStream = new PassThrough({ encoding: 'utf-8' })

    const lineReader = parser({
      bom: true,
      from_line: globalOptions.fromLine,
      to_line: isNull(globalOptions.toLine) ? undefined : globalOptions.toLine,
      trim: true,
      delimiter: globalOptions.delimiter,
      columns: (record: string[]) => {
        if (globalOptions.rangeIncludesHeader !== true)
          return false

        return formatHeaderValues({ data: record })

        // return header
      },
      info: true,
      skip_records_with_error: true,
      on_record: ({
        info,
        record,
      }: {
        info: Info
        record: Record<string, string> | Array<string>
      }) => {
        bytesRead = info.bytes
        recordCount = info.records
        skippedLines = info.lines - info.records
        if (header.length === 0 && isArray(info.columns)) {
          header = info.columns as Exclude<csv.parser.ColumnOption, Primitive>[]
        }

        return {
          info,
          record,
        }
      },
    })

    // .pipe(outputStream)
    // .pipe(outputStream)
    lineReader.on('data', async ({
      // info,
      record,
    }: {
      info: Info
      record: Record<string, string> | Array<string>
    }) => {
      if (globalOptions.rangeIncludesHeader === true && !globalOptions.categoryField && tryToSetCategory) {
        lineReader.pause()
        await selectGroupingField(Object.keys(record), command)
        globalOptions.categoryField = command.parent!.getOptionValue('categoryField') as string
        tryToSetCategory = false
        lineReader.resume()
      }
      if (typeof inputStreamReader === 'undefined') {
        if (header.length > 0 && globalOptions.rangeIncludesHeader) {
          inputStreamReader = stringifier({
            bom: true,
            columns: header.map(({ name }) => ({
              name,
              key: name,
            })),
            header: globalOptions.header,
          })
        }
        else {
          inputStreamReader = stringifier({ bom: true })
        }
        lineReader.pipe(inputStreamReader).pipe(outputStream)
      }
      else if (isWriting === false) {
        writeCsv(outputStream, globalOptions, {
          parsedOutputFile,
          skippedLines,
          bytesRead,
          spinner,
          files: [],
          fields: (header ?? []).map(h => h.name),
          parsedLines: skippedLines + recordCount,
        })
        isWriting = true
      }
      // inputStreamReader.write(line)
      // else {
      //   inputStreamReader.write(line)
      // }
    })
    sourceStream.pipe(lineReader)
  })

program.parse(process.argv)
async function updateCommandOptions(command, globalOptions) {
  for (const commandOption of command.options) {
    const attributeName = commandOption.attributeName() as keyof typeof globalOptions

    const val = command.getOptionValue(attributeName)

    const source = command.getOptionValueSource(attributeName)

    if (typeof source !== 'undefined' && source !== 'env') {
      const optionMessage = `Should ${chalk.yellowBright(commandOption.long)} be set to ${chalk.cyanBright(val)}?\n(${commandOption.description})`

      const [, setValueAnswer] = await tryPrompt('confirm', {
        message: optionMessage,
        default: true,

      })

      if (setValueAnswer === false) {
        if (commandOption.argChoices) {
          const [, optionValue] = await tryPrompt('select', {
            message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})`,
            choices: commandOption.argChoices,
            default: val,
          })

          // globalOptions[attributeName] = optionValue
        }
        else if (typeof val === 'boolean') {
          const [, optionValue] = await tryPrompt('select', {
            message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})`,
            default: val,
            choices: [{
              name: 'true',
              value: true,
            }, {
              name: 'false',
              value: false,
            }],
          })

          // globalOptions[attributeName] = optionValue
        }
        else {
          const [, optionValue] = await tryPrompt('input')({ message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})` })

          // globalOptions[attributeName] = optionValue
        }
      }
    }
  }
}
