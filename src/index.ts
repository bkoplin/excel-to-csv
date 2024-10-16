import type {
  JsonPrimitive,
  Merge,
  Simplify,
} from 'type-fest'
import {
  createReadStream,
  ReadStream,
} from 'node:fs'
import { Command } from '@commander-js/extra-typings'
import fs from 'fs-extra'
import { isUndefined } from 'lodash-es'
import ora from 'ora'
import Papa from 'papaparse'
import {
  join,
  parse,
} from 'pathe'
import { filename } from 'pathe/utils'
import yaml from 'yaml'
import pkg from '../package.json'
import {
  categoryOption,
  filterTypeOption,
  filterValuesOption,
  includesHeaderOption,
  makeFilePathOption,
  maxFileSizeOption,
  rowCountOption,
  sheetNameOption,
  sheetRangeOption,
  skipLinesOption,
  writeHeaderOption,
} from './arguments'
import {
  extractDataFromWorksheet,
  extractRangeInfo,
  getWorkbook,
  isOverlappingRange,
  setRange,
  setRangeIncludesHeader,
  setSheetName,
} from './excel'
import {
  checkAndResolveFilePath,
  generateCommandLineString,
  generateParsedCsvFilePath,
} from './helpers'
import writeCsv from './writeCsv'

const program = new Command(pkg.name).version(pkg.version)
.description('A CLI tool to parse and split Excel Files and split CSV files, includes the ability to filter and group into smaller files based on a column value and/or file size')
.showSuggestionAfterError(true)
.configureHelp({
  sortOptions: true,
  sortSubcommands: true,
  showGlobalOptions: true,
})
.addOption(filterValuesOption)
.addOption(categoryOption)
.addOption(filterTypeOption)
.addOption(maxFileSizeOption)
.addOption(writeHeaderOption)

const _excelCommands = program.command('excel')
  .description('Parse an Excel file')
  .addOption(makeFilePathOption('Excel'))
  .addOption(sheetNameOption)
  .addOption(sheetRangeOption)
  .addOption(includesHeaderOption)
  .action(async (options, command) => {
    const globalOptions = command.optsWithGlobals < Merge<GlobalOptions, ReturnType<typeof command.opts>>>()

    globalOptions.filePath = await checkAndResolveFilePath('Excel', globalOptions.filePath)

    const parsedOutputFile = generateParsedCsvFilePath(parse(globalOptions.filePath), globalOptions.rowFilters)

    const wb = await getWorkbook(globalOptions.filePath)

    if (isUndefined(globalOptions.sheet) || typeof globalOptions.sheet !== 'string' || !wb.SheetNames.includes(globalOptions.sheet)) {
      globalOptions.sheet = await setSheetName(wb)
    }

    const ws = wb.Sheets[globalOptions.sheet!]

    parsedOutputFile.name = `${parsedOutputFile.name} ${globalOptions.sheet}`
    if (typeof ws === 'undefined') {
      ora(`The worksheet "${globalOptions.sheet}" does not exist in the Excel file ${filename(globalOptions.filePath)}`).fail()
      process.exit(1)
    }
    if (!isOverlappingRange(ws, globalOptions.range)) {
      globalOptions.range = await setRange(wb, globalOptions.sheet)
      isOverlappingRange(ws, globalOptions.range)
    }

    if (isUndefined(globalOptions.rangeIncludesHeader))
      globalOptions.rangeIncludesHeader = await setRangeIncludesHeader(globalOptions.range, globalOptions.rangeIncludesHeader)

    if (globalOptions.rangeIncludesHeader === false && globalOptions.header === true)
      globalOptions.header = false

    const { parsedRange } = extractRangeInfo(ws, globalOptions.range)

    const data: (JsonPrimitive | Date)[][] = extractDataFromWorksheet(parsedRange, ws)

    const csv = Papa.unparse(data, { header: globalOptions.rangeIncludesHeader })

    const commandLineString = generateCommandLineString(globalOptions, command)

    fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), yaml.stringify({
      parsedCommandOptions: globalOptions,
      commandLineString,
    }, { lineWidth: 1000 }))
    parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
    fs.ensureDirSync(parsedOutputFile.dir)
    writeCsv(ReadStream.from(csv), {
      ...globalOptions,
      parsedOutputFile,
    })
  })

const _csvCommands = program.command('csv')
  .description('Parse a CSV file')
  .addOption(makeFilePathOption('CSV'))
  .addOption(skipLinesOption)
  .addOption(rowCountOption)
  .action(async (options, command) => {
    const globalOptions = command.optsWithGlobals<GlobalOptions>()

    globalOptions.filePath = await checkAndResolveFilePath('CSV', globalOptions.filePath)

    const parsedOutputFile = generateParsedCsvFilePath(parse(globalOptions.filePath), globalOptions.rowFilters)

    fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), yaml.stringify({
      parsedCommandOptions: globalOptions,
      commandLineString: generateCommandLineString(globalOptions, command),
    }, { lineWidth: 1000 }))
    parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
    fs.ensureDirSync(parsedOutputFile.dir)
    writeCsv(createReadStream(globalOptions.filePath, 'utf-8'), {
      ...globalOptions,
      parsedOutputFile,
    })
  })

type CsvCommand = typeof _csvCommands

type ExcelCommand = typeof _excelCommands

type ProgramCommand = typeof program

export type ProgramOptions = Simplify<ReturnType<CsvCommand['opts']> & ReturnType<ExcelCommand['opts']> & ReturnType<ProgramCommand['opts']>>

export type GlobalOptions = { [Prop in keyof ProgramOptions]: boolean extends ProgramOptions[Prop] ? ProgramOptions[Prop] : Exclude<ProgramOptions[Prop], true> }

export type CommandOptions = Merge<ReturnType<typeof program.opts>, { inputFilePath: string }>

program.parse(process.argv)
