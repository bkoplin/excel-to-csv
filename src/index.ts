#!/usr/bin/env esno
import {
  ReadStream,
  createReadStream,
} from 'node:fs'
import type { ParsedPath } from 'node:path'
import XLSX from 'xlsx'
import {
  Command,
  Option,
} from '@commander-js/extra-typings'
import type {
  JsonPrimitive,
  Merge,
  SetFieldType,
  Simplify,
} from 'type-fest'
import Papa from 'papaparse'
import {
  isEmpty,
  isUndefined,
} from 'lodash-es'
import { filename } from 'pathe/utils'
import ora from 'ora'

import {
  join,
  parse,
} from 'pathe'
import fs from 'fs-extra'
import yaml from 'yaml'
import pkg from '../package.json'
import {
  categoryOption,
  filterTypeOption,
  filterValuesOption,
  makeFilePathOption,
  maxFileSizeOption,
  writeHeaderOption,
} from './arguments'
import {
  checkAndResolveFilePath,
  generateParsedCsvFilePath,
} from './helpers'
import {
  extractRangeInfo,
  getWorkbook,
  isOverlappingRange,
  setRange,
  setRangeIncludesHeader,
  setSheetName,
} from './excel'
import writeCsv from './writeCsv'

const program = new Command('parse').version(pkg.version)

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

export type GlobalOptions = Simplify<SetFieldType<ReturnType<typeof program.opts>, 'fileSize', number | undefined> & {
  inputFilePath: string
  parsedOutputFile: Omit<ParsedPath, 'base'>
}>

program.command('excel')
  .description('Parse an Excel file')
  .addOption(makeFilePathOption('Excel'))
  .addOption(new Option('--sheet [sheet name]', 'the sheet containing the data to parse to CSV').default(undefined)
    .preset(''))
  .addOption(new Option('--range [range]', 'the range of cells to parse in the Excel file').preset('')
    .default(undefined))
  .addOption(new Option('-r, --range-includes-header', 'flag to indicate whether the range include the header row').preset<boolean>(true))
  .action(async (options, command) => {
    const globalOptions = command.optsWithGlobals<GlobalOptions>()
    const localOptions = command.opts()
    let {
      header,
      rowFilters,
    } = globalOptions
    let {
      range,
      sheet,
      rangeIncludesHeader,
      filePath,
    } = localOptions
    filePath = await checkAndResolveFilePath('Excel', filePath)
    const parsedOutputFile = generateParsedCsvFilePath(parse(filePath), rowFilters)
    const wb = await getWorkbook(filePath)
    if (isUndefined(sheet) || typeof sheet !== 'string' || !wb.SheetNames.includes(sheet)) {
      sheet = await setSheetName(wb)
    }
    const ws = wb.Sheets[sheet!]
    if (typeof ws === 'undefined') {
      ora(`The worksheet "${sheet}" does not exist in the Excel file ${filename(filePath)}`).fail()
      process.exit(1)
    }
    if (!isOverlappingRange(ws, range)) {
      range = await setRange(wb, sheet)
    }
    if (isUndefined(rangeIncludesHeader)) {
      rangeIncludesHeader = await setRangeIncludesHeader(range) as true
    }
    if (rangeIncludesHeader === false)
      header = false
    const {
      parsedRange,
      isRowInRange,
    } = extractRangeInfo(ws, range)
    const json = XLSX.utils.sheet_to_json(ws, {
      range,
      raw: true,
      header: 1,
    })

    const data = (json as JsonPrimitive[][]).filter((v, i) => !isEmpty(v) && !isUndefined(v) && isRowInRange(i))

    const csv = Papa.unparse(data, { header: localOptions.rangeIncludesHeader })

    fs.outputFileSync(join(parsedOutputFile.dir, `${parsedOutputFile.name} OPTIONS.yaml`), yaml.stringify({
      ...globalOptions,
      ...localOptions,
      parsedOutputFile,
      filePath,
      range,
      sheet,
      header,
    }))
    writeCsv(ReadStream.from(csv), {
      ...globalOptions,
      header,
      inputFilePath: filePath,
      parsedOutputFile,
    })
  })

program.command('csv')
  .description('Parse a CSV file')
  .addOption(makeFilePathOption('CSV'))
  .action(async (options, command) => {
    const globalOptions = command.optsWithGlobals<GlobalOptions>()
    const localOptions = command.opts()
    const {
      header,
      rowFilters,
    } = globalOptions
    let { filePath } = localOptions
    filePath = await checkAndResolveFilePath('Excel', filePath)
    const parsedOutputFile = generateParsedCsvFilePath(parse(filePath), rowFilters)
    filePath = await checkAndResolveFilePath('CSV', options.filePath as string)
    fs.outputFileSync(join(parsedOutputFile.dir, `${parsedOutputFile.name} OPTIONS.yaml`), yaml.stringify({
      ...globalOptions,
      ...localOptions,
      parsedOutputFile,
      filePath,
    }))
    writeCsv(createReadStream(filePath), {
      ...globalOptions,
      header,
      inputFilePath: filePath,
      parsedOutputFile,
    })
  })

for (const cmd of program.commands) {
  cmd.option('-d, --debug')
}
program.parse(process.argv)
export type CommandOptions = Merge<ReturnType<typeof program.opts>, { inputFilePath: string }>
