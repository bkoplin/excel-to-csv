#!/usr/bin/env esno
import {
  ReadStream,
  createReadStream,
} from 'node:fs'
import type { ParsedPath } from 'node:path'
import {
  Command,
  Option,
} from '@commander-js/extra-typings'
import type {
  JsonPrimitive,
  JsonValue,
  Merge,
  SetFieldType,
  Simplify,
} from 'type-fest'
import Papa from 'papaparse'
import {
  isEmpty,
  isNull,
  isUndefined,
  range as lRange,
} from 'lodash-es'
import { filename } from 'pathe/utils'
import ora from 'ora'

import {
  join,
  parse,
} from 'pathe'
import fs from 'fs-extra'
import yaml from 'yaml'
import { objectEntries } from '@antfu/utils'
import {
  isObject,
  isPrimitive,
  objectify,
} from 'radash'
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
    const globalOptions = command.optsWithGlobals < Merge<GlobalOptions, ReturnType<typeof command.opts>>>()
    let {
      header,
      rowFilters,
      range,
      sheet,
      rangeIncludesHeader,
      filePath,
    } = globalOptions
    filePath = await checkAndResolveFilePath('Excel', filePath)
    const parsedOutputFile = generateParsedCsvFilePath(parse(filePath), rowFilters as Record<string, Array<JsonPrimitive>>)
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
    if (rangeIncludesHeader === false && header === true)
      header = false
    const {
      parsedRange,
      isRowInRange,
    } = extractRangeInfo(ws, range)
    const data: (JsonPrimitive | Date)[][] = []
    const rowIndices = lRange(parsedRange.s.r, parsedRange.e.r + 1)
    const colIndices = lRange(parsedRange.s.c, parsedRange.e.c + 1)
    for (const rowIndex of rowIndices) {
      const row: (JsonPrimitive | Date)[] = []
      for (const colIndex of colIndices) {
        // const cellRef = XLSX.utils.encode_cell({
        //   r: rowIndex,
        //   c: colIndex,
        // })
        const cell = ws?.['!data']?.[rowIndex]?.[colIndex]
        row.push(cell?.v ?? null)
      }
      data.push(row)
    }

    const csv = Papa.unparse(data, { header: globalOptions.rangeIncludesHeader })

    const combinedOptions = {
      ...globalOptions,
      parsedOutputFile,
      filePath,
      range,
      sheet,
      header,
    }
    const commandLineString = generateCommandLineString(combinedOptions, command)
    fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), yaml.stringify({
      combinedOptions,
      commandLineString,
    }, { lineWidth: 1000 }))
    parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
    fs.ensureDirSync(parsedOutputFile.dir)
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
    const globalOptions = command.optsWithGlobals<Merge<GlobalOptions, ReturnType<typeof command.opts>>>()
    let {
      header,
      rowFilters,
      filePath,
    } = globalOptions
    filePath = await checkAndResolveFilePath('Excel', filePath)
    const parsedOutputFile = generateParsedCsvFilePath(parse(filePath), rowFilters as Record<string, Array<JsonPrimitive>>)
    filePath = await checkAndResolveFilePath('CSV', options.filePath as string)
    const combinedOptions = {
      ...globalOptions,
      parsedOutputFile,
      filePath,
    }
    fs.outputFileSync(join(parsedOutputFile.dir, `PARSE AND SPLIT OPTIONS.yaml`), yaml.stringify({
      combinedOptions,
      commandLineString: generateCommandLineString(combinedOptions, command),
    }, { lineWidth: 1000 }))
    parsedOutputFile.dir = join(parsedOutputFile.dir, 'DATA')
    fs.ensureDirSync(parsedOutputFile.dir)
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

function generateCommandLineString(combinedOptions: Record<string | number, JsonValue | undefined>, command: Command & { _name?: string }): string {
  return objectEntries(combinedOptions).reduce((acc, [key, value]): string => {
    const optionFlags = objectify([...command.options, ...command.parent?.options ?? []], o => o.attributeName(), o => o.long)
    if (key in optionFlags) {
      if (!Array.isArray(value)) {
        if (isPrimitive(value)) {
          acc += ` \\\n${optionFlags[key]} ${JSON.stringify(value)}`
        }
        else if (isObject(value) && !isEmpty(value)) {
          acc += ` \\\n${optionFlags[key]} ${objectEntries(value).map(([k, v]) => `${(JSON.stringify(k))}:${(JSON.stringify(v))}`)
            .join(' ')}`
        }
      }
      else if (!isNull(value) && !isUndefined(value) && !isEmpty(value)) {
        acc += ` \\\n${optionFlags[key]} ${value.map(v => JSON.stringify(v)).join(' ')}`
      }
    }
    return acc
  }, command._name!)
}
