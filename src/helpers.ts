import type { Command } from '@commander-js/extra-typings'
import type { Stringifier } from 'csv-stringify'
import type { ParsedPath } from 'node:path'
import type { Readable } from 'node:stream'
import type {
  AsyncReturnType,
  ConditionalPick,
  EmptyObject,
  JsonPrimitive,
  StringKeyOf,
} from 'type-fest'
import type {
  CombinedProgramOptions,
  FileMetrics,
} from './types'
import { homedir } from 'node:os'
import { objectEntries } from '@antfu/utils'
import { Separator } from '@inquirer/core'
import { select } from '@inquirer/prompts'
import * as Prompts from '@inquirer/prompts'
import colors from 'chalk'
import dayjs from 'dayjs'
import fg from 'fast-glob'
import filenamify from 'filenamify'
import fs from 'fs-extra'
import inquirerFileSelector from 'inquirer-file-selector'
import {
  findIndex,
  has,
  isArray,
  isEmpty,
  isNil,
  isNull,
  isObject,
  padStart,
} from 'lodash-es'
import {
  anyOf,
  carriageReturn,
  createRegExp,
  exactly,
  linefeed,
  maybe,
  whitespace,
} from 'magic-regexp'
import ora from 'ora'
import * as Papa from 'papaparse'
import {
  basename,
  join,
  relative,
} from 'pathe'
import {
  get,
  objectify,
  omit,
  shake,
  tryit,

} from 'radash'
import isNumeric from 'validator/lib/isNumeric'
import yaml from 'yaml'
import pkg from '../package.json'

/* async_RS reads a stream and returns a Promise resolving to a workbook */
const spinner = ora({ discardStdin: false })

export async function checkAndResolveFilePath(options: {
  fileType: 'Excel' | 'CSV'
  argFilePath: CombinedProgramOptions['filePath']
}): Promise<string> {
  let argFilePath = options.argFilePath

  if (typeof argFilePath === 'undefined' || isEmpty(argFilePath)) {
    spinner.warn(colors.magentaBright(`You have not provided an input ${options.fileType} file.`))

    const startingFolder = await selectStartingFolder(options.fileType)

    argFilePath = await selectFile(options.fileType, startingFolder)
  }

  else {
    const pathFromCwd = join(process.cwd(), options.argFilePath)

    const pathFromHome = join(homedir(), options.argFilePath)

    const originalPath = options.argFilePath

    if (fs.existsSync(originalPath)) {
      argFilePath = originalPath
    }
    else if (fs.existsSync(pathFromCwd)) {
      argFilePath = pathFromCwd
    }
    else if (fs.existsSync(pathFromHome)) {
      argFilePath = pathFromHome
    }
    else {
      spinner.warn(colors.magentaBright(`Could not find ${options.fileType === 'CSV' ? 'a CSV or' : 'an Excel'} file at the path ${colors.cyanBright(`"${options.argFilePath}"`)}!`))

      const startingFolder = await selectStartingFolder(options.fileType)

      argFilePath = await selectFile(options.fileType, startingFolder)
    }
  }

  return Promise.resolve(argFilePath)
}
export function selectFile(fileType: 'Excel' | 'CSV', basePath: string): Promise<string> {
  let fileExtString

  if (fileType === 'Excel') {
    fileExtString = `${colors.cyanBright('.xls')} or ${colors.cyanBright('.xlsx')}`
  }
  else {
    fileExtString = `${colors.cyanBright('.csv')}, ${colors.cyanBright('.txt')} or ${colors.cyanBright('.tsv')}`
  }

  const pathRegexp = fileType === 'Excel'
    ? createRegExp(exactly('.'), exactly('xlsx').before(maybe('x')).at.lineEnd(), ['i'])
    : createRegExp(exactly('.'), exactly('csv').or('txt')
      .or('tsv').at.lineEnd(), ['i'])

  return inquirerFileSelector({
    message: `Navigate to the ${colors.yellowBright(fileType)} file you want to parse (only files with an ${fileExtString} extension will be shown, and the file names must start with an alphanumeric character)`,
    basePath,
    // showExcluded: false,
    allowCancel: true,
    pageSize: 20,
    theme: { style: { currentDir: (text: string) => colors.magentaBright(join(`.`, basename(basePath), relative(basePath, text))) } },
    match(filePath) {
      return !/^[^A-Z0-9]/i.test(filePath.name) && pathRegexp.test(filePath.name)
    },
  })
}
export function selectStartingFolder(fileType: 'Excel' | 'CSV'): Promise<string> {
  const cloudFolders = fg.sync(['Library/CloudStorage/**'], {
    onlyDirectories: true,
    absolute: true,
    cwd: homedir(),
    deep: 1,
  }).map(folder => ({
    name: basename(folder).replace('OneDrive-SharedLibraries', 'SharePoint-'),
    value: folder,
  }))

  // const homeFolders = fg.sync(['!Desktop', 'Documents', 'Downloads'], {
  const homeFolders = fg.sync([join(homedir(), '/**')], {
    onlyDirectories: true,
    absolute: true,
    cwd: homedir(),
    deep: 1,
    dot: false,
    ignore: ['**/Library', '**/Applications', '**/Music', '**/Movies', '**/Pictures', '**/Public', '**/OneDrive*', '**/Reed Smith*', '**/Git*', '**/Parallels'],
  }).map(folder => ({
    name: basename(folder),
    value: folder,
  }))

  return select({
    message: `Where do you want to start looking for your ${colors.yellowBright(fileType)} file?`,
    pageSize: 20,
    choices: [
      new Separator('----CURRENT----'),
      {
        name: basename(process.cwd()),
        value: process.cwd(),

      },
      new Separator('----ONEDRIVE----'),
      ...cloudFolders,
      new Separator('----HOME----'),
      ...homeFolders,
    ],
  })
}
export function generateParsedCsvFilePath({
  parsedInputFile,
  filters,
  sheetName,
}: {
  parsedInputFile: ParsedPath
  filters: CombinedProgramOptions['rowFilters']
  sheetName?: string
}): Omit<ParsedPath, 'base'> {
  const parsedOutputFile = omit(parsedInputFile, ['base'])

  parsedOutputFile.ext = '.csv'

  const dateTimeString = dayjs().format('YYYY-MM-DD HH-mm')

  const filteredIndicator = !isEmpty(filters) ? ' FILTERED' : ''

  parsedOutputFile.dir = join(parsedOutputFile.dir, `${parsedInputFile.name}`)
  if (typeof sheetName !== 'undefined') {
    parsedOutputFile.dir += ` ${filenamify(sheetName)}${filteredIndicator} ${dateTimeString}`
  }
  else {
    parsedOutputFile.dir += ` ${filteredIndicator} ${dateTimeString}`
  }
  fs.emptyDirSync(parsedOutputFile.dir)

  return parsedOutputFile
}
export function generateCommandLineString(combinedOptions: CombinedProgramOptions, command: Command & { _name?: string }): string {
  return objectEntries(combinedOptions).reduce((acc, [key, value]) => {
    const optionFlags = objectify([...command.options, ...(command.parent?.options ?? [])], o => o.attributeName() as StringKeyOf<CombinedProgramOptions>, o => o.long as string)

    if (key in optionFlags && command.getOptionValueSourceWithGlobals(key) !== 'implied' && command.getOptionValueSourceWithGlobals(key) !== 'default' && typeof value !== 'undefined') {
      if (isObject(value)) {
        if (!isEmptyObject(value)) {
          for (const [k, v] of objectEntries(value)) {
            if (isArray(v)) {
              for (const val of v) {
                const valueToString = stringifyCliValue(val, k)

                acc += ` \\\n${optionFlags[key]} ${stringifyValue(valueToString)} `
              }
            }
            else {
              const valueToString = stringifyCliValue(v, k)

              acc += ` \\\n${optionFlags[key]} ${stringifyValue(valueToString)} `
            }
          }
        }
      }
      else if (!isNull(value)) {
        acc += ` \\\n${optionFlags[key]} ${stringifyValue(value)} `
      }
    }

    return acc
  }, `${pkg.name} ${command._name!}`)
}
function stringifyCliValue(val: any, k: string): string {
  const valIsRegexp = val instanceof RegExp

  const keyIsNumber = isNumeric(`${k}`)

  let valueToString = ''

  if (!keyIsNumber)
    valueToString += `${k}:`

  valueToString += val

  if (valIsRegexp)
    valueToString += `:R`

  return valueToString
}
export function stringifyValue(val: any): any {
  const nonAlphaNumericPattern = createRegExp(anyOf(whitespace, linefeed, carriageReturn, '|', '\\', '/'))

  if (typeof val !== 'string')
    return val
  else if (nonAlphaNumericPattern.test(val))
    return `'${val}'`

  return val
}
export function isEmptyObject(obj: any): obj is EmptyObject {
  return isObject(obj) && Object.keys(obj).length === 0
}
export function formatHeaderValues(results: { data: JsonPrimitive[] }): string[] {
  return results.data.map((value, index, self) => {
    const occurrencesAfter = self.slice(index + 1).filter(v => v === value).length

    const occurrencesBefore = self.slice(0, index).filter(v => v === value).length + 1

    return (occurrencesAfter + occurrencesBefore) > 1 ? `${value}_${occurrencesBefore}` : `${value}`
  })
}
export function createCsvFileName(options: {
  parsedOutputFile: Omit<ParsedPath, 'base'>
  category: string | null | undefined
}, fileNumber: number | undefined): string {
  let csvFileName = options.parsedOutputFile.name

  if (typeof category !== 'undefined' && options.category !== null)
    csvFileName += ` ${options.category}`

  if (typeof fileNumber !== 'undefined')
    csvFileName += ` ${padStart(`${fileNumber}`, 4, '0')}`

  return filenamify(csvFileName, { replacement: '_' })
}
export function createHeaderFile(options: {
  parsedOutputFile: Omit<ParsedPath, 'base'>
  fields: string[]
  parsedLines: number
}, results: Papa.ParseStepResult<JsonPrimitive[]>): void {
  const headerFile = fs.createWriteStream(join(options.parsedOutputFile.dir, `${options.parsedOutputFile.name} HEADER.csv`), 'utf-8')

  options.fields = formatHeaderValues({ data: results.data })
  headerFile.end(Papa.unparse([options.fields]))
}
export function writeToActiveStream(PATH: string, csvOutput: string, options: { files: FileMetrics[] }): void {
  const currentFileIndex = findIndex(options.files, { PATH })

  options.files[currentFileIndex]!.BYTES += Buffer.from(csvOutput).length
  options.files[currentFileIndex]!.ROWS += 1
  options.files[currentFileIndex].stream!.write(`${csvOutput}\n`)
}

type PromptsType = ConditionalPick<typeof Prompts, (...args: any[]) => any>

type PromptKeys = StringKeyOf<PromptsType>

export async function tryPrompt<T extends PromptKeys, N extends number>(type: T, config: Parameters<PromptsType[T]>[0], timeout?: N): N extends undefined ? Promise<[undefined, AsyncReturnType<PromptsType[T]>]> : Promise<[Error, undefined] | [undefined, AsyncReturnType<PromptsType[T]>]> {
  return tryit(() => {
    if (typeof timeout === 'undefined') {
      return Prompts[type](config)
    }

    return Prompts[type](config, { signal: AbortSignal.timeout(timeout) })
  })()
}
export async function selectGroupingField(groupingOptions: (string | Prompts.Separator)[]): Promise<string | undefined> {
  const [, confirmCategory] = await tryit(Prompts.confirm)({
    message: 'Would you like to select a field to split the file into separate files?',
    default: false,
  }, { signal: AbortSignal.timeout(7500) })

  if (confirmCategory === true) {
    const [, selectedCategory] = await tryit((Prompts.select<string>))({
      message: `Select a column to group rows from input file by...`,
      choices: groupingOptions,
      loop: true,
      // pageSize: groupingOptions.length > 15 ? 15 : groupingOptions.length,
    })

    if (typeof selectedCategory === 'string' && selectedCategory.length) {
      // globalOptions.categoryField = selectedCategory
      return selectedCategory
    }
  }
}
export function applyFilters(options: {
  matchType: 'any' | 'all' | 'none'
  rowFilters: EmptyObject | Record<string, (RegExp | JsonPrimitive)[]>
}): (record: Array<JsonPrimitive> | Record<string, JsonPrimitive>) => boolean {
  return (record: Array<JsonPrimitive> | Record<string, JsonPrimitive>) => {
    const filterCriteria = options.rowFilters

    if (!('matchType' in options)) {
      return true
    }
    else if (isEmptyObject(filterCriteria)) {
      return true
    }
    else {
      const testResults: boolean[] = []

      for (const filterKey in filterCriteria) {
        const filterVal = get(filterCriteria, filterKey, [] as (RegExp | JsonPrimitive)[])

        const filterTest = filterVal.some((val) => {
          const recordFieldValue = get(record, filterKey)

          if (!has(record, filterKey)) {
            return false
          }

          else if (val instanceof RegExp) {
            return val.test(`${recordFieldValue}`)
          }
          else if (typeof val === 'boolean') {
            return val === !isNil(recordFieldValue)
          }
          else {
            return `${val}` === recordFieldValue
          }
        })

        testResults.push(filterTest)
      }
      if (options.matchType === 'all' && testResults.every(v => v === true)) {
        return true
      }
      else if (options.matchType === 'any' && testResults.includes(true)) {
        return true
      }
      else if (options.matchType === 'none' && testResults.every(v => v === false)) {
        return true
      }
      else {
        return false
      }
    }
  }
}
export function stringifyCommandOptions(options: CombinedProgramOptions, commandLineString: string): string {
  return yaml.stringify({
    'ALL OPTIONS': shake(options),
    'COMMAND': commandLineString,
  }, { lineWidth: 1000 })
}

// type Events = 'close' | 'drain' | 'error' | 'finish' | 'open' | 'pipe' | 'ready' | 'unpipe'
// interface Listeners<T> {
//   close?: (fn: () => void) => this
//   drain?: (fn: () => void) => this
//   error?: (fn: (err: Error) => void) => this
//   finish?: (fn: () => void) => this
//   open?: (fn: (fd: number) => void) => this
//   pipe?: (fn: (src: T) => void) => this
//   ready?: (fn: () => void) => this
//   unpipe?: (fn: (src: T) => void) => this
//   data?: (fn: (chunk: Buffer) => void) => this
//   cork?: (fn: () => void) => this
//   uncork?: (fn: () => void) => this
// }

interface Listeners {
  close?: () => void
  data?: (chunk: Buffer) => void
  drain?: () => void
  end?: () => void
  error?: (err: Error) => void
  finish?: () => void
  pause?: () => void
  pipe?: (src: Readable) => void
  readable?: () => void
  resume?: () => void
  unpipe?: (src: Readable) => void
}

export function streamToFile(inputStream: Stringifier, filePath: string, callbacks: {
  on?: Listeners
  once?: Listeners
}): fs.WriteStream {
  const fileWriteStream = fs.createWriteStream(filePath, 'utf-8')

  const pipeline = inputStream
    .pipe(fileWriteStream)

  if ('on' in callbacks) {
    for (const event in callbacks.on) {
      pipeline.on(event, callbacks.on[event])
    }
  }
  if ('once' in callbacks) {
    for (const event in callbacks.once) {
      pipeline.on(event, callbacks.once[event])
    }
  }

  return fileWriteStream
}
