import type { Command } from '@commander-js/extra-typings'
import type { ParsedPath } from 'node:path'
import type {
  EmptyObject,
  Merge,
  StringKeyOf,
} from 'type-fest'
import type {
  CombinedProgramOptions,
  CSVOptions,
  ExcelOptions,
  ProgramCommandOptions,
} from './index'
import { homedir } from 'node:os'
import { objectEntries } from '@antfu/utils'
import { Separator } from '@inquirer/core'
import { select } from '@inquirer/prompts'
import colors from 'chalk'
import dayjs from 'dayjs'
import fg from 'fast-glob'
import filenamify from 'filenamify'
import fs from 'fs-extra'
import inquirerFileSelector from 'inquirer-file-selector'
import {
  isArray,
  isNull,
  isObject,
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
import {
  basename,
  join,
  relative,
} from 'pathe'
import {
  isEmpty,
  objectify,
  omit,
} from 'radash'
import pkg from '../package.json'

/* async_RS reads a stream and returns a Promise resolving to a workbook */

export async function checkAndResolveFilePath(options: {
  fileType: 'Excel' | 'CSV'
  argFilePath: CombinedProgramOptions['filePath']
}): Promise<string> {
  let argFilePath = options.argFilePath

  if (typeof argFilePath === 'undefined' || isEmpty(argFilePath)) {
    ora().warn(colors.magentaBright(`You have not provided an input ${options.fileType} file.`))

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
      ora().warn(colors.magentaBright(`Could not find ${options.fileType === 'CSV' ? 'a CSV' : 'an Excel'} file at the path ${colors.cyanBright(`"${options.argFilePath}"`)}!`))

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
    fileExtString = colors.cyanBright('csv')
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
export function generateCommandLineString(combinedOptions: Merge<CSVOptions, ProgramCommandOptions> | Merge<ExcelOptions, ProgramCommandOptions>, command: Command & { _name?: string }): string {
  return objectEntries(combinedOptions).reduce((acc, [key, value]) => {
    const optionFlags = objectify([...command.options, ...(command.parent?.options ?? [])], o => o.attributeName() as StringKeyOf<CombinedProgramOptions>, o => o.long as string)

    if (key in optionFlags && command.getOptionValueSourceWithGlobals(key) !== 'implied' && command.getOptionValueSourceWithGlobals(key) !== 'default' && typeof value !== 'undefined') {
      if (isObject(value)) {
        if (!isEmptyObject(value)) {
          for (const [k, v] of objectEntries(value)) {
            if (isArray(v)) {
              for (const val of v) acc += ` \\\n${optionFlags[key]} ${stringifyValue(`${k}:${val}`)} `
            }
            else {
              acc += ` \\\n${optionFlags[key]} ${stringifyValue(`${k}:${v}`)} `
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
export function stringifyValue(val: any): any {
  const nonAlphaNumericPattern = createRegExp(anyOf(whitespace, linefeed, carriageReturn, '\\', '/'))

  if (typeof val !== 'string')
    return val
  else if (nonAlphaNumericPattern.test(val))
    return `'${val}'`

  return val
}
export function isEmptyObject(obj: any): obj is EmptyObject {
  return isObject(obj) && Object.keys(obj).length === 0
}
