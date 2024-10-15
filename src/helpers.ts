import { homedir } from 'node:os'
import type { ParsedPath } from 'node:path'
import fs from 'fs-extra'
import {
  basename,
  join,
  relative,
  sep,
} from 'pathe'
import fg from 'fast-glob'
import inquirerFileSelector from 'inquirer-file-selector'
import colors from 'chalk'
import { select } from '@inquirer/prompts'
import { Separator } from '@inquirer/core'
import ora from 'ora'
import {
  createRegExp,
  exactly,
  maybe,
} from 'magic-regexp'
import type { JsonPrimitive } from 'type-fest'
import {
  isEmpty,
  omit,
} from 'radash'
import dayjs from 'dayjs'

/* async_RS reads a stream and returns a Promise resolving to a workbook */

export async function checkAndResolveFilePath(fileType: 'Excel' | 'CSV', argFilePath: string | undefined): Promise<string> {
  if (typeof argFilePath === 'undefined' || !fs.existsSync(argFilePath)) {
    ora().warn(colors.yellowBright(`No ${colors.yellowBright(fileType)} exists at path ${colors.cyanBright(`"${argFilePath}"`)}!`))
    const startingFolder = await selectStartingFolder(fileType)
    const selectedFile = await selectFile(fileType, startingFolder)
    return selectedFile
  }
  return Promise.resolve(argFilePath)
}

export function selectFile(fileType: 'Excel' | 'CSV', basePath: string): Promise<string> {
  const fileExtString = fileType === 'Excel' ? `${colors.cyanBright('.xls')} or ${colors.cyanBright('.xlsx')}` : colors.cyanBright('csv')
  const pathRegexp = fileType === 'Excel'
    ? createRegExp(exactly('.'), exactly('xlsx').before(maybe('x')).at.lineEnd(), ['i'])
    : createRegExp(exactly('.'), exactly('csv').or('txt')
      .or('tsv').at.lineEnd(), ['i'])
  return inquirerFileSelector({
    message: `Navigate to the ${colors.yellowBright(fileType)} file you want to parse (only files with an ${fileExtString} extension will be shown, and the file names must start with an alphanumeric character)`,
    basePath,
    hideNonMatch: true,
    allowCancel: true,
    pageSize: 20,
    theme: { style: { currentDir: (text: string) => colors.magentaBright(join(`.`, basename(basePath), relative(basePath, text))) } },
    match(filePath) {
      if (filePath.isDir) {
        return !filePath.path.split(sep).some(v => /^[^A-Z0-9]/i.test(v))
      }

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
  const homeFolders = fg.sync(['Desktop', 'Documents', 'Downloads'], {
    onlyDirectories: true,
    absolute: true,
    cwd: homedir(),
    deep: 1,
  }).map(folder => ({
    name: basename(folder),
    value: folder,
  }))
  return select({
    message: `Where do you want to start looking for your ${colors.yellowBright(fileType)} file?`,
    pageSize: 20,
    choices: [new Separator('----HOME----'), ...homeFolders, new Separator('----ONEDRIVE----'), ...cloudFolders],
  })
}

export function generateParsedCsvFilePath(parsedInputFile: ParsedPath, filters: Record<string, JsonPrimitive[]>): Omit<ParsedPath, 'base'> {
  const parsedOutputFile = omit(parsedInputFile, ['base'])
  parsedOutputFile.ext = '.csv'
  parsedOutputFile.dir = join(parsedOutputFile.dir, `${parsedInputFile.name} PARSE JOBS`, dayjs().format('YYYY-MM-DD HH-mm') + (!isEmpty(filters) ? ' FILTERED' : ''))
  // parsedOutputFile.name = filters.length ? `${parsedInputFile.name} FILTERED` : parsedInputFile.name
  fs.emptyDirSync(parsedOutputFile.dir)
  return parsedOutputFile
}
