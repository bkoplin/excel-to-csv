import type { ParsedPath } from 'node:path'
import type XLSX from 'xlsx'
import {
  Argument,
  Option,
} from '@commander-js/extra-typings'
import chalk from 'chalk'
import {
  isEmpty,
  isNaN,
  toNumber,
} from 'lodash-es'
import type { JsonPrimitive } from 'type-fest'
import { checkAndResolveFilePath } from './helpers'

export interface Arguments<T extends boolean = false> {
  bytesWritten?: number
  columnIndices?: Generator<number>
  csvFileSize?: number
  csvSizeInMb?: number
  decodedRange?: XLSX.Range
  dirName?: string
  fileNum?: number
  filePath?: string
  isLastRow?: boolean
  outputFile?: ParsedPath
  outputFilePath?: string
  outputFileName?: string
  outputFileDir?: string
  outputFiles?: string[]
  parsedFile?: ParsedPath
  range?: string
  rangeIncludesHeader?: boolean
  rawSheet?: XLSX.WorkSheet
  rowCount?: number
  rowIndices?: Generator<number>
  sheetName?: string
  headerRow?: string[]
  Sheets?: { [sheet: string]: XLSX.WorkSheet }
  splitWorksheet?: T
}

export const filePathArgument = new Argument('[path]', 'the full path to the CSV file')
  .argParser(async (value: string | undefined) => await checkAndResolveFilePath('CSV', value))

export function makeFilePathOption(parserType: 'Excel' | 'CSV'): Option<'--file-path [path]', 'Excel' | 'CSV', string, undefined, false, undefined> {
  return new Option('--file-path [path]', `the full path to the ${chalk.yellowBright(parserType)} file`).default('')
    .preset('')
}
export const filterValuesOption = new Option(
  '--row-filters [operators...]',
  `one or more pairs of colum headers and values to apply as a filter to each row as ${`${chalk.cyan('[COLULMN NAME]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE]')}`}`,
)
  .implies({ matchType: `all` })
  .default(undefined)
  .preset({})
  .argParser((val, filters: Record<string, Array<JsonPrimitive>> = {}) => {
    if (typeof val !== 'undefined' && !isEmpty(val)) {
      const [key, value] = (val || '').split(':').map(v => v.trim())
      if (key.length) {
        if (!filters[key])
          filters[key] = []
        if (value.length) {
          if (!isNaN(toNumber(value))) {
            filters[key] = [...filters[key], toNumber(value)]
          }
          else if (value === 'true' || value === 'false') {
            filters[key] = [...filters[key], value === 'true']
          }
          else {
            filters[key] = [...filters[key], value]
          }
        }
        else {
          filters[key] = [...filters[key], true]
        }
      }
      return filters
    }
  })
export const categoryOption = new Option(
  '-c, --category-field [column title]',
  'the name of a column whose value will be used to create each separate file',
)
  .default(undefined as unknown as string | undefined)
  .preset('')
export const maxFileSizeOption = new Option(
  '--file-size [number]',
  'the maximum size of each file in MB (if not set, the files will not be split by size)',
).preset(0)
  .argParser((val): number | undefined => typeof val === 'undefined' || val === null || isNaN(toNumber(val)) ? undefined : toNumber(val))
export const filterTypeOption = new Option('--match-type', 'the type of match to use when filtering rows')
  .choices([`all`, `any`, `none`] as const)
  .default<`all`>(`all`)
  .preset<`all` | `any` | `none`>(`all`)

export const writeHeaderOption = new Option(
  '-h, --header [boolean]',
  'enable/disable writing the CSV header to each file (if you select this option, the header will be written separately even if there is only one file)',
).default(true)
  .argParser((val) => {
    if (val === 'false')
      return false
    else return true
  })
