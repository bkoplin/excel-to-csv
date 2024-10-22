
import type { JsonPrimitive } from 'type-fest'
import { Option } from '@commander-js/extra-typings'
import chalk from 'chalk'
import {
  has,
  isEmpty,
  isNaN,
  toNumber,
} from 'lodash-es'
import {
  set,
  toInt,
} from 'radash'

export function makeFilePathOption(parserType: 'Excel' | 'CSV'): Option<'-p, --file-path [path]', undefined, string, undefined, false, undefined> {
  return new Option('-p, --file-path [path]', `the full path to the ${chalk.yellowBright(parserType)} file`)
    .default('')
    .preset(undefined)
}

export const sheetNameOption = new Option('-n, --sheet [sheet name]', 'the sheet containing the data to parse to CSV').default(undefined)
.preset(undefined)

export const sheetRangeOption = new Option('-r, --range [range]', 'the range of cells to parse in the Excel file').preset(undefined)
.default(undefined)

export const includesHeaderOption = new Option('-i, --range-includes-header [true|false]', 'flag to indicate whether the range include the header row')
  .preset(true)
  .default(true, 'the input range includes a header row')

export const skipLinesOption = new Option('-l, --skip-lines [number]', 'the number of rows to skip before reading the CSV file')
  .preset(-1 as const)
  .default(-1 as const, 'skip -1 lines (i.e., skip no lines)')
  .argParser((val: `${number}` | string): number | -1 => toInt(val, -1 as const))

export const rowCountOption = new Option('-r, --row-count [number]', 'the number of rows to parse in the CSV file')
  .preset(-1 as const)
  .default(-1 as const, 'parse -1 lines (i.e., parse all lines)')
  .argParser((val: `${number}` | string): number | -1 => toInt(val, -1 as const))

export const filterValuesOption = new Option(
  '-f, --row-filters [operators...]',
  `a header/value pair to apply as a filter to each row as ${`${chalk.cyan('[COLULMN NAME]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE]')}`}. \nTo apply multiple filters, input the option multiple times, e.g., \n${`${chalk.whiteBright('-f')} ${chalk.cyan('[COLULMN NAME 1]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 1]')} \n${chalk.whiteBright('-f')} ${chalk.cyan('[COLULMN NAME 1]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 2]')} \n${chalk.whiteBright('-f')} ${chalk.cyan('[COLULMN NAME 2]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 3]')}`}`,
)
  .implies({ matchType: `all` })
  .preset('' as '' | `${string}:${string}`)
  .default('' as '' | `${string}:${string}`)
  .argParser((val: '' | `${string}:${string}` | string, filters: Record<string, JsonPrimitive[]> = {}) => {
    if (typeof val !== 'undefined' && val.includes(':')) {
      const [key, value] = (val || '').split(':').map(v => v.trim())

      if (!isEmpty(key)) {
        if (!has(filters, key))
          filters = set(filters, key, [])

        if (!isEmpty(value)) {
          if (!isNaN(toNumber(value))) {
            filters[key] = [...filters[key], toNumber(value)]
          }
          else if (['true', 'false', 't', 'f'].includes(value.toLowerCase())) {
            filters[key] = [...filters[key], ['true', 't'].includes(value.toLowerCase())]
          }
          else {
            filters[key] = [...filters[key], value]
          }
        }
        else {
          filters[key] = [...filters[key], true]
        }
      }
    }

    return filters
  })

export const categoryOption = new Option(
  '-g, --group-by-category [column name]',
  'the name of a column whose value will be used to create each separate file',
)
  .default<undefined | string>(undefined)
  .preset<undefined | string>(undefined)

export const maxFileSizeOption = new Option(
  '-m, --max-file-size [number]',
  'the maximum size of each file in MB (if not set, the files will not be split by size)',
)
  .preset(undefined)
  .default(0 as const, 'max file size is (0), so unlimited')
  .argParser<number | 0>((val: `${number}` | string) => toInt(val, 0 as const))

export const filterTypeOption = new Option('-t, --filter-type [choice]', 'the type of match to use when filtering rows')
  .choices([`all`, `any`, `none`] as const)
  .default<`all`>(`all`, 'each row against which the filter is compared must satisfy all filter tests')
  .preset<`all` | `any` | `none`>(`all`)

export const writeHeaderOption = new Option(
  '-w, --write-header [true|false]',
  'enable/disable writing the CSV header to each file (if you select this option, the header will be written separately even if there is only one file)',
)
  .preset(true)
  .default(false, 'do not write the header to each file')

export function parseRowFilters(filters: string[]): Record<string, JsonPrimitive[]> {
  return filters.reduce((acc: Record<string, Array<JsonPrimitive>> = {}, filter): Record<string, Array<JsonPrimitive>> => {
    if (typeof filter !== 'undefined' && !isEmpty(filter)) {
      const [key, value] = (filter || '').split(':').map(v => v.trim())

      if (key.length) {
        if (!acc[key])
          acc[key] = []

        if (value.length) {
          if (!isNaN(toNumber(value))) {
            acc[key] = [...acc[key], toNumber(value)]
          }
          else if (value === 'true' || value === 'false') {
            acc[key] = [...acc[key], value === 'true']
          }
          else {
            acc[key] = [...acc[key], value]
          }
        }
        else {
          acc[key] = [...acc[key], true]
        }
      }
    }

    return acc
  }, {} as Record<string, Array<JsonPrimitive>>)
}
