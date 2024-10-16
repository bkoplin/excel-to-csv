
import type {
  EmptyObject,
  JsonPrimitive,
} from 'type-fest'
import { Option } from '@commander-js/extra-typings'
import chalk from 'chalk'
import {
  isEmpty,
  isNaN,
  toNumber,
} from 'lodash-es'
import { toInt } from 'radash'

export function makeFilePathOption(parserType: 'Excel' | 'CSV'): Option<'-p, --file-path [path]', 'Excel' | 'CSV', string, undefined, false, undefined> {
  return new Option('-p, --file-path [path]', `the full path to the ${chalk.yellowBright(parserType)} file`)
    .default('')
    .preset('')
}

export const sheetNameOption = new Option('-n, --sheet [sheet name]', 'the sheet containing the data to parse to CSV').default(undefined)
.preset('')

export const sheetRangeOption = new Option('-r, --range [range]', 'the range of cells to parse in the Excel file').preset('')
.default(undefined)

export const includesHeaderOption = new Option('-i, --range-includes-header', 'flag to indicate whether the range include the header row').preset<boolean>(true)
.default(true)

export const skipLinesOption = new Option('-l, --skip-lines [number]', 'the number of rows to skip before reading the CSV file')
  .preset(-1)
  .argParser((val) => {
    const n = toInt(val, null)

    return n
  })

export const rowCountOption = new Option('-r, --row-count [number]', 'the number of rows to parse in the CSV file')
  .preset(-1)
  .argParser((val) => {
    const n = toInt(val, null)

    return n
  })

export const filterValuesOption = new Option(
  '-f, --row-filters [operators...]',
  `a header/value pair to apply as a filter to each row as ${`${chalk.cyan('[COLULMN NAME]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE]')}`}. \nTo apply multiple filters, input the option multiple times, e.g., \n${`${chalk.whiteBright('-f')} ${chalk.cyan('[COLULMN NAME 1]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 1]')} \n${chalk.whiteBright('-f')} ${chalk.cyan('[COLULMN NAME 1]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 2]')} \n${chalk.whiteBright('-f')} ${chalk.cyan('[COLULMN NAME 2]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 3]')}`}`,
)
  .implies({ matchType: `all` })
  .default<EmptyObject | undefined>(undefined)
  .preset<EmptyObject | undefined>(undefined)
  .argParser<Record<string, Array<JsonPrimitive>>>((val, filters = {}) => {
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
    }

    return filters
  })

export const categoryOption = new Option(
  '-c, --category-field [column name]',
  'the name of a column whose value will be used to create each separate file',
)
  .default<undefined | string>(undefined)
  .preset<undefined | string>(undefined)

export const maxFileSizeOption = new Option(
  '-s, --file-size [number]',
  'the maximum size of each file in MB (if not set, the files will not be split by size)',
).preset<undefined | string>(undefined)
.argParser((val): number | undefined => typeof val === 'undefined' || val === null || isNaN(toNumber(val)) ? undefined : toNumber(val))

export const filterTypeOption = new Option('-t, --match-type [choice]', 'the type of match to use when filtering rows')
  .choices([`all`, `any`, `none`] as const)
  // .default<`all`>(`all`)
  .preset<`all` | `any` | `none`>(`all`)

export const writeHeaderOption = new Option(
  '-h, --header [boolean]',
  'enable/disable writing the CSV header to each file (if you select this option, the header will be written separately even if there is only one file)',
)
  .default<undefined | boolean>(undefined)
  .argParser(val => val === 'true')
  .preset('true')

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
