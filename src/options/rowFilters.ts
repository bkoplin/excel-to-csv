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

export default new Option<'--row-filters [operators...]', undefined, EmptyObject, Record<string, (JsonPrimitive | RegExp)[]>>(
  '--row-filters [operators...]',
`a header/value pair to apply as a filter to each row as ${`${chalk.cyan('[COLULMN NAME|INDEX]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE]')}${chalk.magentaBright('[:R]')}`}. \nThe ${chalk.magentaBright('[:R]')} is an optional addition that indicates that the filter is a regular expression. If not included, ${chalk.cyan('[COLULMN NAME|INDEX]')} must match ${chalk.yellow('[FILTER VALUE]')} exactly. \nTo apply multiple filters, input the option multiple times, e.g., \n${`${chalk.whiteBright('--row-filters')} ${chalk.cyan('[COLULMN NAME|INDEX 1]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 1]')} \n${chalk.whiteBright('--row-filters')} ${chalk.cyan('[COLULMN NAME|INDEX 1]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 2]')}${chalk.magentaBright(':R')} \n${chalk.whiteBright('--row-filters')} ${chalk.cyan('[COLULMN NAME|INDEX 2]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 3]')}`}`,
)
  .implies({ matchType: `all` })
  .preset(undefined as unknown as EmptyObject)
  .default(undefined as unknown as EmptyObject)
  .argParser<Record<string, (JsonPrimitive | RegExp)[]>>((val, filters = {}): Record<string, (JsonPrimitive | RegExp)[]> => processRowFilter(val, filters))

export function processRowFilter(val: string | `${string}:${string}` | `${string}:${string}:R`, filters: Record<string, (JsonPrimitive | RegExp)[]>): Record<string, (JsonPrimitive | RegExp)[]> {
  if (typeof val !== 'undefined' && !isEmpty(val)) {
    const [key, value, regexp] = (val || '').split(':').map(v => v.trim()) as [string] | [string, string] | [string, string, `R`]

    if (key.length) {
      if (!filters[key])
        filters[key] = []

      if (typeof value === 'string' && value.length) {
        if (regexp === 'R') {
          filters[key] = [...filters[key], new RegExp(value)]
        }
        else if (!isNaN(toNumber(value))) {
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

  return filters as Record<string, (JsonPrimitive | RegExp)[]>
}
