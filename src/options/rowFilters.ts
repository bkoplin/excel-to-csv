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

export default new Option(
  '--row-filters [operators...]',
  `a header/value pair to apply as a filter to each row as ${`${chalk.cyan('[COLULMN NAME]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE]')}`}. \nTo apply multiple filters, input the option multiple times, e.g., \n${`${chalk.whiteBright('-f')} ${chalk.cyan('[COLULMN NAME 1]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 1]')} \n${chalk.whiteBright('-f')} ${chalk.cyan('[COLULMN NAME 1]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 2]')} \n${chalk.whiteBright('-f')} ${chalk.cyan('[COLULMN NAME 2]')}${chalk.whiteBright(':')}${chalk.yellow('[FILTER VALUE 3]')}`}`,
)
  .implies({ matchType: `all` })
  .preset(undefined as unknown as EmptyObject)
  .default(undefined as unknown as EmptyObject)
  .argParser<Record<string, Array<JsonPrimitive>>>((val, filters = {}) => {
    return processRowFilter(val, filters)
  })

export function processRowFilter(val: string, filters: Record<string, JsonPrimitive[]>): Record<string, JsonPrimitive[]> {
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
}
