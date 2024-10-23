import { Option } from '@commander-js/extra-typings'
import { toInt } from 'radash'

export default new Option('--row-count [number]', 'the number of rows to parse in the CSV file')
  .default(undefined)
  .argParser((val: `${number}` | string) => toInt(val, undefined))
