import { Option } from '@commander-js/extra-typings'
import { toInt } from 'radash'

export default new Option('--from-line [number]', 'the number of rows to skip before reading the CSV file')
  .default(1 as const)
  .argParser((val: `${number}` | string) => toInt(val, 1 as const))
