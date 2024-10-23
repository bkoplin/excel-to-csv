import { Option } from '@commander-js/extra-typings'
import { toInt } from 'radash'

export default new Option('-r, --to-line [number]', 'the number of rows to parse in the CSV file')
  .default(undefined)
  .argParser((val: `${number}` | string) => toInt(val))
