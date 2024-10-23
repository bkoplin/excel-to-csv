import { Option } from '@commander-js/extra-typings'
import { toInt } from 'radash'

export default new Option('-l, --from-line [number]', 'the number of rows to skip before reading the CSV file')
  .preset(1 as const)
  .default(1 as const)
  .argParser((val: `${number}` | string) => toInt(val, 1 as const))
