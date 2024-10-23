import { Option } from '@commander-js/extra-typings'
import { toFloat } from 'radash'

export default new Option(
  '--file-size [number]',
  'the maximum size of each file in MB (if not set, the files will not be split by size)',
)
  .argParser<number | undefined>((val): number | undefined => toFloat(val, undefined))
  .preset<undefined>(undefined)
  .default<undefined>(undefined)
