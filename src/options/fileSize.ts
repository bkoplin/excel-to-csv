import { Option } from '@commander-js/extra-typings'
import {
  isNaN,
  toNumber,
} from 'lodash-es'

export default new Option(
  '-s, --file-size [number]',
  'the maximum size of each file in MB (if not set, the files will not be split by size)',
)
  .preset<undefined | string>(undefined)
  .argParser((val): number | undefined => typeof val === 'undefined' || val === null || isNaN(toNumber(val)) ? undefined : toNumber(val))
