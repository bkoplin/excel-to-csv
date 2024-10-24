import { Option } from '@commander-js/extra-typings'
import isBoolean from 'validator/lib/isBoolean'
import toBoolean from 'validator/lib/toBoolean'

export default new Option(
  '-w, --write-header [boolean]',
  'enable/disable writing the CSV header to each file (if you select this to "true", the header will be written separately even if there is only one file)',
)
  .default(false)
  .preset(true)
  .argParser((val: string): boolean => isBoolean(val, { loose: true }) ? toBoolean(val) : false)
