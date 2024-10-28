import { Option } from '@commander-js/extra-typings'
import isBoolean from 'validator/lib/isBoolean'
import toBoolean from 'validator/lib/toBoolean'

export default new Option('-i, --range-includes-header [boolean]', 'flag to indicate whether the range include the header row')
  .default(true, 'the range includes the header row')
  .preset(true)
  .argParser((val: string): boolean => {
    if (typeof val === 'boolean')
      return val
    else return isBoolean(val, { loose: true }) ? toBoolean(val) : false
  })
