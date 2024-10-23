import { Option } from '@commander-js/extra-typings'

export default new Option('-i, --range-includes-header [true|false]', 'flag to indicate whether the range include the header row')
  .default(true)
  .preset(true)
