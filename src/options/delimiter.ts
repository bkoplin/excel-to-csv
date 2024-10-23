import { Option } from '@commander-js/extra-typings'

export default new Option('-d, --delimiter [string]', 'the CSV delimiter to use')
  .default(',')
