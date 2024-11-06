import { Option } from '@commander-js/extra-typings'

export default new Option<'--quote [string]', undefined, null, undefined, false>('--quote [string]', 'the quote string used in the source file')
  .default(null, 'the source file does not use quotes')
