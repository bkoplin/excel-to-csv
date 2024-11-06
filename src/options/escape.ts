import { Option } from '@commander-js/extra-typings'

export default new Option<'--escape [string]', undefined, null, undefined, false>('--escape [string]', 'the escape character used in the source file')
  .default(null, 'the source file does not use an escape character')
