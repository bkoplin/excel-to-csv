import { Option } from '@commander-js/extra-typings'
import { isEmpty } from 'lodash-es'

export default new Option(
  '--category-field [column name]',
  'the name of a column whose value will be used to create each separate file',
)
  .default<undefined>(undefined)
  .preset<undefined>(undefined)
  .argParser<string | undefined>((val): string | undefined => {
    if (isEmpty(val)) {
      return undefined
    }
    else {
      return val
    }
  })
