import { Option } from '@commander-js/extra-typings'
import { isEmpty } from 'lodash-es'

export default new Option(
  '--category-field [column name]',
  'the name of the column(s) whose value(s) will be used to split the data into separate files',
)
  .default<[]>('', 'records will not be split into separate files')
  .preset<[]>(undefined)
  .argParser<string[]>((val, previous = []): string[] => {
    if (!isEmpty(val)) {
      previous.push(val)
    }

    return previous
  })
