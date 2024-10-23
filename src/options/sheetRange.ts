import { Option } from '@commander-js/extra-typings'

export default new Option('--sheet-range [range]', 'the range of cells to parse in the Excel file')
  .default(undefined)
