import { Option } from '@commander-js/extra-typings'

export default new Option('-r, --range [range]', 'the range of cells to parse in the Excel file').preset('')
.default(undefined)
