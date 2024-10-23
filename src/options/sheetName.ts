import { Option } from '@commander-js/extra-typings'

export default new Option('-n, --sheet [sheet name]', 'the sheet containing the data to parse to CSV').default(undefined)
.preset('')
