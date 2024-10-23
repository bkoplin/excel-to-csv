import { Option } from '@commander-js/extra-typings'

export default new Option('--sheet-name [sheet name]', 'the sheet containing the data to parse to CSV').default<string>('').preset<undefined>(undefined)
