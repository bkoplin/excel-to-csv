import { Option } from '@commander-js/extra-typings'
import chalk from 'chalk'

export default function (parserType: 'Excel' | 'CSV'): Option<'-p, --file-path [path]', 'Excel' | 'CSV', string, undefined, false, undefined> {
  return new Option('-p, --file-path [path]', `the full path to the ${chalk.yellowBright(parserType)} file`)
    .default('')
    .preset('')
}
