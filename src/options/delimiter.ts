import type { IterableElement } from 'type-fest'
import { Option } from '@commander-js/extra-typings'

const csvDelimiters = [`,`, `;`, `|`, `\t`] as const

export default new Option<'--delimiter [string]', undefined, `,`, undefined, false, IterableElement<typeof csvDelimiters>>('--delimiter [string]', 'the CSV delimiter to use')
  .default(',')
  .choices(csvDelimiters)
