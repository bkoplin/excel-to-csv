import type { IterableElement } from 'type-fest'
import { Option } from '@commander-js/extra-typings'

const csvDelimiters = [`,`, `;`, `|`, `tab`] as const

export default new Option<'--delimiter [string]', undefined, `,`, undefined, true, IterableElement<typeof csvDelimiters>>('--delimiter [string]', 'the CSV delimiter to use to parse the file')
  .default(',')
  .choices<typeof csvDelimiters>(csvDelimiters)
