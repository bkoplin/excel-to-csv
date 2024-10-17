
declare module 'table-layout' {
  import type { Get, JsonObject, Paths } from 'type-fest'

  interface Padding {
    left?: string
    right?: string
  }

  interface ColumnMapper<T extends JsonObject = JsonObject, Name extends Paths<T> = Paths<T>> {
    /**
     * column name, must match a property name in the input
     */
    name: Name
    /**
     * A custom getter function for the column. The function receives the cell value and should return a string.
     * @default String(cellValue)
     */
    get?: <N extends this['name']>(cellValue: Get<T, N>) => string
    /**
     * A specific column width. Supply either this or a min and/or max width.
     */
    width?: number
    /**
     * A minimum column width. If the column content is smaller than this value, the column will be expanded.
     */
    minWidth?: number
    /**
     * A maximum column width. If the column content is larger than this value, the column will be compressed.
     */
    maxWidth?: number
    /**
     * disable wrapping on this columns
     * @default false
     */
    noWrap?: boolean
    /**
     * Enable breaking lines between words.
     * @default false
     */
    break?: boolean
    /**
     * Add strings to the `left` or `right` of the column.
     * @default { left: ' ', right: ' ' }
     */
    padding?: Padding
  }

  interface TableOptions<T> {
    /**
     * maximum width of layout in characters
     * @default 80
     */
    maxWidth?: number
    /**
     * disable wrapping on all columns
     * @default false
     */
    noWrap?: boolean
    /**
     * disable line-trimming on all columns
     * @default false
     */
    noTrim?: boolean
    /**
     * enable breaking lines between words on all columns
     * @default false
     */
    break?: boolean
    /**
     * ignore empty columns (columns containing only whitespace)
     * @default false
     */
    ignoreEmptyColumns?: boolean
    /**
     * Add strings to the `left` or `right` of the table.
     * @default { left: ' ', right: ' ' }
     */
    padding?: Padding
    /**
     * EOL character used
     * @default '\n'
     */
    eol?: string
    columns?: ColumnMapper<T>[]
  }

  export default class Table<T> {
    constructor(data: T[], options?: TableOptions<T>)
    toString(): string
  }
}
