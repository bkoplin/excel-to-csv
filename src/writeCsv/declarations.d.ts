
declare module 'table-layout' {
  import type {
    Get,
    JsonObject,
    Paths,
  } from 'type-fest'

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

declare module 'inquirer-file-selector' {
  import type { Theme } from '@inquirer/core'
  import type * as _inquirer_type from '@inquirer/type'
  import type { PartialDeep } from '@inquirer/type'

  interface FileSelectorTheme {
    icon: {
      /**
       * The prefix to use for the line.
       * @default isLast => isLast ? └── : ├──
       */
      linePrefix: (isLast: boolean) => string
    }
    style: {
      /**
       * The style to use for the disabled items.
       * @default chalk.dim
       */
      disabled: (text: string) => string
      /**
       * The style to use for the active item.
       * @default chalk.cyan
       */
      active: (text: string) => string
      /**
       * The style to use for the cancel text.
       * @default chalk.red
       */
      cancelText: (text: string) => string
      /**
       * Alias for `emptyText`.
       * @deprecated Use `emptyText` instead. Will be removed in the next major version.
       */
      noFilesFound?: (text: string) => string
      /**
       * The style to use for the empty text.
       * @default chalk.red
       */
      emptyText: (text: string) => string
      /**
       * The style to use for items of type directory.
       * @default chalk.yellow
       */
      directory: (text: string) => string
      /**
       * The style to use for items of type file.
       * @default chalk.white
       */
      file: (text: string) => string
      /**
       * The style to use for the current directory header.
       * @default chalk.magenta
       */
      currentDir: (text: string) => string
      /**
       * The style to use for the key bindings help.
       * @default chalk.white
       */
      help: (text: string) => string
      /**
       * The style to use for the keys in the key bindings help.
       * @default chalk.cyan
       */
      key: (text: string) => string
    }
  }

  interface Item {
    /**
     * The name of the item.
     */
    name: string
    /**
     * The path to the item.
     */
    path: string
    /**
     * If the item is a directory.
     */
    isDir: boolean
    /**
     * If the item is disabled, it will be displayed in the list with the `disabledLabel` property.
     *
     * Set to `true` if the `match` function returns `false`.
     */
    isDisabled?: boolean
  }

  interface FileSelectorConfig {
    message: string
    /**
     * Alias for `basePath`.
     * @deprecated Use `basePath` instead. Will be removed in the next major version.
     */
    path?: string
    /**
     * The path to the directory where it will be started.
     * @default process.cwd()
     */
    basePath?: string
    /**
     * The maximum number of items to display in the list.
     * @default 10
     */
    pageSize?: number
    /**
     * The function to use to filter the files. Returns `true` to include the file in the list.
     *
     * If not provided, all files will be included.
     */
    filter?: (file: Item) => boolean
    /**
     * The function to use to filter the files. Returns `true` to include the file in the list.
     *
     * If not provided, all files will be included.
     */
    match?: (file: Item) => boolean
    /**
     * If true, the list will be filtered to show only files that match the `match` function.
     * @default false
     */
    hideNonMatch?: boolean
    /**
     * If true, the list will be filtered to show only files that match the `match` function.
     * @default true
     */
    showExcluded?: boolean
    /**
     * The label to display when a file is disabled.
     * @default ' (not allowed)'
     */
    disabledLabel?: string
    /**
     * If true, the prompt will allow the user to cancel the selection.
     * @default false
     */
    allowCancel?: boolean
    /**
     * Alias for `cancelText`.
     * @deprecated Use `cancelText` instead. Will be removed in the next major version.
     */
    canceledLabel?: string
    /**
     * The message to display when the user cancels the selection.
     * @default 'Canceled.'
     */
    cancelText?: string
    /**
     * Alias for `emptyText`.
     * @deprecated Use `emptyText` instead. Will be removed in the next major version.
     */
    noFilesFound?: string
    /**
     * The message that will be displayed when the directory is empty.
     * @default 'Directory is empty.'
     */
    emptyText?: string
    /**
     * The theme to use for the file selector.
     */
    theme?: PartialDeep<Theme<FileSelectorTheme>>
  }

  declare const _default: _inquirer_type.Prompt<string, FileSelectorConfig>

  export { _default as default }

}
