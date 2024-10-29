import type { WriteStream } from 'node:fs'
import type { ParsedPath } from 'pathe'
import type {
  JsonPrimitive,
  Simplify,
} from 'type-fest'
import type { program } from '.'
import type { csvCommand } from './subcommands/csvCommand'
import type { excelCommamd } from './subcommands/excelCommand'

export interface FileMetrics {
  BYTES: number
  FILENUM?: number
  ROWS: number
  PATH: string
  CATEGORY?: string
  FILTER?: Record<string, (RegExp | JsonPrimitive)[]> | undefined
  stream?: WriteStream
}

type CsvCommand = typeof csvCommand

type ExcelCommand = typeof excelCommamd

type ProgramCommand = typeof program

type CSVCommandOpts = ReturnType<CsvCommand['opts']>

export type CSVOptions =
  { [Prop in keyof CSVCommandOpts as `${Prop}`]: {
    1: Exclude<CSVCommandOpts[Prop], boolean>
    0: CSVCommandOpts[Prop]
  }[CSVCommandOpts[Prop] extends string | number ? 1 : 0] }

export type ExcelOptions = ReturnType<ExcelCommand['opts']>

export type CSVOptionsWithGlobals = Simplify<CSVOptions & {
  skippedLines: number
  rowCount: number
  parsedOutputFile: Omit<ParsedPath, 'base'>
  bytesRead: number
}>

export type ExcelOptionsWithGlobals = Simplify<ExcelOptions & {
  parsedOutputFile: Omit<ParsedPath, 'base'>
  bytesRead: number
}>

export type CombinedProgramOptions = CSVOptions | ExcelOptionsWithGlobals
