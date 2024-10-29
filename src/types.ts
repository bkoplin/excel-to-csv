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

export type CSVOptions =
  { [Prop in keyof ReturnType<CsvCommand['opts']>]: ReturnType<CsvCommand['opts']>[Prop] extends string | number ? Exclude<ReturnType<CsvCommand['opts']>[Prop], true> : ReturnType<CsvCommand['opts']>[Prop] }

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

export type CombinedProgramOptions = CSVOptionsWithGlobals | ExcelOptionsWithGlobals
