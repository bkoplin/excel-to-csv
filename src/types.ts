import type {
  CastingContext,
  Info,
} from 'csv-parse'
import type { WriteStream } from 'node:fs'
import type { ParsedPath } from 'node:path'
import type {
  IfNever,
  JsonPrimitive,
  Simplify,
  UnionToIntersection,
} from 'type-fest'
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

export interface CsvDataPayload {
  record: Record<string, JsonPrimitive>
  info: Info & CastingContext
  raw: string
}

export type CSVOptions =
  { [Prop in keyof ReturnType<CsvCommand['opts']> as `${Prop}`]: {
    1: IfNever<Exclude<ReturnType<CsvCommand['opts']>[Prop], boolean>, boolean, Exclude<ReturnType<CsvCommand['opts']>[Prop], boolean>>
    0: ReturnType<CsvCommand['opts']>[Prop]
  }[UnionToIntersection<ReturnType<CsvCommand['opts']>[Prop]> extends boolean ? 1 : 0] }

export type ExcelOptions = { [Prop in keyof ReturnType<ExcelCommand['opts']> as `${Prop}`]: {
  1: IfNever<Exclude<ReturnType<ExcelCommand['opts']>[Prop], boolean>, boolean, Exclude<ReturnType<ExcelCommand['opts']>[Prop], boolean>>
  0: ReturnType<ExcelCommand['opts']>[Prop]
}[UnionToIntersection<ReturnType<ExcelCommand['opts']>[Prop]> extends true ? 1 : 0] }

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
