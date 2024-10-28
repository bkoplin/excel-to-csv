import type { WriteStream } from 'node:fs'
import type { ParsedPath } from 'pathe'
import type {
  JsonPrimitive,
  Simplify,
  TaggedUnion,
} from 'type-fest'
import type {
  _csvCommands,
  program,
} from '.'
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

type CsvCommand = typeof _csvCommands

type ExcelCommand = typeof excelCommamd

type ProgramCommand = typeof program

export type CSVOptions = ReturnType<CsvCommand['opts']>

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

export type CombinedProgramOptions = TaggedUnion<'command', {
  CSV: CSVOptionsWithGlobals
  Excel: ExcelOptionsWithGlobals
}>
