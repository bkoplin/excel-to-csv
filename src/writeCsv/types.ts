import type { JsonPrimitive } from 'type-fest'

export interface FileMetrics {
  BYTES: number
  FILENUM?: number
  ROWS: number
  PATH: string
  CATEGORY?: string
  FILTER?: Record<string, JsonPrimitive[]> | undefined
}
export interface SplitOptions {
  inputFilePath: string
  filterValues?: string[][]
  categoryField?: string
  maxFileSizeInMb?: number
  writeHeaderOnEachFile?: boolean
}
