import type { WriteStream } from 'node:fs'
import type { JsonPrimitive } from 'type-fest'

export interface FileMetrics {
  BYTES: number
  FILENUM?: number
  ROWS: number
  PATH: string
  CATEGORY?: string
  FILTER?: Record<string, JsonPrimitive[]> | undefined
  stream?: WriteStream
}
