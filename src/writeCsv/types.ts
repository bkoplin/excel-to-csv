
export interface FileMetrics {
  BYTES: number
  FILENUM?: number
  ROWS: number
  PATH: string
  CATEGORY?: string
}
export interface SplitOptions {
  inputFilePath: string
  filterValues?: string[][]
  categoryField?: string
  maxFileSizeInMb?: number
  writeHeaderOnEachFile?: boolean
}
