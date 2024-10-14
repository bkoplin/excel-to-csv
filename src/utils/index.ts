import type {
  Readable,
  TransformCallback,
  TransformOptions,
  WritableOptions,
} from 'node:stream'
import {
  PassThrough,
  Transform,
  Writable,
} from 'node:stream'
import { EOL } from 'node:os'

export class LineBuffered extends Transform {
  private _buffer = ''
  constructor(options: TransformOptions = {}) {
    super({
      ...options,
      decodeStrings: false,
      encoding: 'utf-8',
    })
  }

  public override _transform(data: any, _: string, callback: TransformCallback): void {
    if (typeof data !== 'string') {
      callback(new Error('typeof data !== "string"'))
      return
    }
    const lines = data.split(EOL)
    if (lines.length > 0) {
      while (lines.length > 1) {
        const currentLine = `${this._buffer}${lines.shift()}${EOL}`
        this.push(currentLine)
        this._buffer = ''
      }
      this._buffer += lines.shift()
    }
    callback()
  }

  public override _flush(callback: TransformCallback) {
    this.push(this._buffer)
    callback()
  }
}

export class EolNormalizer extends Transform {
  constructor(options: TransformOptions = {}) {
    super({
      ...options,
      decodeStrings: false,
      encoding: 'utf-8',
    })
  }

  public override _transform(data: any, _: string, callback: TransformCallback): void {
    if (typeof data !== 'string') {
      callback(new Error('typeof data !== "string"'))
      return
    }
    callback(undefined, data.replace(/\r\n|\r|\n/g, EOL))
  }
}

export class RegExpMatcher extends Transform {
  constructor(private readonly _pattern: RegExp, options: TransformOptions = {}) {
    super({
      ...options,
      decodeStrings: false,
      readableObjectMode: true,
    })
  }

  public override _transform(data: any, _: string, callback: TransformCallback): void {
    if (typeof data !== 'string') {
      callback(new Error('typeof data !== "string"'))
      return
    }
    const decoded = this._pattern.exec(data)
    if (decoded !== null) {
      callback(undefined, decoded.input)
    }
    else {
      callback()
    }
  }
}

export class RegExpDecoder<T extends object> extends Transform {
  constructor(private readonly _pattern: RegExp, options: TransformOptions = {}) {
    super({
      ...options,
      decodeStrings: false,
      readableObjectMode: true,
    })
  }

  public override _transform(data: any, _: string, callback: TransformCallback): void {
    if (typeof data !== 'string') {
      callback(new Error('typeof data !== "string"'))
      return
    }
    const decoded = this._pattern.exec(data)
    if (decoded !== null) {
      if (decoded.groups !== undefined) {
        callback(undefined, decoded.groups as T)
      }
      else {
        callback(new Error('Line matched, but no named capture groups were found!'))
      }
    }
    else {
      callback()
    }
  }
}

export class MapStream<F, T> extends Transform {
  constructor(private readonly _mapFunction: (obj: F) => T, options: TransformOptions = {}) {
    super({
      ...options,
      objectMode: true,
    })
  }

  public override _transform(data: F, _: string, callback: TransformCallback): void {
    callback(undefined, this._mapFunction(data))
  }
}

export class FilterStream<T> extends Transform {
  constructor(private readonly _filterFunction: (obj: T) => boolean, options: TransformOptions = {}) {
    super({
      ...options,
      objectMode: true,
    })
  }

  public override _transform(data: T, _: string, callback: TransformCallback): void {
    if (this._filterFunction(data)) {
      callback(undefined, data)
    }
    else {
      callback()
    }
  }
}

export class CsvSink extends Transform {
  private _header: string | null = null
  constructor(private readonly _delim = ';', options: TransformOptions = {}) {
    super({
      ...options,
      writableObjectMode: true,
      encoding: 'utf-8',
    })
  }

  public override _transform(data: object, _: string, callback: TransformCallback): void {
    if (typeof data !== 'object') {
      callback(new Error('typeof data !== "object"'))
      return
    }
    const header = Object.keys(data).join(this._delim) + EOL
    if (this._header === null) {
      this._header = header
      this.push(header)
    }
    else {
      if (header !== this._header) {
        callback(new Error(`header mismatch: ${header} !== ${this._header}`))
        return
      }
    }
    const strings = Object.values(data).map(obj => (obj instanceof Date ? obj.toISOString() : obj))
    const csv = strings.join(this._delim) + EOL
    callback(undefined, csv)
  }
}

export class PlainSink extends Transform {
  constructor(options: TransformOptions = {}) {
    super({
      ...options,
      writableObjectMode: true,
      encoding: 'utf-8',
    })
  }

  public override _transform(data: string, _: string, callback: TransformCallback): void {
    if (typeof data !== 'string') {
      callback(new Error('typeof data !== "string"'))
      return
    }
    callback(undefined, data + EOL)
  }
}

export class JsonSink extends Transform {
  constructor(options: TransformOptions = {}) {
    super({
      ...options,
      writableObjectMode: true,
      encoding: 'utf-8',
    })
  }

  public override _transform(data: any, _: string, callback: TransformCallback): void {
    callback(undefined, JSON.stringify(data) + EOL)
  }
}

export class Collector<T> extends Writable {
  public readonly items: T[] = []

  constructor(opts?: WritableOptions) {
    super({
      ...opts,
      objectMode: true,
    })
  }

  public override _write(item: T, _: never, callback: (error?: Error | null) => void): void {
    this.items.push(item)
    callback(null)
  }
}

export function concat(...streams: Readable[]): Readable {
  const ostream = new PassThrough({ objectMode: streams[0]?.readableObjectMode })
  function chain([istream, ...rest]: Readable[]): void {
    const more = rest.length > 0
    istream
      .on('end', () => {
        if (more) {
          chain(rest)
        }
      })
      .on('error', (err) => {
        ostream.destroy(err)
      })
      .pipe(ostream, { end: !more })
  }
  chain(streams)
  return ostream
}
