
import type { JsonPrimitive } from 'type-fest'
import {
  isEmpty,
  isNaN,
  toNumber,
} from 'lodash-es'

export function parseRowFilters(filters: string[]): Record<string, JsonPrimitive[]> {
  return filters.reduce((acc: Record<string, Array<JsonPrimitive>> = {}, filter): Record<string, Array<JsonPrimitive>> => {
    if (typeof filter !== 'undefined' && !isEmpty(filter)) {
      const [key, value] = (filter || '').split(':').map(v => v.trim())

      if (key.length) {
        if (!acc[key])
          acc[key] = []

        if (value.length) {
          if (!isNaN(toNumber(value))) {
            acc[key] = [...acc[key], toNumber(value)]
          }
          else if (value === 'true' || value === 'false') {
            acc[key] = [...acc[key], value === 'true']
          }
          else {
            acc[key] = [...acc[key], value]
          }
        }
        else {
          acc[key] = [...acc[key], true]
        }
      }
    }

    return acc
  }, {} as Record<string, Array<JsonPrimitive>>)
}
