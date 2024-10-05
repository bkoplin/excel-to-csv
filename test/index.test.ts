import { TestScheduler } from 'rxjs/testing'
import { expect, it } from 'vitest'
import { iif, of, switchMap } from 'rxjs'

const testScheduler = new TestScheduler((actual, expected) => {
  expect(actual).deep.equal(expected)
})
it('should receive inputPath once', () => {
  testScheduler.run((helpers) => {
    // const streamer = new SizeTrackingWritable({ filePath: '~/Desktop' })
    // streamer.inputPath$ = inputPath$
    const path = 'a'
    const e1subs = `${path}|`
    const obs = of(null).pipe(switchMap(value => iif(() => typeof value === 'undefined', of('b'), of('c'))))
    // streamer.setInputFile()
    helpers.expectObservable(obs).toBe(e1subs)
  })
})
