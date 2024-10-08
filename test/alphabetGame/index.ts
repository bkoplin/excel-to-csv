import ora from 'ora'
// RxJS v6+
import { BehaviorSubject, combineLatest, interval, of } from 'rxjs'
import { delay, scan, switchMap, takeWhile } from 'rxjs/operators'
import type { Letters, State } from '../alphabetGame/interfaces'

function randomLetter(): string {
  return String.fromCharCode(
    Math.random() * ('z'.charCodeAt(0) - 'a'.charCodeAt(0)) + 'a'.charCodeAt(0),
  )
}
const levelChangeThreshold = 20
const speedAdjust = 50
const endThreshold = 15
const gameWidth = 30

const intervalSubject = new BehaviorSubject(600)

const letters$ = intervalSubject.pipe(
  switchMap(i =>
    interval(i).pipe(
      scan<number, Letters>
      (letters => ({
        intrvl: i,
        ltrs: [
          {
            letter: randomLetter(),
            yPos: Math.floor(Math.random() * gameWidth),
          },
          ...letters.ltrs,
        ],
      }), {
        ltrs: [],
        intrvl: 0,
      }),
    ),
  ),
)

const keys$ = of('a', 'b', 'c', 'd', 'e', 'f', 'g').pipe(
  delay(1000),
)
const spinner = ora().start()

function renderGame(state: State) {
  return (spinner.text = `Score: ${state.score}, Level: ${state.level} \n`),
  state.letters.forEach(
    l =>
      (spinner.text += `${' '.repeat(l.yPos) + l.letter}\n`),
  ),
  (spinner.text
        += '\n'.repeat(endThreshold - state.letters.length - 1)
        + '-'.repeat(gameWidth))
}
function renderGameOver() {
  spinner.text += '\nGAME OVER!'
  spinner.succeed()
}
function noop() { }

const game$ = combineLatest([keys$, letters$]).pipe(
  scan<[string, Letters], State>
  ((state, [key, letters]) => (
    letters.ltrs[letters.ltrs.length - 1]
    && letters.ltrs[letters.ltrs.length - 1].letter === key
      ? ((state.score = state.score + 1), letters.ltrs.pop())
      : noop,
    state.score > 0 && state.score % levelChangeThreshold === 0
      ? ((letters.ltrs = []),
        (state.level = state.level + 1),
        (state.score = state.score + 1),
        intervalSubject.next(letters.intrvl - speedAdjust))
      : noop,
    {
      score: state.score,
      letters: letters.ltrs,
      level: state.level,
    }
  ), {
    score: 0,
    letters: [],
    level: 1,
  }),
  takeWhile(state => state.letters.length < endThreshold),
)

game$.subscribe(renderGame, noop, renderGameOver)
