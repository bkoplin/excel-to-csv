export interface Letter {
  letter: string
  yPos: number
}
export interface Letters {
  ltrs: Letter[]
  intrvl: number
}
export interface State {
  score: number
  letters: Letter[]
  level: number
}
