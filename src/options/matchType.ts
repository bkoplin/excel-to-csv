import { Option } from '@commander-js/extra-typings'

export default new Option('--match-type [choice]', 'the type of match to use when filtering rows')
  .choices([`all`, `any`, `none`] as const)
  .preset(`all` as const)
  .default(`all` as const)
