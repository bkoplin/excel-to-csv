import { Option } from '@commander-js/extra-typings'

export default new Option(
  '-w, --write-header [boolean]',
  'enable/disable writing the CSV header to each file (if you select this to "true", the header will be written separately even if there is only one file)',
)
  .default(false)
  .preset(true)
