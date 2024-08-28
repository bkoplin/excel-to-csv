// @ts-check
import antfu from '@antfu/eslint-config'

export default antfu(
  {
    type: 'lib',
    rules: {
      'no-undef': 'error',
      'no-unused-vars': 'error',
      'no-use-before-define': 'error',
      'style/newline-per-chained-call': ['error', { ignoreChainWithDepth: 2 }],
    },
  },
)
