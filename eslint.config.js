// @ts-check
import antfu from '@antfu/eslint-config'

export default antfu(
  {
    type: 'lib',
    rules: {
      'no-undef': 'error',
      'no-unused-vars': 'warn',
      'no-duplicate-imports': 'warn',
      'unused-imports/no-unused-imports': 'error',
      'style/newline-per-chained-call': ['error', { ignoreChainWithDepth: 2 }],
      // 'style/object-property-newline': 'error',
      'style/multiline-comment-style': ['error', 'bare-block'],
      'ts/no-use-before-define': 'error',
    },
    typescript: {
      tsconfigPath: 'tsconfig.json',
    },
  },
)
