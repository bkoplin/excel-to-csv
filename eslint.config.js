// @ts-check
import antfu from '@antfu/eslint-config'

export default antfu(
  {
    type: 'lib',
    rules: {
      'no-undef': 'error',
      'no-unused-vars': 'warn',
      'no-duplicate-imports': 'warn',
      'no-use-before-define': 'off',
      'unused-imports/no-unused-imports': 'error',
      'style/newline-per-chained-call': ['error', { ignoreChainWithDepth: 2 }],
      'style/object-property-newline': ['error', {
        allowAllPropertiesOnSameLine: false,
        allowMultiplePropertiesPerLine: false,
      }],
      'style/multiline-comment-style': ['error', 'separate-lines'],
      'node/prefer-global/process': ['error', 'always'],
      'style/object-curly-newline': ['error', {
        multiline: true,
        consistent: true,
      }],
    },
    typescript: {
      tsconfigPath: './tsconfig.json',
      overrides: {
        'no-use-before-define': 'error',
      },
    },
  },
)
