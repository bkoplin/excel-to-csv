// @ts-check
import antfu from '@antfu/eslint-config'

export default antfu(
  {
    type: 'lib',
    rules: {
      'no-undef': 'error',
      'no-unused-vars': 'warn',
      'no-duplicate-imports': 'off',
      'no-use-before-define': 'off',
      'no-cond-assign': 'off',
      'no-console': 'warn',
      'unused-imports/no-unused-imports': 'error',
      'style/newline-per-chained-call': ['error', { ignoreChainWithDepth: 2 }],
      'style/object-property-newline': ['error', {
        allowAllPropertiesOnSameLine: false,
        allowMultiplePropertiesPerLine: false,
      }],
      'style/multiline-comment-style': ['error', 'separate-lines'],
      'style/no-multiple-empty-lines': ['error', {
        max: 1,
        maxEOF: 0,
      }],
      'style/dot-location': ['error', 'property'],
      'style/multiline-ternary': ['error', 'always-multiline'],
      'style/padding-line-between-statements': ['error', {
        blankLine: 'always',
        prev: '*',
        next: '*',
      }, {
        blankLine: 'never',
        prev: 'import',
        next: 'import',
      }, {
        blankLine: 'never',
        prev: ['block', 'expression', 'block-like', 'multiline-block-like'],
        next: ['block', 'expression', 'block-like', 'multiline-block-like'],
      }],
      'style/function-paren-newline': ['error', 'consistent'],
      'node/prefer-global/process': ['error', 'always'],
      'style/object-curly-newline': ['error', {
        multiline: true,
        // consistent: true,
        minProperties: 2,
      }],
      'node/prefer-global/buffer': ['error', 'always'],
      'unicorn/prefer-modern-math-apis': 'error',
      'unicorn/no-lonely-if': 'error',
      'unicorn/no-array-for-each': 'error',
    },
    typescript: {
      parserOptions: {
        projectService: {
          allowDefaultProject: ['src/*.d.ts'],
          defaultProject: 'tsconfig.json',
        },
      },
      overrides: {
        'no-use-before-define': 'error',
        'import/default': 'warn',
        'unused-imports/no-unused-imports': 'error',
        'antfu/import-dedupe': 'error',
        'import/no-duplicates': 'error',
      },
    },
  },
)
