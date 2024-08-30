import { defineBuildConfig } from 'unbuild'

export default defineBuildConfig({
  entries: [
    'src/index',
    'src/cli',
  ],
  declaration: true,
  clean: true,
  failOnWarn: false,
  rollup: {
    dts: {
      compilerOptions: {
        noEmitOnError: false,
      },
    },
    emitCJS: true,
    cjsBridge: true,
  },
})
