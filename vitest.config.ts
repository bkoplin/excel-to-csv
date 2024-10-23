import { defineConfig } from 'vite'
import tsconfigPaths from 'vite-tsconfig-paths'

export default defineConfig({
  test: {
    update: true,
    updateSnapshots: true,
    includeSource: ['./src/**/*.ts'],
    include: ['./test/**/*.test.ts'],
  },
  plugins: [tsconfigPaths()],
})
