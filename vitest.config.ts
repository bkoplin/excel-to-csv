import { defineConfig } from 'vite'

export default defineConfig({
  test: {
    update: true,
    includeSource: ['src/**/*.ts'],
    include: ['test/**/*.test.ts'],
  },
})
