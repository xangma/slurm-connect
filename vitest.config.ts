import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['src/test/unit/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      include: [
        'src/connect/**/*.ts',
        'src/localProxy/**/*.ts',
        'src/state/**/*.ts',
        'src/utils/**/*.ts',
        'src/webview/**/*.ts'
      ],
      exclude: ['src/connect/types.ts', 'src/test/**'],
      thresholds: {
        lines: 50,
        functions: 65,
        branches: 55,
        statements: 50
      }
    }
  }
});
