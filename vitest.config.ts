import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['src/test/unit/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      include: ['src/utils/**/*.ts'],
      exclude: ['src/test/**'],
      thresholds: {
        lines: 80,
        functions: 90,
        branches: 60,
        statements: 80
      }
    }
  }
});
