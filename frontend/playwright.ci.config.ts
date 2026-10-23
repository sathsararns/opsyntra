import { defineConfig } from "@playwright/test";
import path from "node:path";

const apiPort = process.env.OPSYNTRA_TEST_API_PORT || "8011";
const webPort = process.env.OPSYNTRA_TEST_WEB_PORT || "3011";

export default defineConfig({
  testDir: "./tests",
  testMatch: ["**/ci/*.ci.ts", "**/analytics.spec.ts"],
  // The real Kafka/model/database event test runs in full-data.yml, not this fixture job.
  grepInvert: /a real producer event appears automatically/,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 45_000,
  reporter: [["list"], ["json", { outputFile: "test-results/ci-browser.json" }],
    ["junit", { outputFile: "test-results/ci-browser.xml" }], ["html", { open: "never" }]],
  use: {
    baseURL: `http://127.0.0.1:${webPort}`,
    channel: process.platform === "win32" ? "msedge" : undefined,
    headless: true,
    viewport: { width: 1440, height: 1050 },
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  webServer: [
    {
      command: `python -m pipelines.ci_api --fixture artifacts/ci/fixture --port ${apiPort}`,
      cwd: path.resolve(process.cwd(), "../backend"),
      url: `http://127.0.0.1:${apiPort}/api/health`,
      reuseExistingServer: false,
      timeout: 60_000,
    },
    {
      command: `npm run start -- --port ${webPort}`,
      url: `http://127.0.0.1:${webPort}`,
      env: { ML_API_URL: `http://127.0.0.1:${apiPort}` },
      reuseExistingServer: false,
      timeout: 60_000,
    },
  ],
});
