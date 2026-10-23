import { defineConfig } from "@playwright/test";
import path from "node:path";
const apiPort = process.env.OPSYNTRA_TEST_API_PORT || "8000";
const webPort = process.env.OPSYNTRA_TEST_WEB_PORT || "3000";

export default defineConfig({
  testDir: "./tests",
  fullyParallel: false,
  workers: 1,
  timeout: 45_000,
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
      command: `python -m uvicorn opsyntra.web_api:app --app-dir src --host 127.0.0.1 --port ${apiPort}`,
      cwd: path.resolve(process.cwd(), "../backend"),
      url: `http://127.0.0.1:${apiPort}/api/health`,
      reuseExistingServer: !process.env.CI,
      timeout: 60_000,
    },
    {
      command: `npm run start -- --port ${webPort}`,
      url: `http://127.0.0.1:${webPort}`,
      env: { ML_API_URL: `http://127.0.0.1:${apiPort}` },
      reuseExistingServer: !process.env.CI,
      timeout: 60_000,
    },
  ],
});
