import { chromium } from "../frontend/node_modules/playwright-core/index.mjs";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

const url = process.env.OPSYNTRA_CONTAINER_WEB_URL;
const directory = process.env.OPSYNTRA_CONTAINER_EVIDENCE;
if (!url || !directory) throw new Error("Use the isolated containers/verify.py runner");
const browser = await chromium.launch({ headless: true, ...(process.platform === "win32" ? { channel: "msedge" } : {}) });
try {
  const page = await browser.newPage({ viewport: { width: 1500, height: 1100 } });
  await page.goto(url);
  await page.getByRole("button", { name: "Live monitoring", exact: true }).click();
  await page.getByTestId("live-volume").filter({ hasText: /^3$/ }).waitFor({ timeout: 45000 });
  const response = await page.request.get(url + "/api/analytics/predictions");
  const predictions = await response.json();
  const first = predictions.items[0];
  if (predictions.total !== 3 || !first.event_id || !first.model_version) throw new Error("Missing actual predictions");
  await page.getByTestId(`stream-${first.prediction_id}`).first().locator("summary").click();
  await page.getByText(first.event_id, { exact: true }).first().waitFor();
  const model = await page.getByTestId("live-model-version").innerText();
  if (model !== first.model_version) throw new Error("Dashboard model mismatch");
  await mkdir(directory, { recursive: true });
  await page.screenshot({ path: path.join(directory, "live-monitoring.png"), fullPage: true });
  await writeFile(path.join(directory, "browser.json"), JSON.stringify({ volume: 3, event_id: first.event_id, model_version: model, prediction_id: first.prediction_id }, null, 2));
  console.log("PASS: real production UI count, event ID and model version agree with API/SQL");
} finally {
  await browser.close();
}
