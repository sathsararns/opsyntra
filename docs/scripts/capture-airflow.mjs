// Capture genuine completed Airflow runs through the local UI.
import { chromium } from "../../frontend/node_modules/@playwright/test/index.mjs";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
const output = new URL("../screenshots/", import.meta.url);
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ channel: "msedge", headless: true });
const captures = [];
try {
  for (const engine of ["sklearn", "spark"]) {
    const evidence = JSON.parse(await readFile(new URL(`../../backend/.airflow-runtime/last_success_${engine}.json`, import.meta.url), "utf8"));
    if (evidence.state !== "success" || evidence.tasks.length !== 8 || evidence.tasks.some(t => t.state !== "success")) throw new Error(`${engine} is not a completed successful run`);
    const page = await browser.newPage({ viewport: { width: 1800, height: 1100 } });
    const url = `http://localhost:8080/dags/${evidence.dag_id}/runs/${encodeURIComponent(evidence.run_id)}`;
    await page.goto(url);
    await page.getByText("validate_data", { exact: true }).first().waitFor({ timeout: 60000 });
    await page.getByRole("button", { name: "Show Grid (Press g)", exact: true }).click();
    await page.getByText("8 Task Instances", { exact: true }).waitFor();
    await page.waitForTimeout(1500);
    await page.screenshot({ path: fileURLToPath(new URL(`airflow-grid-${engine}.png`, output)), fullPage: true });
    await page.getByRole("button", { name: "Show Graph (Press g)", exact: true }).click();
    await page.getByRole("button", { name: "Collapse Details Panel", exact: true }).click();
    await page.waitForTimeout(1500);
    const fit = page.getByRole("button", { name: /fit view/i });
    if (await fit.count()) await fit.first().click();
    await page.waitForTimeout(1000);
    await page.screenshot({ path: fileURLToPath(new URL(`airflow-graph-${engine}.png`, output)), fullPage: true });
    captures.push({ ...evidence, url, screenshots: [`airflow-grid-${engine}.png`, `airflow-graph-${engine}.png`] });
    await page.goto(`${url}/tasks/save_results`);
    await page.getByRole("button", { name: "Show Grid (Press g)", exact: true }).click();
    await page.getByRole("link", { name: "Logs", exact: true }).waitFor({ timeout: 60000 });
    await page.getByRole("link", { name: "Logs", exact: true }).click();
    await page.getByText(/SUCCESS: manual__opsyntra/).first().waitFor({ timeout: 60000 });
    await page.waitForTimeout(1000);
    await page.screenshot({ path: fileURLToPath(new URL(`airflow-log-${engine}.png`, output)), fullPage: true });
    captures.at(-1).screenshots.push(`airflow-log-${engine}.png`);
    console.log(`${engine}: captured real Grid, Graph and successful save_results log`);
    await page.close();
  }
  await writeFile(new URL("airflow-evidence.json", output), JSON.stringify({ captured_at: new Date().toISOString(), source: "Actual local Airflow UI and scheduler CLI", runs: captures }, null, 2) + "\n");
} finally {
  await browser.close();
}
