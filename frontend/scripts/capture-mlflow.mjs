// Capture evidence from the real localhost MLflow UI after training.
import { chromium } from "@playwright/test";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const report = JSON.parse(await readFile(new URL("../../backend/reports/model_training_report.json", import.meta.url), "utf8"));
const output = new URL("../../docs/screenshots/", import.meta.url);
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ channel: process.platform === "win32" ? "msedge" : undefined, headless: true });
try {
  const page = await browser.newPage({ viewport: { width: 1600, height: 1100 }, deviceScaleFactor: 1 });
  page.on("pageerror", error => console.error(error.message));
  await page.goto(`http://127.0.0.1:5000/#/experiments/${report.tracking.experiment_id}/runs`);
  await page.getByText(report.tracking.experiment_name, { exact: true }).first().waitFor();
  await page.waitForTimeout(3000);
  if (await page.getByRole("button", { name: "Got it", exact: true }).isVisible()) {
    await page.getByRole("button", { name: "Got it", exact: true }).click();
  }
  await page.getByText("Model training", { exact: true }).click();
  await page.getByText("xgboost", { exact: true }).first().waitFor({ timeout: 60000 });
  await page.getByRole("button", { name: "Close", exact: true }).click();
  const search = page.getByPlaceholder('metrics.rmse < 1 and params.model = "tree"');
  await search.fill(`tags.batch_id = '${report.tracking.batch_id}'`);
  await search.press("Enter");
  await page.getByText("4 matching runs", { exact: true }).waitFor();
  await page.screenshot({ path: fileURLToPath(new URL("mlflow-runs.png", output)), fullPage: true });
  await page.getByRole("button", { name: "Columns", exact: true }).click();
  for (const column of ["Source", "Models", "Dataset", "validation_average_precision", "validation_f1", "validation_recall", "decision_threshold"]) {
    await page.getByText(column, { exact: true }).last().click();
  }
  await page.keyboard.press("Escape");
  await page.getByText("validation_average_precision", { exact: true }).first().waitFor();
  for (const handle of await page.locator(".ag-header-cell-resize").all()) {
    if (await handle.isVisible()) await handle.dblclick({ force: true });
  }
  await page.screenshot({ path: fileURLToPath(new URL("mlflow-comparison.png", output)) });
  const selected = report.tracking.runs[report.selected_model];
  await page.goto(`http://127.0.0.1:5000/#/experiments/${report.tracking.experiment_id}/runs/${selected}`);
  await page.getByText("decision_threshold", { exact: true }).first().waitFor();
  await page.getByPlaceholder("Search parameters").fill("decision_threshold");
  await page.getByPlaceholder("Search metrics").fill("test_");
  await page.screenshot({ path: fileURLToPath(new URL("mlflow-selected-model.png", output)) });
  await writeFile(new URL("mlflow-evidence.json", output), JSON.stringify({
    captured_at: new Date().toISOString(), batch_id: report.tracking.batch_id,
    experiment_id: report.tracking.experiment_id, runs: report.tracking.runs,
    selected_model: report.selected_model, model_version: report.selected_model_version,
    decision_threshold: report.decision_threshold, test_metrics: report.selected_test_metrics,
    dataset_sha256: report.provenance.source_sha256, split_sha256: report.provenance.membership_sha256,
    screenshots: ["mlflow-runs.png", "mlflow-comparison.png", "mlflow-selected-model.png"],
  }, null, 2) + "\n");
  console.log(`Saved MLflow screenshots for batch ${report.tracking.batch_id}, selected run ${selected}.`);
} finally {
  await browser.close();
}
