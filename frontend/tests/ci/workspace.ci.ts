import { expect, test } from "@playwright/test";
import { readFile } from "node:fs/promises";
import path from "node:path";

test("synthetic fixture overview loads genuine reports and exports them", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  const response = await page.request.get("/api/overview");
  expect(response.ok()).toBe(true);
  const overview = await response.json();
  expect(overview.ci_fixture).toBe(true);
  expect(overview.dataset.customers).toBe(120);
  await page.goto("/");
  await expect(page.getByText("Model connected", { exact: true })).toBeVisible();
  await expect(page.locator(".metric-value").first()).toHaveText("120");
  await page.getByLabel("Group customer cohorts by").selectOption("contract");
  await expect(page.getByRole("img", { name: "Retention and churn percentages grouped by contract" })).toBeVisible();
  const download = page.waitForEvent("download");
  await page.getByRole("button", { name: "Export report" }).click();
  const result = JSON.parse(await readFile((await (await download).path())!, "utf8"));
  expect(result.dataset.customers).toBe(overview.dataset.customers);
  expect(result.model.version).toBe(overview.model.version);
  expect(errors).toEqual([]);
});

test("single prediction agrees with API and history can be cleared", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Model connected", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Customer prediction", exact: true }).click();
  await page.getByRole("button", { name: "Load example" }).click();
  const pending = page.waitForResponse(r => r.url().endsWith("/api/predict") && r.request().method() === "POST");
  await page.getByRole("button", { name: "Predict churn risk" }).click();
  const prediction = (await (await pending).json()).predictions[0];
  await expect(page.locator(".risk-gauge strong")).toHaveText(`${(prediction.churn_probability * 100).toFixed(1)}%`);
  await expect(page.getByText(prediction.prediction === "Yes" ? "Predicted to churn" : "Predicted to stay", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Overview", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Recent predictions" })).toBeVisible();
  await page.getByRole("button", { name: "Clear history" }).click();
  await expect(page.getByRole("heading", { name: "Recent predictions" })).toHaveCount(0);
});

test("CSV inference matches direct API probabilities and downloads both IDs", async ({ page }) => {
  const input = path.resolve("public/sample-customers.csv");
  const direct = await page.request.post("/api/predict/batch", { data: { csv_text: await readFile(input, "utf8") } });
  expect(direct.ok()).toBe(true);
  const expected = await direct.json();
  await page.goto("/");
  await expect(page.getByText("Model connected", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Batch predictions", exact: true }).click();
  await page.locator("input[type=file]").setInputFiles(input);
  const pending = page.waitForResponse(r => r.url().endsWith("/api/predict/batch") && r.request().method() === "POST");
  await page.getByRole("button", { name: "Run batch prediction" }).click();
  const actual = await (await pending).json();
  expect(actual.predictions).toEqual(expected.predictions);
  await expect(page.getByRole("heading", { name: "Your batch, decoded" })).toBeVisible();
  await page.getByLabel("Search customer IDs").fill("002");
  await expect(page.getByRole("cell", { name: "DEMO-NEW-001" })).toHaveCount(0);
  await expect(page.getByRole("cell", { name: "DEMO-NEW-002" })).toBeVisible();
  const pendingDownload = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download results" }).click();
  const csv = await readFile((await (await pendingDownload).path())!, "utf8");
  expect(csv).toContain("DEMO-NEW-001"); expect(csv).toContain("DEMO-NEW-002");
});

test("model and data-quality screens reflect synthetic saved reports", async ({ page }) => {
  const overview = await (await page.request.get("/api/overview")).json();
  await page.goto("/");
  await expect(page.getByText("Model connected", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Model performance", exact: true }).click();
  await expect(page.getByRole("cell", { name: "XGBoost", exact: true })).toBeVisible();
  await expect(page.locator(".confusion-matrix")).toContainText(String(overview.model.metrics.confusion_matrix[0][0]));
  await page.getByRole("button", { name: "Data quality", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Quality checks" })).toBeVisible();
  await expect(page.locator(".quality-checks")).toContainText("Passed");
});

test("mobile navigation, supplied logo and offline prediction state work", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await expect(page.getByText("Model connected", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Open navigation" }).click();
  await expect(page.getByRole("img", { name: "Opsyntra", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Batch predictions", exact: true }).click();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBe(true);
  await page.route("**/api/overview", route => route.fulfill({ status: 503, json: { detail: "Fixture outage" } }));
  await page.reload();
  await expect(page.getByText("Saved report snapshot", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Open navigation" }).click();
  await page.getByRole("button", { name: "Customer prediction", exact: true }).click();
  await expect(page.getByRole("button", { name: "Predict churn risk" })).toBeDisabled();
});
