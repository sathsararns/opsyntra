import { expect, test } from "@playwright/test";
import { readFile } from "node:fs/promises";
import path from "node:path";

test("overview shows real reports, changes cohorts and exports data", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/");
  await expect(page.getByText("Model connected", { exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Retention, in focus." })).toBeVisible();
  await expect(page.locator(".metric-value").first()).toHaveText("7,043");
  await page.getByLabel("Group customer cohorts by").selectOption("contract");
  await expect(page.getByRole("img", { name: "Retention and churn percentages grouped by contract" })).toBeVisible();
  await page.getByLabel("Group customer cohorts by").selectOption("tenure");
  const downloading = page.waitForEvent("download");
  await page.getByRole("button", { name: "Export report" }).click();
  const downloaded = await downloading;
  const report = JSON.parse(await readFile((await downloaded.path())!, "utf8"));
  expect(report.dataset.customers).toBe(7043);
  expect(report.model.name).toBe("random_forest");
  expect(errors).toEqual([]);
  await page.screenshot({ path: path.resolve("../docs/screenshots/frontend-desktop.png"), fullPage: true });
});

test("a customer form uses the real pipeline and stores clearable history", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Model connected", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Customer prediction", exact: true }).click();
  await page.getByRole("button", { name: "Load example" }).click();
  const predicting = page.waitForResponse((response) => response.url().endsWith("/api/predict") && response.request().method() === "POST");
  await page.getByRole("button", { name: "Predict churn risk" }).click();
  const result = (await (await predicting).json()).predictions[0];
  await expect(page.getByText(result.prediction === "Yes" ? "Predicted to churn" : "Predicted to stay", { exact: true })).toBeVisible();
  await expect(page.locator(".risk-gauge strong")).toHaveText(`${(result.churn_probability * 100).toFixed(1)}%`);
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: "instant" }));
  await page.screenshot({ path: path.resolve("../docs/screenshots/frontend-prediction.png"), fullPage: true });
  await page.getByRole("button", { name: "Overview", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Recent predictions" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "DEMO-NEW-001" })).toBeVisible();
  await page.getByRole("button", { name: "Clear history" }).click();
  await expect(page.getByRole("heading", { name: "Recent predictions" })).toHaveCount(0);
});

test("CSV batch produces real searchable predictions and downloadable results", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Model connected", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Batch predictions", exact: true }).click();
  await page.locator("input[type=file]").setInputFiles(path.resolve("public/sample-customers.csv"));
  await page.getByRole("button", { name: "Run batch prediction" }).click();
  await expect(page.getByRole("heading", { name: "Your batch, decoded" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "DEMO-NEW-001" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "DEMO-NEW-002" })).toBeVisible();
  await page.getByLabel("Search customer IDs").fill("002");
  await expect(page.getByRole("cell", { name: "DEMO-NEW-001" })).toHaveCount(0);
  await expect(page.getByRole("cell", { name: "DEMO-NEW-002" })).toBeVisible();
  const downloading = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download results" }).click();
  const downloaded = await downloading;
  const csv = await readFile((await downloaded.path())!, "utf8");
  expect(csv).toContain("DEMO-NEW-001");
  expect(csv).toContain("DEMO-NEW-002");
  await page.getByLabel("Filter predictions").selectOption("Yes");
  // Search still restricts this to the second example (a low-risk customer).
  await expect(page.getByText("No customers match this search.")).toBeVisible();
});

test("model evidence, data quality and setup guide are navigable", async ({ page }) => {
  const overview = await (await page.request.get("/api/overview")).json();
  await page.goto("/");
  await page.getByRole("button", { name: "Model performance", exact: true }).click();
  await expect(page.getByRole("heading", { name: "The candidate comparison" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "XGBoost", exact: true })).toBeVisible();
  await expect(page.locator(".confusion-matrix")).toContainText(String(overview.model.metrics.confusion_matrix[0][0]));
  await expect(page.getByText(/Decision threshold/)).toBeVisible();
  await page.getByRole("button", { name: "Data quality", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Quality checks" })).toBeVisible();
  await expect(page.locator(".quality-checks")).toContainText("Passed");
  await page.getByRole("button", { name: "Workspace guide", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Connect the prediction service" })).toBeVisible();
});

test("mobile navigation works without page overflow", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await expect(page.getByText("Model connected", { exact: true })).toHaveCount(1);
  await expect(page.getByRole("heading", { name: "Retention, in focus." })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBe(true);
  await page.screenshot({ path: path.resolve("../docs/screenshots/frontend-mobile.png"), fullPage: true });
  await page.getByRole("button", { name: "Open navigation" }).click();
  await page.getByRole("button", { name: "Batch predictions", exact: true }).click();
  await expect(page.getByRole("heading", { name: "A bigger picture. In one go." })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBe(true);
});

test("an unavailable API is labelled and cannot fabricate predictions", async ({ page }) => {
  await page.route("**/api/overview", (route) => route.fulfill({ status: 503, contentType: "application/json", body: JSON.stringify({ error: "Service offline" }) }));
  await page.goto("/");
  await expect(page.getByText("Saved report snapshot", { exact: true })).toBeVisible();
  await expect(page.getByText(/Showing your saved dataset and model reports/)).toBeVisible();
  await page.getByRole("button", { name: "Customer prediction", exact: true }).click();
  await expect(page.getByRole("button", { name: "Predict churn risk" })).toBeDisabled();
});
