// Screenshots of real Airflow UI and explicitly labelled recorded CLI transcripts.
import { createRequire } from "node:module";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
const require = createRequire(new URL("../../frontend/package.json", import.meta.url));
const { chromium } = require("@playwright/test");
const output = new URL("../screenshots/", import.meta.url);
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ channel: "msedge", headless: true });
const captures = [];
const escape = value => value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
try {
  const page = await browser.newPage({ viewport: { width: 1800, height: 1100 } });
  for (const kind of ["batch", "streaming"]) {
    const evidence = JSON.parse(await readFile(new URL(`../../backend/.airflow-runtime/last_success_kafka_${kind}.json`, import.meta.url), "utf8"));
    if (evidence.state !== "success" || evidence.tasks.length !== 4 || evidence.tasks.some(t => t.state !== "success")) throw new Error(`${kind} has no complete successful scheduler run`);
    const url = `http://localhost:8080/dags/${evidence.dag_id}/runs/${encodeURIComponent(evidence.run_id)}`;
    await page.goto(url);
    await page.getByText("health_checks", { exact: true }).first().waitFor({ timeout: 60000 });
    await page.getByRole("button", { name: "Show Grid (Press g)", exact: true }).click();
    await page.getByText("4 Task Instances", { exact: true }).waitFor();
    await page.waitForTimeout(1000);
    await page.screenshot({ path: fileURLToPath(new URL(`kafka-airflow-${kind}-grid.png`, output)), fullPage: true });
    await page.getByRole("button", { name: "Show Graph (Press g)", exact: true }).click();
    const collapse = page.getByRole("button", { name: "Collapse Details Panel", exact: true });
    if (await collapse.count()) await collapse.click();
    await page.waitForTimeout(1000);
    const fit = page.getByRole("button", { name: /fit view/i });
    if (await fit.count()) await fit.first().click();
    await page.screenshot({ path: fileURLToPath(new URL(`kafka-airflow-${kind}-graph.png`, output)), fullPage: true });
    const task = kind === "streaming" ? "start_and_monitor_consumer" : "summarize";
    await page.goto(`${url}/tasks/${task}`);
    await page.getByRole("button", { name: "Show Grid (Press g)", exact: true }).click();
    await page.getByRole("link", { name: "Logs", exact: true }).waitFor({ timeout: 60000 });
    await page.getByRole("link", { name: "Logs", exact: true }).click();
    await page.waitForTimeout(3000);
    // The log viewer virtualizes rows and auto-scrolls to the bottom. Scroll
    // its actual containers to the top so startup/heartbeat lines are rendered.
    await page.evaluate(() => {
      for (const element of document.querySelectorAll("*")) {
        if (/auto|scroll/.test(getComputedStyle(element).overflowY) && element.scrollHeight > element.clientHeight) element.scrollTop = 0;
      }
    });
    await page.waitForTimeout(1000);
    await page.getByText(kind === "streaming" ? /STREAMING heartbeat/ : /SUCCESS: batch Kafka/).first().waitFor({ timeout: 60000 });
    await page.waitForTimeout(1000);
    await page.screenshot({ path: fileURLToPath(new URL(`kafka-airflow-${kind}-log.png`, output)), fullPage: true });
    captures.push({ ...evidence, url, screenshots: [`kafka-airflow-${kind}-grid.png`, `kafka-airflow-${kind}-graph.png`, `kafka-airflow-${kind}-log.png`] });
  }
  for (const [name, title, files] of [
    ["kafka-input-output", "Kafka input and model predictions", ["kafka-raw.txt", "kafka-predictions.txt"]],
    ["kafka-deadletter", "Malformed messages routed to deadletter", ["kafka-deadletter.txt"]],
    ["kafka-recovery", "Real crash and restart: delivery ledger suppresses replay", ["crash_after_ledger_commit.log", "restart_after_ledger_commit.log"]],
    ["kafka-replay-window", "Real crash and restart: output ack before ledger can replay", ["crash_after_output_ack.log", "restart_after_output_ack.log"]],
  ]) {
    const texts = await Promise.all(files.map(async name => [name, await readFile(new URL(`../evidence/${name}`, import.meta.url), "utf8")]));
    const html = `<!doctype html><html><meta charset="utf-8"><title>${title}</title><style>body{margin:0;padding:32px;background:#101820;color:#e0eced;font:16px/1.5 Consolas,monospace}h1{font:600 28px system-ui;color:#40d99d}p{color:#a7c1c5}section{background:#182630;padding:20px;margin:20px 0;border:1px solid #35505b;border-radius:8px}h2{font-size:18px;color:#62dbb3}pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:14px;margin:0}</style><h1>${title}</h1><p>Recorded CLI output · actual local broker / recovery checks · this transcript view is not a Kafka administration UI.</p>${texts.map(([name, text]) => `<section><h2>${name}</h2><pre>${escape(text)}</pre></section>`).join("")}</html>`;
    await writeFile(new URL(`../evidence/${name}.html`, import.meta.url), html);
    await page.setViewportSize({ width: 1600, height: 1100 });
    await page.goto(new URL(`../evidence/${name}.html`, import.meta.url).href);
    await page.screenshot({ path: fileURLToPath(new URL(`${name}.png`, output)), fullPage: true });
    captures.push({ kind: "recorded CLI transcript", screenshot: `${name}.png`, sources: files });
  }
  await writeFile(new URL("kafka-evidence.json", output), JSON.stringify({ captured_at: new Date().toISOString(), captures }, null, 2) + "\n");
  console.log("Saved actual bonus Airflow UI and labelled Kafka CLI/recovery transcript screenshots.");
} finally {
  await browser.close();
}
