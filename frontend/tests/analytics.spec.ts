import { expect, test, type Page } from "@playwright/test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
const execute = promisify(execFile);
const screenshotDir = process.env.OPSYNTRA_SCREENSHOT_DIR || "../docs/screenshots";

function fixture(volume = 1) {
  const now = new Date().toISOString();
  const scope = { start: new Date(Date.now()-86400000).toISOString(), end: now, engine: "sklearn", model_version: "browser-fixture" };
  const envelope = { scope, timezone: "Asia/Colombo", generated_at: now };
  const row = { prediction_id: "a".repeat(64), event_id: "00000000-0000-4000-8000-000000000001", customer_id: "BROWSER-FIXTURE", engine: "sklearn", model_version: "browser-fixture", churn_probability:.8,prediction:"Yes",event_ts:now,processed_ts:now,ingested_at:now,raw_available:true,contract:"Month-to-month",payment_method:"Electronic check" };
  return { health: { database:"ready",broker:"ready",consumer_state:"active",lag:0,total_predictions:volume,last_ingested_at:now,recent_deliveries:volume,stale_after_seconds:60 },
    summary:{ ...envelope,metrics:{ prediction_volume:volume,unique_events:volume,unique_customers:volume,predicted_churn_share:volume?1:null,mean_risk:volume ? .8 : null,matched_raw_predictions:volume,last_ingested_at:now },available_models:[{engine:"sklearn",model_version:"browser-fixture",last_ingested_at:now}],segments:{contract:[],payment_method:[]},raw_event_volume:volume },
    page:{...envelope,items:volume?[row]:[],total:volume,offset:0,limit:10},series:{...envelope,interval:"hour",bucket_timezone:"UTC",items:[{bucket_start:now,predictions:volume}]} };
}
async function openLive(page: Page) {
  await page.goto("/");
  await page.getByRole("button",{name:"Live monitoring",exact:true}).click();
  await expect(page.getByRole("heading",{name:"Live monitoring."})).toBeVisible();
}

test("polling updates successful zero activity without a reload",async({page})=>{
  let volume=0;
  await page.route("**/api/analytics/**", async route=>{
    const f=fixture(volume), endpoint=new URL(route.request().url()).pathname.split("/").pop();
    await route.fulfill({json:endpoint==="health"?f.health:endpoint==="summary"?f.summary:endpoint==="timeseries"?f.series:f.page});
  });
  await openLive(page);
  await expect(page.getByTestId("live-volume")).toHaveText("0");
  await expect(page.getByRole("heading",{name:"Empty stream",exact:true})).toBeVisible();
  await expect(page.getByTestId("live-status")).toHaveText("Connected · no new activity");
  await page.getByLabel("Analytics refresh interval").selectOption("1000");
  volume=1;
  await expect(page.getByTestId("live-volume")).toHaveText("1");
  await expect(page.getByRole("cell",{name:"BROWSER-FIXTURE",exact:true}).first()).toBeVisible();
});

test("outages retain labelled old data and recover automatically",async({page})=>{
  let offline=false,volume=1;
  await page.route("**/api/analytics/**",async route=>{
    if(offline){await route.fulfill({status:503,json:{error:"Analytics API disconnected"}});return;}
    const f=fixture(volume),e=new URL(route.request().url()).pathname.split("/").pop();
    await route.fulfill({json:e==="health"?f.health:e==="summary"?f.summary:e==="timeseries"?f.series:f.page});
  });
  await openLive(page);
  await expect(page.getByTestId("live-volume")).toHaveText("1");
  await page.getByLabel("Analytics refresh interval").selectOption("1000");
  offline=true;
  await expect(page.locator(".live-monitoring").getByRole("alert")).toContainText("last successful result");
  await expect(page.getByTestId("live-status")).toHaveText("Disconnected");
  await expect(page.getByTestId("live-volume")).toHaveText("1");
  offline=false;volume=2;
  await expect(page.getByTestId("live-volume")).toHaveText("2");
  await expect(page.locator(".live-monitoring").getByRole("alert")).toHaveCount(0);
});

test("stale consumer, broker outage and database outage are distinct",async({page})=>{
  let mode="inactive";
  await page.route("**/api/analytics/**",async route=>{
    const f=fixture(0),e=new URL(route.request().url()).pathname.split("/").pop();
    if(e==="health"){
      if(mode==="database"){await route.fulfill({status:503,json:{database:"unavailable",broker:"unknown",consumer_state:"unknown",detail:"Analytics database unavailable."}});return;}
      await route.fulfill({json:{...f.health,total_predictions:10,consumer_state:mode==="inactive"?"inactive":"active",last_ingested_at:new Date(Date.now()-120000).toISOString(),broker:mode==="broker"?"unavailable":"ready"}});return;
    }
    await route.fulfill({json:e==="summary"?f.summary:e==="timeseries"?f.series:f.page});
  });
  await openLive(page);
  await expect(page.getByTestId("live-status")).toHaveText("Consumer inactive");
  await expect(page.getByText(/Stored data is stale/)).toBeVisible();
  await expect(page.getByRole("heading",{name:"No activity in this selection"})).toBeVisible();
  await page.getByLabel("Analytics refresh interval").selectOption("1000");
  mode="broker";
  await expect(page.getByTestId("live-status")).toHaveText("Broker disconnected");
  mode="database";
  await expect(page.locator(".live-monitoring").getByRole("alert")).toContainText("Analytics database unavailable");
});

test("live monitoring fits a mobile screen and preserves the supplied logo",async({page})=>{
  const f=fixture(0);
  await page.route("**/api/analytics/**",async route=>{
    const e=new URL(route.request().url()).pathname.split("/").pop();
    await route.fulfill({json:e==="health"?f.health:e==="summary"?f.summary:e==="timeseries"?f.series:f.page});
  });
  await page.setViewportSize({width:390,height:844});
  await page.goto("/");
  await page.getByRole("button",{name:"Open navigation"}).click();
  await expect(page.getByRole("img",{name:"Opsyntra",exact:true})).toBeVisible();
  await page.getByRole("button",{name:"Live monitoring",exact:true}).click();
  await expect(page.getByTestId("live-volume")).toHaveText("0");
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
});

test("a real producer event appears automatically and dashboard counts match SQL",async({page})=>{
  test.skip(!process.env.OPSYNTRA_DASHBOARD_MANIFEST,"Run tools.verification.analytics_dashboard_check for isolated real Kafka/model/database verification");
  const manifestPath=process.env.OPSYNTRA_DASHBOARD_MANIFEST!;
  const manifest=JSON.parse(await readFile(manifestPath,"utf8"));
  await openLive(page);
  await page.getByLabel("Analytics customer ID").fill(manifest.customer_id);
  await page.getByRole("button",{name:"Apply filters"}).click();
  await expect(page.getByTestId("live-volume")).toHaveText("0");
  const python=process.env.OPSYNTRA_TEST_PYTHON || "python";
  const helper=path.resolve("../backend/tools/verification/dashboard_event.py");
  const published=JSON.parse((await execute(python,[helper,"publish","--manifest",manifestPath],{maxBuffer:2000000})).stdout);
  const row=page.getByRole("row").filter({has:page.getByRole("cell",{name:manifest.customer_id,exact:true})}).first();
  await expect(row).toBeVisible({timeout:20000});
  const seenAt=Date.now();
  const json=await (await page.request.get(`/api/analytics/predictions?customer_id=${encodeURIComponent(manifest.customer_id)}`)).json();
  expect(json.total).toBe(1);
  const prediction=json.items[0];
  expect(prediction.event_id).toBe(published.event_id);
  const latency=(seenAt-Date.parse(prediction.ingested_at))/1000;
  expect(latency).toBeGreaterThanOrEqual(0);
  expect(latency).toBeLessThanOrEqual(10);
  await expect(page.getByTestId("live-volume")).toHaveText("1");
  await row.getByText("Trace event",{exact:true}).click();
  await expect(row.getByText(prediction.event_id,{exact:true})).toBeVisible();
  await expect(row.getByText(prediction.model_version,{exact:true})).toBeVisible();
  const summary=await(await page.request.get(`/api/analytics/summary?customer_id=${encodeURIComponent(manifest.customer_id)}`)).json();
  const summaryFile=path.join(manifest.directory,"browser-summary.json");
  await writeFile(summaryFile,JSON.stringify(summary));
  const sql=JSON.parse((await execute(python,[helper,"check-sql","--manifest",manifestPath,"--summary",summaryFile])).stdout);
  expect(sql.prediction_volume).toBe(summary.metrics.prediction_volume);
  expect(sql.unique_events).toBe(summary.metrics.unique_events);
  expect(sql.unique_customers).toBe(summary.metrics.unique_customers);
  expect(sql.predicted_churn_share).toBe(summary.metrics.predicted_churn_share);
  expect(sql.mean_risk).toBeCloseTo(summary.metrics.mean_risk,12);
  const measurement={event_id:prediction.event_id,prediction_id:prediction.prediction_id,model_version:prediction.model_version,customer_id:manifest.customer_id,published_at:published.published_at,ingested_at:prediction.ingested_at,visible_at:new Date(seenAt).toISOString(),ingestion_to_visible_seconds:latency,poll_interval_ms:5000,sql_agreement:sql};
  await writeFile(path.join(manifest.directory,"visibility.json"),JSON.stringify(measurement,null,2));
  await page.evaluate(()=>{(document.activeElement as HTMLElement)?.blur();window.scrollTo({top:0,behavior:"instant"});});
  await page.screenshot({path:path.resolve(screenshotDir,"live-monitoring.png"),fullPage:true,animations:"disabled"});
  await page.setViewportSize({width:390,height:844});
  await page.evaluate(()=>window.scrollTo({top:0,behavior:"instant"}));
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
  await page.screenshot({path:path.resolve(screenshotDir,"live-monitoring-mobile.png"),fullPage:true,animations:"disabled"});
});
