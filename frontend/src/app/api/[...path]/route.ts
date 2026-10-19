import { NextRequest, NextResponse } from "next/server";

const allowed = new Map([
  ["health", "GET"], ["overview", "GET"], ["predict", "POST"], ["predict/batch", "POST"],
]);

async function proxy(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  const endpoint = path.join("/");
  if (allowed.get(endpoint) !== request.method) {
    return NextResponse.json({ error: "Endpoint not found." }, { status: 404 });
  }
  try {
    const body = request.method === "POST" ? await request.text() : undefined;
    if (body && Buffer.byteLength(body, "utf8") > 3_000_000) {
      return NextResponse.json({ error: "Choose a CSV smaller than 2 MB." }, { status: 413 });
    }
    const base = (process.env.ML_API_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
    const response = await fetch(`${base}/api/${endpoint}`, {
      method: request.method, headers: { "Content-Type": "application/json" },
      body, cache: "no-store", signal: AbortSignal.timeout(85_000),
    });
    return new NextResponse(await response.text(), {
      status: response.status, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    });
  } catch {
    return NextResponse.json({ error: "Prediction service is offline. Start the local API and reconnect from the sidebar." }, { status: 503 });
  }
}

export const GET = proxy;
export const POST = proxy;
