import { hydrateGatewayModelPricingCacheFromSnapshot } from "../gateway/model-pricing-cache-state.js";
import { parseUsageCostRefreshParams, refreshCostUsageCache } from "./session-cost-usage.js";

const RESULT_MARKER = "openclawUsageCostRefresh";

async function readStdin(): Promise<string> {
  let raw = "";
  process.stdin.setEncoding("utf-8");
  for await (const chunk of process.stdin) {
    raw += chunk;
  }
  return raw;
}

try {
  const params = parseUsageCostRefreshParams(await readStdin());
  if (params.gatewayModelPricingCache) {
    hydrateGatewayModelPricingCacheFromSnapshot(params.gatewayModelPricingCache);
  }
  const result = await refreshCostUsageCache(params);
  process.stdout.write(`${JSON.stringify({ marker: RESULT_MARKER, result })}\n`);
} catch (error) {
  const message = error instanceof Error ? error.stack || error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
}
