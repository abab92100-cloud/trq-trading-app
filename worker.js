// TRQ relay (Cloudflare Worker) - optional fallback for the TRQ trading app (v1.5)
// Forwards requests to KuCoin Futures API only. All other targets are rejected.
export default {
  async fetch(request) {
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, KC-API-KEY, KC-API-SIGN, KC-API-TIMESTAMP, KC-API-PASSPHRASE, KC-API-KEY-VERSION",
      "Access-Control-Max-Age": "86400"
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    const u = new URL(request.url);
    let target = u.search.slice(1);
    if (target.startsWith("url=")) target = decodeURIComponent(target.slice(4));
    if (!/^https:\/\/api-futures\.kucoin\.com\//.test(target)) {
      return new Response(JSON.stringify({ error: "target not allowed" }), { status: 400, headers: { ...cors, "Content-Type": "application/json" } });
    }
    const h = new Headers();
    for (const [k, v] of request.headers) {
      const lk = k.toLowerCase();
      if (["host","origin","referer","content-length","accept-encoding","cf-connecting-ip","cf-ipcountry","cf-ray","cf-visitor","x-forwarded-proto","x-forwarded-for","x-forwarded-host","true-client-ip","cdn-loop","sec-fetch-mode","sec-fetch-site","sec-fetch-dest"].includes(lk)) continue;
      h.set(k, v);
    }
    const init = { method: request.method, headers: h };
    if (request.method !== "GET" && request.method !== "HEAD") init.body = request.body;
    const upstream = await fetch(target, init);
    const res = new Response(upstream.body, upstream);
    for (const [k, v] of Object.entries(cors)) res.headers.set(k, v);
    return res;
  },
};
