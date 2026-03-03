/**
 * Button Game – Relay Server  (WebSocket subscription edition)
 *
 * Instead of polling, this server SUBSCRIBES to on-chain account changes via
 * Solana's WebSocket API. The RPC node pushes a notification the instant the
 * state or vault account is written — typically within 100–400 ms of a tx
 * confirming, with zero polling overhead.
 *
 * Architecture:
 *   Solana WS ──push──► relay-server ──SSE fan-out──► all browser clients
 *
 * Fallback: a 1-second safety poll runs in the background to catch any missed
 * WS notifications (e.g. dropped messages during reconnect). It is cheap
 * because it only broadcasts when data actually changed.
 *
 * Deploy anywhere Node.js runs: Railway, Render, Fly.io, a VPS, etc.
 *
 * Install:  npm install @solana/web3.js
 * Run:      node relay-server.js
 *
 * Environment variables (all optional):
 *   PORT       – HTTP port (default: 3001)
 *   CLUSTER    – "devnet" | "mainnet-beta" (default: "devnet")
 *   RPC_URL    – HTTP RPC endpoint  (overrides CLUSTER)
 *   WS_URL     – WebSocket RPC endpoint (overrides CLUSTER; must be ws:// or wss://)
 *   POLL_MS    – Safety-poll interval in ms (default: 1000)
 */

"use strict";

const http = require("http");
const { Connection, PublicKey } = require("@solana/web3.js");

// ─── CONFIG ────────────────────────────────────────────────────────────────
const PORT    = Number(process.env.PORT || 3001);
const CLUSTER = process.env.CLUSTER || "devnet";
const POLL_MS = Number(process.env.POLL_MS || 1000); // safety-poll fallback

const DEFAULT_HTTP = CLUSTER === "mainnet-beta"
  ? "https://api.mainnet-beta.solana.com"
  : "https://api.devnet.solana.com";

const DEFAULT_WS = CLUSTER === "mainnet-beta"
  ? "wss://api.mainnet-beta.solana.com"
  : "wss://api.devnet.solana.com";

const RPC_URL = process.env.RPC_URL || DEFAULT_HTTP;
const WS_URL  = process.env.WS_URL  || DEFAULT_WS;

const STATE_ADDR = "FnCLwBY38p1LUtCs6GaC438EZ3HmanAdAhGB4nfNANAz";
const VAULT_ADDR = "CbpG1mzYkbPKAKcVMDsjfPnqJhDdHceHXuuQ9UUeA9K";

const VAULT_TTL_MS = 30_000; // max time between vault reads if plays haven't changed

// ─── SOLANA ────────────────────────────────────────────────────────────────
const connection = new Connection(RPC_URL, {
  commitment: "confirmed",
  wsEndpoint: WS_URL,
});
const statePk = new PublicKey(STATE_ADDR);
const vaultPk = new PublicKey(VAULT_ADDR);

// ─── DECODE STATE (mirrors app.js exactly) ─────────────────────────────────
function readPubkey(data, offset) {
  return new PublicKey(data.slice(offset, offset + 32)).toBase58();
}
function readU64(data, offset) {
  const view = new DataView(data.buffer, data.byteOffset + offset, 8);
  return view.getBigUint64(0, true).toString();
}
function readI64(data, offset) {
  const view = new DataView(data.buffer, data.byteOffset + offset, 8);
  return Number(view.getBigInt64(0, true));
}
function readU32(data, offset) {
  const view = new DataView(data.buffer, data.byteOffset + offset, 4);
  return view.getUint32(0, true);
}

function decodeState(accountData) {
  const data = accountData.slice(8); // skip Anchor discriminator
  let o = 0;

  const owner             = readPubkey(data, o); o += 32;
  const tokenMint         = readPubkey(data, o); o += 32;
  const vault             = readPubkey(data, o); o += 32;
  const playCost          = readU64(data, o);    o += 8;
  const roundDurationSecs = readI64(data, o);    o += 8;
  const currentWinner     = readPubkey(data, o); o += 32;
  const currentWinnerAta  = readPubkey(data, o); o += 32;
  const timerEnd          = readI64(data, o);    o += 8;
  const cooldownEnd       = readI64(data, o);    o += 8;
  const unclaimed         = data[o] === 1;       o += 1;
  const sessionPlays      = readU32(data, o);    o += 4;
  const enabled           = data[o] === 1;       o += 1;
  const bump              = data[o];

  return {
    owner, tokenMint, vault,
    playCost, roundDurationSecs,
    currentWinner, currentWinnerAta,
    timerEnd, cooldownEnd,
    unclaimed, sessionPlays, enabled, bump,
  };
}

// ─── RELAY STATE ───────────────────────────────────────────────────────────
let latestPayload      = null;   // last SSE message, replayed to new clients instantly
let lastStateSig       = "";
let lastPlaysSeen      = null;
let lastVaultFetchMs   = 0;
let vaultFetchInFlight = false;
const clients          = new Set();

function stateSignature(gs) {
  return [
    gs.enabled ? 1 : 0,
    gs.timerEnd,
    gs.cooldownEnd,
    gs.unclaimed ? 1 : 0,
    gs.sessionPlays,
    gs.currentWinner,
    gs.playCost,
  ].join("|");
}

function broadcast(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  latestPayload = msg;
  for (const res of clients) {
    try { res.write(msg); }
    catch { clients.delete(res); }
  }
}

// Fetch vault balance and, if it changed, broadcast an updated state payload
async function refreshVault(gs, reason) {
  if (vaultFetchInFlight) return;
  const nowMs     = Date.now();
  const plays     = gs.sessionPlays;
  const changed   = lastPlaysSeen !== null && plays !== lastPlaysSeen;
  const ttlExpired= (nowMs - lastVaultFetchMs) >= VAULT_TTL_MS;
  const never     = lastVaultFetchMs === 0;

  if (!never && !changed && !ttlExpired) return;

  vaultFetchInFlight = true;
  try {
    const bal = await connection.getTokenAccountBalance(vaultPk, "confirmed");
    const vaultAmount = bal?.value?.amount || "0";
    lastVaultFetchMs = Date.now();
    lastPlaysSeen    = plays;
    broadcast({
      type: "state",
      serverTs: Math.floor(Date.now() / 1000),
      state: gs,
      vaultAmount,
    });
  } catch (e) {
    console.warn(`[Vault] fetch failed (${reason}):`, e.message);
  } finally {
    vaultFetchInFlight = false;
  }
}

// Core handler — called from WS notifications AND safety polls
async function handleStateData(rawData, source) {
  let gs;
  try { gs = decodeState(rawData); }
  catch (e) { console.error("[Decode] failed:", e.message); return; }

  const sig     = stateSignature(gs);
  const changed = sig !== lastStateSig;

  if (changed) {
    lastStateSig = sig;
    console.log(`[State] changed via ${source} — plays=${gs.sessionPlays} timerEnd=${gs.timerEnd}`);

    // Push state immediately so clients react without waiting for the vault fetch
    broadcast({ type: "state", serverTs: Math.floor(Date.now() / 1000), state: gs });

    // Fetch vault in the background; will broadcast again with vaultAmount attached
    await refreshVault(gs, source);
  } else {
    // State unchanged — still honour vault TTL refresh
    await refreshVault(gs, `${source}-ttl`);
  }
}

// ─── WEBSOCKET SUBSCRIPTIONS ───────────────────────────────────────────────
let stateSubId       = null;
let vaultSubId       = null;
let wsReconnectTimer = null;

async function subscribeAccounts() {
  if (wsReconnectTimer) { clearTimeout(wsReconnectTimer); wsReconnectTimer = null; }

  try {
    // Subscribe to state account — fires on every write to the game state
    stateSubId = connection.onAccountChange(
      statePk,
      (accountInfo) => {
        handleStateData(accountInfo.data, "ws-state").catch(console.error);
      },
      "confirmed"
    );

    // Subscribe to vault token account — fires whenever the balance moves
    vaultSubId = connection.onAccountChange(
      vaultPk,
      async () => {
        // Re-read state too so sessionPlays is current when we compute vault delta
        try {
          const acc = await connection.getAccountInfo(statePk, "confirmed");
          if (acc?.data) await handleStateData(acc.data, "ws-vault");
        } catch (e) {
          console.warn("[WS-vault] state re-read failed:", e.message);
        }
      },
      "confirmed"
    );

    console.log(`[WS] subscribed — stateSubId=${stateSubId}  vaultSubId=${vaultSubId}`);
  } catch (e) {
    console.error("[WS] subscribe error:", e.message);
    scheduleWsReconnect();
  }
}

function scheduleWsReconnect(delayMs = 5000) {
  if (wsReconnectTimer) return;
  console.log(`[WS] scheduling reconnect in ${delayMs}ms`);
  wsReconnectTimer = setTimeout(async () => {
    wsReconnectTimer = null;
    try {
      if (stateSubId != null) {
        await connection.removeAccountChangeListener(stateSubId);
        stateSubId = null;
      }
      if (vaultSubId != null) {
        await connection.removeAccountChangeListener(vaultSubId);
        vaultSubId = null;
      }
    } catch {}
    await subscribeAccounts();
  }, delayMs);
}

// ─── SAFETY POLL ───────────────────────────────────────────────────────────
// Runs every POLL_MS (default 1 s) as a catch-all for missed WS messages.
// handleStateData() is a no-op when nothing changed, so this is very cheap.
async function safetyPoll() {
  try {
    const acc = await connection.getAccountInfo(statePk, "confirmed");
    if (acc?.data) await handleStateData(acc.data, "poll");
  } catch (e) {
    console.warn("[Poll] error:", e.message);
  } finally {
    setTimeout(safetyPoll, POLL_MS);
  }
}

// ─── HTTP / SSE SERVER ─────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  const url = new URL(req.url, "http://localhost");

  // ── GET /events  (SSE stream) ──────────────────────────────────────────
  if (url.pathname === "/events" && req.method === "GET") {
    res.writeHead(200, {
      "Content-Type":      "text/event-stream",
      "Cache-Control":     "no-cache",
      "Connection":        "keep-alive",
      "X-Accel-Buffering": "no",   // disable nginx buffering
    });
    res.write(": connected\n\n");

    // Replay last known state immediately so new clients don't wait
    if (latestPayload) res.write(latestPayload);

    clients.add(res);
    console.log(`[SSE] +client  total=${clients.size}`);

    // Heartbeat every 25 s to keep proxies from closing idle connections
    const hb = setInterval(() => {
      try { res.write(": ping\n\n"); }
      catch { clearInterval(hb); clients.delete(res); }
    }, 25_000);

    req.on("close", () => {
      clearInterval(hb);
      clients.delete(res);
      console.log(`[SSE] -client  total=${clients.size}`);
    });
    return;
  }

  // ── GET /state  (one-shot JSON snapshot) ──────────────────────────────
  if (url.pathname === "/state" && req.method === "GET") {
    if (!latestPayload) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "State not yet available, retry in 1s" }));
      return;
    }
    const payload = latestPayload.replace(/^data: /, "").trim();
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(payload);
    return;
  }

  // ── GET /health ────────────────────────────────────────────────────────
  if (url.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      ok:           true,
      clients:      clients.size,
      wsSubscribed: stateSubId != null,
      lastStateSig,
      uptime:       Math.floor(process.uptime()),
    }));
    return;
  }

  res.writeHead(404); res.end("Not found");
});

// ─── BOOT ──────────────────────────────────────────────────────────────────
server.listen(PORT, async () => {
  console.log(`\nRelay server ready`);
  console.log(`  HTTP : http://0.0.0.0:${PORT}`);
  console.log(`  RPC  : ${RPC_URL}`);
  console.log(`  WS   : ${WS_URL}\n`);

  // Seed initial state before any subscription fires
  try {
    const acc = await connection.getAccountInfo(statePk, "confirmed");
    if (acc?.data) await handleStateData(acc.data, "boot");
    console.log("[Boot] initial state loaded");
  } catch (e) {
    console.warn("[Boot] initial state read failed:", e.message);
  }

  // Primary: WebSocket account subscriptions (sub-second latency)
  await subscribeAccounts();

  // Secondary: 1-second safety poll (catches any dropped WS notifications)
  setTimeout(safetyPoll, POLL_MS);
});
