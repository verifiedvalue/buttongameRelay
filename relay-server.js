/**
 * Button Game – Relay Server  (WebSocket subscription edition)
 *
 * UPDATE:
 *  - Durable Claim Log persisted to Railway Postgres.
 *  - Username system tied to wallet address via signature:
 *      GET  /challenge?address=...
 *      POST /username  { address, username, signature, nonce }
 *      GET  /usernames?addresses=a,b,c
 *
 * SSE:
 *  - state messages include `names` mapping for any addresses in the payload
 *  - claim messages include `names`
 *  - username updates broadcast as { type:"username", address, username }
 *
 * Env:
 *   CLAIM_DB         – "postgres" | "none"    (default: "postgres")
 *   CLAIM_LOG_MAX    – max claims kept in memory (default: 200)
 *   CLAIM_DB_URL     – optional override; else DATABASE_PRIVATE_URL or DATABASE_URL
 *   USERNAME_MAX_LEN – default 18
 *   USERNAME_MIN_LEN – default 3
 *   CHALLENGE_TTL_MS – default 5 minutes
 *   Rate limits (per IP per minute): RATE_LIMIT_STATE, RATE_LIMIT_CHALLENGE,
 *   RATE_LIMIT_USERNAME_AVAILABLE, RATE_LIMIT_USERNAMES, RATE_LIMIT_CLAIMS,
 *   RATE_LIMIT_HEALTH, RATE_LIMIT_USERNAME_POST; RATE_LIMIT_SSE_CONCURRENT = max SSE connections per IP.
 */

"use strict";

const http = require("http");
const { Connection, PublicKey } = require("@solana/web3.js");
const { Pool } = require("pg");
const nacl = require("tweetnacl");
const bs58 = require("bs58");

// ─── CONFIG ────────────────────────────────────────────────────────────────
const PORT    = Number(process.env.PORT || 3001);
const CLUSTER = process.env.CLUSTER || "devnet";
const POLL_MS = Number(process.env.POLL_MS || 1000);
const SSE_HEARTBEAT_MS = Number(process.env.SSE_HEARTBEAT_MS || 15000);
const SSE_RETRY_MS = Number(process.env.SSE_RETRY_MS || 3000);

const CLAIM_LOG_MAX = Number(process.env.CLAIM_LOG_MAX || 200);

const USERNAME_MIN_LEN = Number(process.env.USERNAME_MIN_LEN || 3);
const USERNAME_MAX_LEN = Number(process.env.USERNAME_MAX_LEN || 18);
const CHALLENGE_TTL_MS = Number(process.env.CHALLENGE_TTL_MS || 5 * 60 * 1000);

// Rate limits (per IP) — generous for normal use, enough to curb abuse
const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const RATE_LIMITS = {
  "/state":               Number(process.env.RATE_LIMIT_STATE || 60),
  "/wallet-state":        Number(process.env.RATE_LIMIT_WALLET_STATE || 120),
  "/tx-context":          Number(process.env.RATE_LIMIT_TX_CONTEXT || 120),
  "/tx-status":           Number(process.env.RATE_LIMIT_TX_STATUS || 180),
  "/challenge":           Number(process.env.RATE_LIMIT_CHALLENGE || 20),
  "/username/available":  Number(process.env.RATE_LIMIT_USERNAME_AVAILABLE || 30),
  "/usernames":           Number(process.env.RATE_LIMIT_USERNAMES || 60),
  "/claims":              Number(process.env.RATE_LIMIT_CLAIMS || 60),
  "/health":              Number(process.env.RATE_LIMIT_HEALTH || 120),
  "POST:/username":      Number(process.env.RATE_LIMIT_USERNAME_POST || 10),
};
const RATE_LIMIT_SSE_MAX_CONCURRENT = Number(process.env.RATE_LIMIT_SSE_CONCURRENT || 10);

const CLAIM_DB = (process.env.CLAIM_DB || "postgres").toLowerCase();
const CLAIM_DB_URL =
  process.env.CLAIM_DB_URL ||
  process.env.DATABASE_PRIVATE_URL ||
  process.env.DATABASE_URL ||
  "";

const DEFAULT_HTTP = CLUSTER === "mainnet-beta"
  ? "https://api.mainnet-beta.solana.com"
  : "https://api.devnet.solana.com";

const DEFAULT_WS = CLUSTER === "mainnet-beta"
  ? "wss://api.mainnet-beta.solana.com"
  : "wss://api.devnet.solana.com";

const RPC_URL = process.env.RPC_URL || DEFAULT_HTTP;
const WS_URL  = process.env.WS_URL  || DEFAULT_WS;
const GAME_MINT_ADDR = process.env.GAME_MINT || "";
const WALLET_STATE_TTL_MS = Number(process.env.WALLET_STATE_TTL_MS || 2500);
const TX_CONTEXT_TTL_MS = Number(process.env.TX_CONTEXT_TTL_MS || 12000);

const STATE_ADDR = "5sfJLUePwpDJuxUY9X2cW8DKafq7bqxrQ6XD61tWUvQr";
const VAULT_ADDR = "2436ZcMWA61as89tT99RURppUpT7CjkDoFHmwskwStSa";
const TOKEN_PROGRAM_ID = new PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const ASSOCIATED_TOKEN_PROGRAM_ID = new PublicKey("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

const VAULT_TTL_MS = 30_000;

// ─── POSTGRES ──────────────────────────────────────────────────────────────
let pgPool = null;
let pgReady = false;

function pgEnabled() {
  return CLAIM_DB === "postgres" && !!CLAIM_DB_URL;
}

async function initPg() {
  if (!pgEnabled()) return;

  pgPool = new Pool({
    connectionString: CLAIM_DB_URL,
    ssl: { rejectUnauthorized: false },
    max: 3,
  });

  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS claims (
      id TEXT PRIMARY KEY,
      detected_at TIMESTAMPTZ NOT NULL,
      detected_via TEXT,
      winner TEXT,
      session_plays INTEGER,
      timer_end BIGINT,
      cooldown_end BIGINT,
      vault_before TEXT,
      vault_after TEXT,
      amount TEXT,
      note TEXT
    );
  `);

  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS usernames (
      address TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pgPool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS usernames_username_lower_key
    ON usernames (LOWER(TRIM(username)));
  `);

  pgReady = true;
  console.log("[DB] Postgres ready (claims + usernames tables ensured)");
}

async function loadRecentClaims(limit = CLAIM_LOG_MAX) {
  if (!pgEnabled() || !pgReady) return [];
  const { rows } = await pgPool.query(
    `SELECT *
     FROM claims
     ORDER BY detected_at DESC
     LIMIT $1`,
    [limit]
  );
  return rows.reverse().map((r) => ({
    id: r.id,
    detectedAtMs: new Date(r.detected_at).getTime(),
    detectedVia: r.detected_via,
    winner: r.winner,
    sessionPlays: r.session_plays,
    timerEnd: Number(r.timer_end),
    cooldownEnd: Number(r.cooldown_end),
    vaultBefore: r.vault_before,
    vaultAfter: r.vault_after,
    amount: r.amount,
    note: r.note,
  }));
}

async function persistClaim(entry) {
  if (!pgEnabled() || !pgReady) return;
  try {
    await pgPool.query(
      `INSERT INTO claims (
        id, detected_at, detected_via, winner, session_plays,
        timer_end, cooldown_end, vault_before, vault_after, amount, note
      ) VALUES (
        $1, to_timestamp($2 / 1000.0), $3, $4, $5,
        $6, $7, $8, $9, $10, $11
      )
      ON CONFLICT (id) DO NOTHING;`,
      [
        entry.id,
        entry.detectedAtMs,
        entry.detectedVia,
        entry.winner,
        entry.sessionPlays ?? null,
        entry.timerEnd ?? null,
        entry.cooldownEnd ?? null,
        entry.vaultBefore ?? null,
        entry.vaultAfter ?? null,
        entry.amount ?? null,
        entry.note ?? null,
      ]
    );
  } catch (e) {
    console.warn("[DB] persistClaim failed:", e.message);
  }
}

// ─── USERNAMES (cache + DB) ───────────────────────────────────────────────
const usernameCache = new Map(); // address -> username
const usernameCacheLoadedAtMs = { v: 0 };

// Load all usernames (small scale), or you can replace with batch fetch per address.
async function loadAllUsernames() {
  if (!pgEnabled() || !pgReady) return;
  const { rows } = await pgPool.query(`SELECT address, username FROM usernames`);
  usernameCache.clear();
  for (const r of rows) usernameCache.set(r.address, r.username);
  usernameCacheLoadedAtMs.v = Date.now();
  console.log(`[DB] loaded ${rows.length} usernames into cache`);
}

const USERNAME_TAKEN_ERR = "Username already taken";

async function upsertUsername(address, username) {
  const norm = (s) => String(s || "").trim().toLowerCase();
  const want = norm(username);

  if (!pgEnabled() || !pgReady) {
    // In-memory: enforce no duplicate (same process)
    for (const [addr, un] of usernameCache) {
      if (addr !== address && norm(un) === want) throw new Error(USERNAME_TAKEN_ERR);
    }
    usernameCache.set(address, username);
    return;
  }

  // Postgres: run duplicate check + upsert in a transaction to avoid races
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query(
      `SELECT address FROM usernames WHERE LOWER(TRIM(username)) = $1`,
      [want]
    );
    const takenByOther = rows.some((r) => r.address !== address);
    if (takenByOther) {
      await client.query("ROLLBACK");
      throw new Error(USERNAME_TAKEN_ERR);
    }
    await client.query(
      `INSERT INTO usernames (address, username, updated_at)
       VALUES ($1, $2, now())
       ON CONFLICT (address)
       DO UPDATE SET username = EXCLUDED.username, updated_at = now();`,
      [address, username]
    );
    await client.query("COMMIT");
    usernameCache.set(address, username);
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

function validateUsername(u) {
  if (typeof u !== "string") return { ok: false, reason: "username must be a string" };
  const username = u.trim();

  if (username.length < USERNAME_MIN_LEN) return { ok: false, reason: `min length ${USERNAME_MIN_LEN}` };
  if (username.length > USERNAME_MAX_LEN) return { ok: false, reason: `max length ${USERNAME_MAX_LEN}` };

  // Allowed: letters, numbers, underscore, dash, dot
  if (!/^[a-zA-Z0-9_.-]+$/.test(username)) {
    return { ok: false, reason: "only letters, numbers, _, -, . allowed" };
  }

  return { ok: true, username };
}

function getName(address) {
  return usernameCache.get(address) || null;
}

/** Returns true if username is taken by another address (case-insensitive). Pass exceptAddress to allow that address to keep/reuse it. */
async function isUsernameTaken(username, exceptAddress = "") {
  const norm = (s) => String(s || "").trim().toLowerCase();
  const want = norm(username);
  if (!want) return false;
  if (pgEnabled() && pgReady) {
    const { rows } = await pgPool.query(
      `SELECT address FROM usernames WHERE LOWER(TRIM(username)) = $1`,
      [want]
    );
    if (rows.length === 0) return false;
    if (exceptAddress && rows.some((r) => r.address === exceptAddress)) return false;
    return true;
  }
  for (const [addr, un] of usernameCache) {
    if (addr === exceptAddress) continue;
    if (norm(un) === want) return true;
  }
  return false;
}

function namesForAddresses(addresses) {
  const out = {};
  for (const a of addresses) {
    const n = getName(a);
    if (n) out[a] = n;
  }
  return out;
}

// ─── SIGNED CHALLENGES ─────────────────────────────────────────────────────
const challenges = new Map(); // address -> { nonce, expiresAtMs }

function makeNonce() {
  // fast nonce (good enough for auth challenge)
  return `${Date.now()}-${Math.random().toString(16).slice(2)}-${Math.random().toString(16).slice(2)}`;
}

function setChallenge(address) {
  const nonce = makeNonce();
  const expiresAtMs = Date.now() + CHALLENGE_TTL_MS;
  challenges.set(address, { nonce, expiresAtMs });
  return { nonce, expiresAtMs };
}

function getChallenge(address) {
  const c = challenges.get(address);
  if (!c) return null;
  if (Date.now() > c.expiresAtMs) {
    challenges.delete(address);
    return null;
  }
  return c;
}

function buildUsernameMessage(address, username, nonce) {
  // IMPORTANT: client must sign EXACTLY this string (UTF-8)
  return [
    "ButtonGame Username Set",
    `Address: ${address}`,
    `Username: ${username}`,
    `Nonce: ${nonce}`,
  ].join("\n");
}

function verifyUsernameSignature({ address, username, signature, nonce }) {
  // Validate pubkey
  let pk;
  try { pk = new PublicKey(address); }
  catch { return { ok: false, reason: "invalid address" }; }

  // Validate nonce
  const c = getChallenge(address);
  if (!c) return { ok: false, reason: "no active challenge (or expired). call /challenge first" };
  if (c.nonce !== nonce) return { ok: false, reason: "nonce mismatch" };

  // Decode signature
  let sigBytes;
  try { sigBytes = bs58.decode(signature); }
  catch { return { ok: false, reason: "invalid signature encoding" }; }

  const msg = buildUsernameMessage(address, username, nonce);
  const msgBytes = new TextEncoder().encode(msg);
  const pubBytes = pk.toBytes();

  const ok = nacl.sign.detached.verify(msgBytes, sigBytes, pubBytes);
  if (!ok) return { ok: false, reason: "signature verification failed" };

  // One-time challenge (prevents replay)
  challenges.delete(address);

  return { ok: true };
}

// ─── SOLANA ────────────────────────────────────────────────────────────────
const connection = new Connection(RPC_URL, {
  commitment: "confirmed",
  wsEndpoint: WS_URL,
});
const statePk = new PublicKey(STATE_ADDR);
const vaultPk = new PublicKey(VAULT_ADDR);

// ─── DECODE STATE ──────────────────────────────────────────────────────────
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
  const data = accountData.slice(8);
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
let latestPayload      = null;
let latestSnapshot     = null;
let lastStateSig       = "";
let lastPlaysSeen      = null;
let lastVaultFetchMs   = 0;
let vaultFetchInFlight = false;
const clients          = new Set();
let stateVersion       = 0;
let sseEventId         = 0;
const recentEvents     = [];
const RECENT_EVENTS_MAX = Number(process.env.SSE_RECENT_EVENTS_MAX || 300);

let lastGs = null;
let lastVaultAmountStr = null;

const claimLog = [];
let pendingClaim = null;
const walletStateCache = new Map(); // address -> { expiresAtMs, data }
let txContextCache = null; // { blockhash, lastValidBlockHeight, fetchedAtMs }
let txContextInFlight = null;

async function findClaimTxSignature(detectedAtMs) {
  try {
    const limit = 20;
    const sigInfos = await connection.getSignaturesForAddress(statePk, { limit });
    if (!Array.isArray(sigInfos) || sigInfos.length === 0) return null;

    const targetSec = Math.floor((detectedAtMs ?? Date.now()) / 1000);
    let best = null;
    let bestDiff = Infinity;

    for (const info of sigInfos) {
      if (!info.blockTime) continue;
      const diff = Math.abs(info.blockTime - targetSec);
      if (diff < bestDiff) {
        best = info;
        bestDiff = diff;
      }
    }

    const chosen = best || sigInfos[0];
    return chosen && chosen.signature ? chosen.signature : null;
  } catch (e) {
    console.warn("[ClaimTx] lookup failed:", e.message);
    return null;
  }
}

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

function computeCanonicalPhase(gs, nowSec) {
  if (!gs?.enabled) return "DISABLED";
  if (gs.timerEnd === 0) return "IDLE";
  if (!gs.unclaimed) return "IDLE";
  if (nowSec < gs.timerEnd) return "ACTIVE";
  if (nowSec <= gs.cooldownEnd) return "COOLDOWN";
  return "POST_COOLDOWN_UNCLAIMED";
}

function makeStateEvent(gs, vaultAmount = null) {
  const nowSec = Math.floor(Date.now() / 1000);
  const event = {
    type: "state",
    serverTs: nowSec,
    stateVersion,
    phase: computeCanonicalPhase(gs, nowSec),
    state: gs,
  };
  if (vaultAmount != null) {
    event.vaultAmount = vaultAmount;
    const vb = safeBigInt(vaultAmount);
    const divisor = gs.sessionPlays < 250 ? 40n : (gs.sessionPlays < 1000 ? 10n : 5n);
    if (vb != null && divisor > 0n) event.potAmount = (vb / divisor).toString();
  }
  return event;
}

function broadcast(data) {
  // Attach names mapping to any message that has addresses
  // (so clients can render usernames everywhere without extra calls)
  const enriched = enrichWithNames(data);

  const id = ++sseEventId;
  const msg = `id: ${id}\ndata: ${JSON.stringify(enriched)}\n\n`;
  latestPayload = msg;
  if (enriched?.type === "state") latestSnapshot = enriched;
  recentEvents.push({ id, msg });
  while (recentEvents.length > RECENT_EVENTS_MAX) recentEvents.shift();
  for (const res of [...clients]) {
    try {
      if (!res.writableEnded) res.write(msg);
      else {
        sseConnectionRemove(res);
        clients.delete(res);
      }
    } catch {
      sseConnectionRemove(res);
      clients.delete(res);
    }
  }
}

function safeBigInt(s) {
  try { return BigInt(s); } catch { return null; }
}

function findAta(ownerPk, mintPk) {
  const [ata] = PublicKey.findProgramAddressSync(
    [ownerPk.toBuffer(), TOKEN_PROGRAM_ID.toBuffer(), mintPk.toBuffer()],
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  return ata;
}

async function getTxContext(force = false) {
  const now = Date.now();
  if (!force && txContextCache && (now - txContextCache.fetchedAtMs) < TX_CONTEXT_TTL_MS) {
    return txContextCache;
  }
  if (txContextInFlight) return txContextInFlight;
  const run = (async () => {
    const latest = await connection.getLatestBlockhash("processed");
    const out = {
      blockhash: latest.blockhash,
      lastValidBlockHeight: latest.lastValidBlockHeight,
      fetchedAtMs: Date.now(),
    };
    txContextCache = out;
    return out;
  })();
  txContextInFlight = run;
  try {
    return await run;
  } finally {
    txContextInFlight = null;
  }
}

// Collect addresses from payload to attach `{ names: { address: username } }`
function enrichWithNames(payload) {
  const addrs = new Set();

  // state payload
  if (payload?.state) {
    const s = payload.state;
    if (s.owner) addrs.add(s.owner);
    if (s.tokenMint) addrs.add(s.tokenMint);
    if (s.vault) addrs.add(s.vault);
    if (s.currentWinner) addrs.add(s.currentWinner);
    if (s.currentWinnerAta) addrs.add(s.currentWinnerAta);
  }

  // claim payload
  if (payload?.claim) {
    const c = payload.claim;
    if (c.winner) addrs.add(c.winner);
  }

  // username payload
  if (payload?.address) addrs.add(payload.address);

  const names = namesForAddresses([...addrs]);
  if (Object.keys(names).length) return { ...payload, names };
  return payload;
}

function pushClaim(entry) {
  claimLog.push(entry);
  while (claimLog.length > CLAIM_LOG_MAX) claimLog.shift();

  persistClaim(entry).catch(() => {});

  broadcast({
    type: "claim",
    serverTs: Math.floor(Date.now() / 1000),
    claim: entry,
  });
}

function detectClaimTransition(gs, source) {
  if (!lastGs) return;
  const transitioned = (lastGs.unclaimed === true && gs.unclaimed === false);
  if (!transitioned) return;

  if (pendingClaim) {
    const entry = {
      ...pendingClaim,
      amount: pendingClaim.amount ?? null,
      note: (pendingClaim.note ? pendingClaim.note + " | " : "") + "superseded-by-new-pending",
    };
    pushClaim(entry);
  }

  pendingClaim = {
    id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
    detectedAtMs: Date.now(),
    detectedVia: source,
    winner: gs.currentWinner,
    sessionPlays: gs.sessionPlays,
    timerEnd: gs.timerEnd,
    cooldownEnd: gs.cooldownEnd,
    vaultBefore: lastVaultAmountStr,
    vaultAfter: null,
    amount: null,
    note: null,
    tx: null,
  };
}

async function refreshVault(gs, reason) {
  if (vaultFetchInFlight) return;
  const nowMs      = Date.now();
  const plays      = gs.sessionPlays;
  const changed    = lastPlaysSeen !== null && plays !== lastPlaysSeen;
  const ttlExpired = (nowMs - lastVaultFetchMs) >= VAULT_TTL_MS;
  const never      = lastVaultFetchMs === 0;

  if (!never && !changed && !ttlExpired) return;

  vaultFetchInFlight = true;
  try {
    const bal = await connection.getTokenAccountBalance(vaultPk, "confirmed");
    const vaultAmount = bal?.value?.amount || "0";

    lastVaultAmountStr = vaultAmount;
    lastVaultFetchMs = Date.now();
    lastPlaysSeen    = plays;

    if (pendingClaim) {
      pendingClaim.vaultAfter = vaultAmount;

      if (!pendingClaim.tx) {
        pendingClaim.tx = await findClaimTxSignature(pendingClaim.detectedAtMs);
      }

      const beforeBI = safeBigInt(pendingClaim.vaultBefore);
      const afterBI  = safeBigInt(pendingClaim.vaultAfter);

      if (beforeBI != null && afterBI != null) {
        const delta = beforeBI - afterBI;
        pendingClaim.amount = delta > 0n ? delta.toString() : "0";
        if (delta <= 0n) pendingClaim.note = `vault-delta-nonpositive(${delta.toString()})`;
      } else {
        pendingClaim.amount = null;
        pendingClaim.note = "missing-vault-before-or-after";
      }

      pushClaim({ ...pendingClaim });
      pendingClaim = null;
    }

    broadcast(makeStateEvent(gs, vaultAmount));
  } catch (e) {
    console.warn(`[Vault] fetch failed (${reason}):`, e.message);
    if (pendingClaim) {
      pendingClaim.note = (pendingClaim.note ? pendingClaim.note + " | " : "") + `vault-fetch-failed(${reason})`;
      pushClaim({ ...pendingClaim });
      pendingClaim = null;
    }
  } finally {
    vaultFetchInFlight = false;
  }
}

async function handleStateData(rawData, source) {
  let gs;
  try { gs = decodeState(rawData); }
  catch (e) { console.error("[Decode] failed:", e.message); return; }

  const sig     = stateSignature(gs);
  const changed = sig !== lastStateSig;

  if (changed) detectClaimTransition(gs, source);

  if (changed) {
    stateVersion += 1;
    lastStateSig = sig;
    console.log(`[State] changed via ${source} — plays=${gs.sessionPlays} timerEnd=${gs.timerEnd}`);

    broadcast(makeStateEvent(gs));
    await refreshVault(gs, source);
  } else {
    await refreshVault(gs, `${source}-ttl`);
  }

  lastGs = gs;
}

// ─── WEBSOCKET SUBSCRIPTIONS ───────────────────────────────────────────────
let stateSubId       = null;
let vaultSubId       = null;
let wsReconnectTimer = null;
let wsReconnectAttempts = 0;
let pollErrorStreak = 0;

async function subscribeAccounts() {
  if (wsReconnectTimer) { clearTimeout(wsReconnectTimer); wsReconnectTimer = null; }

  try {
    stateSubId = connection.onAccountChange(
      statePk,
      (accountInfo) => {
        handleStateData(accountInfo.data, "ws-state").catch(console.error);
      },
      "confirmed"
    );

    vaultSubId = connection.onAccountChange(
      vaultPk,
      async () => {
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
    wsReconnectAttempts = 0;
  } catch (e) {
    console.error("[WS] subscribe error:", e.message);
    scheduleWsReconnect("subscribe-error");
  }
}

function scheduleWsReconnect(reason = "unspecified") {
  if (wsReconnectTimer) return;
  wsReconnectAttempts += 1;
  const base = Math.min(60_000, 1000 * (2 ** Math.min(wsReconnectAttempts, 6)));
  const jitter = Math.floor(Math.random() * 750);
  const delayMs = base + jitter;
  console.log(`[WS] scheduling reconnect in ${delayMs}ms (attempt=${wsReconnectAttempts}, reason=${reason})`);
  wsReconnectTimer = setTimeout(async () => {
    wsReconnectTimer = null;
    try {
      if (stateSubId != null) { await connection.removeAccountChangeListener(stateSubId); stateSubId = null; }
      if (vaultSubId != null) { await connection.removeAccountChangeListener(vaultSubId); vaultSubId = null; }
    } catch {}
    await subscribeAccounts();
  }, delayMs);
}

// ─── SAFETY POLL ───────────────────────────────────────────────────────────
async function safetyPoll() {
  try {
    const acc = await connection.getAccountInfo(statePk, "confirmed");
    if (acc?.data) await handleStateData(acc.data, "poll");
    pollErrorStreak = 0;
  } catch (e) {
    console.warn("[Poll] error:", e.message);
    pollErrorStreak += 1;
    if (pollErrorStreak >= 3) {
      scheduleWsReconnect("poll-error-streak");
      pollErrorStreak = 0;
    }
  } finally {
    setTimeout(safetyPoll, POLL_MS);
  }
}

// ─── HELPERS: JSON body ────────────────────────────────────────────────────
function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 1_000_000) { // 1MB limit
        reject(new Error("body too large"));
        req.destroy();
      }
    });
    req.on("end", () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (e) {
        reject(new Error("invalid json"));
      }
    });
  });
}

// ─── RATE LIMITING ─────────────────────────────────────────────────────────
function getClientIp(req) {
  const forwarded = req.headers["x-forwarded-for"];
  if (forwarded) {
    const first = typeof forwarded === "string" ? forwarded.split(",")[0] : forwarded[0];
    return (first && first.trim()) || req.socket?.remoteAddress || "unknown";
  }
  return req.socket?.remoteAddress || "unknown";
}

const rateLimitBuckets = new Map(); // path -> Map(ip -> { count, resetAt })
function checkRateLimit(ip, pathKey) {
  const max = RATE_LIMITS[pathKey];
  if (!max || max <= 0) return { limited: false };
  let byIp = rateLimitBuckets.get(pathKey);
  if (!byIp) {
    byIp = new Map();
    rateLimitBuckets.set(pathKey, byIp);
  }
  const now = Date.now();
  for (const [k, v] of byIp) {
    if (now >= v.resetAt) byIp.delete(k);
  }
  let entry = byIp.get(ip);
  if (!entry || now >= entry.resetAt) {
    entry = { count: 0, resetAt: now + RATE_LIMIT_WINDOW_MS };
    byIp.set(ip, entry);
  }
  entry.count++;
  if (entry.count > max) {
    return { limited: true, retryAfterSec: Math.ceil((entry.resetAt - now) / 1000) };
  }
  return { limited: false };
}

const sseClientIp = new Map(); // res -> ip (for SSE connections)
const sseCountByIp = new Map(); // ip -> number of active SSE connections
function sseConnectionCount(ip) {
  return sseCountByIp.get(ip) || 0;
}
function sseConnectionAdd(res, ip) {
  sseClientIp.set(res, ip);
  sseCountByIp.set(ip, (sseCountByIp.get(ip) || 0) + 1);
}
function sseConnectionRemove(res) {
  const ip = sseClientIp.get(res);
  sseClientIp.delete(res);
  if (ip) {
    const n = (sseCountByIp.get(ip) || 1) - 1;
    if (n <= 0) sseCountByIp.delete(ip);
    else sseCountByIp.set(ip, n);
  }
}

function sendRateLimitResponse(res, retryAfterSec) {
  res.writeHead(429, {
    "Content-Type":   "application/json",
    "Retry-After":    String(Math.max(1, retryAfterSec || 60)),
  });
  res.end(JSON.stringify({ error: "Too many requests", retryAfterSec: retryAfterSec || 60 }));
}

// ─── HTTP / SSE SERVER ─────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  const url = new URL(req.url, "http://localhost");
  const ip = getClientIp(req);

  // Rate limit: per-path checks (skip for /events until we know we allow the connection)
  const pathKey = req.method === "POST" && url.pathname === "/username" ? "POST:/username" : url.pathname;
  if (pathKey !== "/events") {
    const rl = checkRateLimit(ip, pathKey);
    if (rl.limited) {
      sendRateLimitResponse(res, rl.retryAfterSec);
      return;
    }
  }

  // SSE — limit concurrent connections per IP (multiple tabs ok, abuse not)
  if (url.pathname === "/events" && req.method === "GET") {
    if (sseConnectionCount(ip) >= RATE_LIMIT_SSE_MAX_CONCURRENT) {
      sendRateLimitResponse(res, 60);
      return;
    }
    res.writeHead(200, {
      "Content-Type":      "text/event-stream",
      "Cache-Control":     "no-cache, no-transform",
      "Connection":        "keep-alive",
      "X-Accel-Buffering": "no",
    });
    try {
      req.socket?.setNoDelay?.(true);
      req.socket?.setKeepAlive?.(true, 60_000);
    } catch {}
    if (typeof res.flushHeaders === "function") {
      try { res.flushHeaders(); } catch {}
    }
    res.write(`retry: ${SSE_RETRY_MS}\n`);
    res.write(": connected\n\n");
    const lastEventIdRaw = req.headers["last-event-id"];
    const lastEventId = Number(Array.isArray(lastEventIdRaw) ? lastEventIdRaw[0] : lastEventIdRaw);
    if (Number.isFinite(lastEventId) && lastEventId > 0) {
      for (const ev of recentEvents) {
        if (ev.id > lastEventId) {
          try { res.write(ev.msg); } catch {}
        }
      }
    } else if (latestPayload) {
      res.write(latestPayload);
    }

    sseConnectionAdd(res, ip);
    clients.add(res);
    console.log(`[SSE] +client  total=${clients.size}`);

    let cleaned = false;
    const cleanup = () => {
      if (cleaned) return;
      cleaned = true;
      clearInterval(hb);
      sseConnectionRemove(res);
      clients.delete(res);
      console.log(`[SSE] -client  total=${clients.size}`);
    };

    const hb = setInterval(() => {
      try {
        if (res.writableEnded) { cleanup(); return; }
        res.write(`: ping ${Date.now()}\n\n`);
      } catch {
        cleanup();
      }
    }, SSE_HEARTBEAT_MS);

    req.on("close", cleanup);
    req.on("error", cleanup);
    res.on("close", cleanup);
    res.on("error", cleanup);
    return;
  }

  // one-shot state snapshot
  if (url.pathname === "/state" && req.method === "GET") {
    if (!latestSnapshot) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "State not yet available, retry in 1s" }));
      return;
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(latestSnapshot));
    return;
  }

  // claim log
  if (url.pathname === "/claims" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      max: CLAIM_LOG_MAX,
      count: claimLog.length,
      claims: claimLog,
      persisted: pgEnabled() && pgReady,
    }));
    return;
  }

  // wallet state (ATA + balance), cached briefly to reduce wallet-switch RPC bursts
  if (url.pathname === "/wallet-state" && req.method === "GET") {
    const address = (url.searchParams.get("address") || "").trim();
    let ownerPk;
    try { ownerPk = new PublicKey(address); } catch {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "invalid address" }));
      return;
    }

    const nowMs = Date.now();
    const cached = walletStateCache.get(address);
    if (cached && cached.expiresAtMs > nowMs) {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ...cached.data, fromCache: true }));
      return;
    }

    const mint58 = latestSnapshot?.state?.tokenMint || GAME_MINT_ADDR;
    if (!mint58) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "mint unavailable" }));
      return;
    }

    let mintPk;
    try { mintPk = new PublicKey(mint58); } catch {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "invalid relay mint config" }));
      return;
    }

    try {
      const ata = findAta(ownerPk, mintPk);
      const info = await connection.getAccountInfo(ata, "confirmed");
      const ataExists = !!info;
      let balanceRaw = "0";
      if (ataExists) {
        const bal = await connection.getTokenAccountBalance(ata, "confirmed");
        balanceRaw = bal?.value?.amount || "0";
      }
      const payload = {
        address,
        ata: ata.toBase58(),
        ataExists,
        balanceRaw,
        mint: mintPk.toBase58(),
        cachedAtMs: nowMs,
      };
      walletStateCache.set(address, { expiresAtMs: nowMs + WALLET_STATE_TTL_MS, data: payload });
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ...payload, fromCache: false }));
      return;
    } catch (e) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: `wallet-state unavailable: ${e.message}` }));
      return;
    }
  }

  // tx context for frontend signing flow; relay caches this to avoid client RPC storms.
  if (url.pathname === "/tx-context" && req.method === "GET") {
    try {
      const ctx = await getTxContext(false);
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify({
        blockhash: ctx.blockhash,
        lastValidBlockHeight: ctx.lastValidBlockHeight,
        fetchedAtMs: ctx.fetchedAtMs,
        serverTs: Math.floor(Date.now() / 1000),
        stateVersion,
      }));
      return;
    } catch (e) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: `tx-context unavailable: ${e.message}` }));
      return;
    }
  }

  // tx status via relay (frontends avoid direct RPC status polling under load).
  if (url.pathname === "/tx-status" && req.method === "GET") {
    const sig = (url.searchParams.get("sig") || "").trim();
    if (!sig) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "missing sig" }));
      return;
    }
    try {
      const out = await connection.getSignatureStatuses([sig], { searchTransactionHistory: true });
      const st = out?.value?.[0] || null;
      const confirmationStatus = st?.confirmationStatus || null;
      const err = st?.err ?? null;
      const done = !!st && (confirmationStatus === "confirmed" || confirmationStatus === "finalized" || err != null);
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify({
        sig,
        found: !!st,
        done,
        confirmationStatus,
        err,
      }));
      return;
    } catch (e) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: `tx-status unavailable: ${e.message}` }));
      return;
    }
  }

  // ── USERNAMES ──────────────────────────────────────────────────────────

  // GET /challenge?address=...
  if (url.pathname === "/challenge" && req.method === "GET") {
    const address = (url.searchParams.get("address") || "").trim();
    try { new PublicKey(address); } catch {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "invalid address" }));
      return;
    }
    const { nonce, expiresAtMs } = setChallenge(address);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      address,
      nonce,
      expiresAtMs,
      messageToSign: buildUsernameMessage(address, "<username>", nonce),
      ttlMs: CHALLENGE_TTL_MS,
    }));
    return;
  }

  // GET /username/available?username=xyz&address=optional — check if username is available (not taken by another address)
  if (url.pathname === "/username/available" && req.method === "GET") {
    const username = (url.searchParams.get("username") || "").trim();
    const address = (url.searchParams.get("address") || "").trim();
    const taken = await isUsernameTaken(username, address);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ available: !taken }));
    return;
  }

  // GET /usernames?addresses=a,b,c
  if (url.pathname === "/usernames" && req.method === "GET") {
    const raw = (url.searchParams.get("addresses") || "").trim();
    const addresses = raw ? raw.split(",").map(s => s.trim()).filter(Boolean) : [];
    const out = namesForAddresses(addresses);

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ names: out }));
    return;
  }

  // POST /username  { address, username, signature, nonce }
  if (url.pathname === "/username" && req.method === "POST") {
    let body;
    try { body = await readJsonBody(req); }
    catch (e) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: e.message }));
      return;
    }

    const address = (body.address || "").trim();
    const usernameInput = body.username;
    const signature = (body.signature || "").trim();
    const nonce = (body.nonce || "").trim();

    // Validate username format
    const v = validateUsername(usernameInput);
    if (!v.ok) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: `invalid username: ${v.reason}` }));
      return;
    }
    const username = v.username;

    // Enforce unique usernames (case-insensitive): reject if taken by another address
    const taken = await isUsernameTaken(username, address);
    if (taken) {
      res.writeHead(409, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Username already taken" }));
      return;
    }

    // Verify signature
    const ver = verifyUsernameSignature({ address, username, signature, nonce });
    if (!ver.ok) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: ver.reason }));
      return;
    }

    try {
      await upsertUsername(address, username);
    } catch (e) {
      const isDuplicate =
        e?.message === USERNAME_TAKEN_ERR ||
        e?.code === "23505" ||
        /unique|duplicate|already taken/i.test(String(e?.message || ""));
      if (isDuplicate) {
        res.writeHead(409, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Username already taken" }));
        return;
      }
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: `db update failed: ${e.message}` }));
      return;
    }

    // Broadcast username update so all clients can update UI immediately
    broadcast({
      type: "username",
      serverTs: Math.floor(Date.now() / 1000),
      address,
      username,
    });

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, address, username }));
    return;
  }

  // health
  if (url.pathname === "/health") {
    const lastClaim = claimLog.length ? claimLog[claimLog.length - 1] : null;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      ok:           true,
      clients:      clients.size,
      wsSubscribed: stateSubId != null,
      lastStateSig,
      stateVersion,
      sseEventId,
      uptime:       Math.floor(process.uptime()),
      claimCount:   claimLog.length,
      lastClaim,
      db: {
        enabled: pgEnabled(),
        ready: pgReady,
      },
      usernames: {
        cached: usernameCache.size,
        cacheLoadedAtMs: usernameCacheLoadedAtMs.v,
      },
      walletStateCacheSize: walletStateCache.size,
      txContextCachedAtMs: txContextCache?.fetchedAtMs || null,
    }));
    return;
  }

  // shared server timestamp for clients that want wall-clock alignment
  if (url.pathname === "/time" && req.method === "GET") {
    const unixtime = Math.floor(Date.now() / 1000);
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    res.end(JSON.stringify({
      unixtime,
      now: unixtime,
      ms: Date.now(),
    }));
    return;
  }

  res.writeHead(404); res.end("Not found");
});
server.keepAliveTimeout = 75_000;
server.headersTimeout = 80_000;
server.requestTimeout = 0;

// ─── BOOT ──────────────────────────────────────────────────────────────────
server.listen(PORT, async () => {
  console.log(`\nRelay server ready`);
  console.log(`  HTTP : http://0.0.0.0:${PORT}`);
  console.log(`  RPC  : ${RPC_URL}`);
  console.log(`  WS   : ${WS_URL}\n`);

  // Init DB + load caches
  try {
    await initPg();

    if (pgEnabled() && pgReady) {
      const loadedClaims = await loadRecentClaims(CLAIM_LOG_MAX);
      for (const c of loadedClaims) claimLog.push(c);
      console.log(`[DB] loaded ${loadedClaims.length} recent claims into memory`);

      await loadAllUsernames();
    }
  } catch (e) {
    console.warn("[DB] init/load failed:", e.message);
  }

  // Seed initial state
  try {
    const acc = await connection.getAccountInfo(statePk, "confirmed");
    if (acc?.data) await handleStateData(acc.data, "boot");
    console.log("[Boot] initial state loaded");
  } catch (e) {
    console.warn("[Boot] initial state read failed:", e.message);
  }

  // Seed initial vault baseline
  try {
    const bal = await connection.getTokenAccountBalance(vaultPk, "confirmed");
    lastVaultAmountStr = bal?.value?.amount || "0";
  } catch (e) {
    console.warn("[Boot] initial vault read failed:", e.message);
  }

  await subscribeAccounts();
  setTimeout(safetyPoll, POLL_MS);
});

process.on("unhandledRejection", (reason) => {
  console.error("[Process] unhandledRejection:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("[Process] uncaughtException:", err);
});