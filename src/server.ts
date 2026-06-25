import express from "express";
import http from "http";
import WebSocket, { Server as WebSocketServer } from "ws";
import cors from "cors";
import dotenv from "dotenv";
import redis, { redisSub } from "./redis";
import prisma from "./db";

dotenv.config();

const PORT = process.env.PORT || 4000;
const NODE_RPC_URL = process.env.NODE_RPC_URL || "http://127.0.0.1:9103";
const LATEST_STATS_KEY = "lumina:latest_stats";
const LATEST_BLOCKS_KEY = "lumina:latest_blocks";
const LATEST_TXS_KEY = "lumina:latest_txs";
const TOTAL_ACCOUNTS_KEY = "lumina:total_accounts";
const BALANCE_CACHE_TTL_SECONDS = 10;
const CONTRACT_SUMMARY_CACHE_TTL_SECONDS = 300;
const CONTRACT_BYTECODE_CACHE_TTL_SECONDS = 3600;
const MAX_INTERNAL_TX_PAGE_SIZE = 100;
const MAX_CONTRACT_EVENT_LIMIT = 100;
const MAX_TOKEN_DISCOVERY_TXS = 2000;

const corsOptions = {
  origin: "*",
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["DNT", "User-Agent", "X-Requested-With", "If-Modified-Since", "Cache-Control", "Content-Type", "Range", "Authorization"],
  exposedHeaders: ["Content-Length", "Content-Range"],
  optionsSuccessStatus: 204,
};

const app = express();
app.use(cors(corsOptions));
app.options("*", cors(corsOptions)); // Enable pre-flight for all routes
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/explorer/ws" });

function safeJsonParse<T>(value: string | null): T | null {
  if (!value) return null;
  try {
    return JSON.parse(value) as T;
  } catch {
    return null;
  }
}

function normalizeTxHash(hash: string): string {
  return hash.trim().replace(/^Hash\(/, "").replace(/\)$/, "").replace(/^0x/i, "");
}

function toNetworkStatsResponse(cachedStats: any, nodeStats?: any) {
  return {
    circulating_supply: cachedStats.circulating_supply,
    total_supply: cachedStats.total_supply,
    confirmed_tps: cachedStats.confirmed_tps ?? 0,
    inbound_tps: cachedStats.inbound_tps ?? 0,
    total_transactions: Number(cachedStats.total_transactions ?? 0),
    consensus_time_ms: Number(cachedStats.consensus_time_ms ?? 0),
    commit_time_ms: Number(cachedStats.commit_time_ms ?? 0),
    block_time_ms: Number(cachedStats.block_time_ms ?? 0),
    aups: Number(cachedStats.aups ?? 0),
    persistence_lag: Number(cachedStats.persistence_lag ?? 0),
    total_height: Number(cachedStats.height ?? cachedStats.total_height ?? 0),
    avg_fee: nodeStats?.avg_fee ?? cachedStats.avg_fee ?? "0",
    active_nodes: nodeStats?.active_nodes ?? cachedStats.active_nodes ?? 0,
    chain_id: nodeStats?.chain_id ?? cachedStats.chain_id ?? "lumina-testnet-1",
  };
}

async function fetchNetworkStatsFromNode(): Promise<any | null> {
  try {
    return await proxyToNode("/network/stats", "GET");
  } catch {
    return null;
  }
}

// Helper function to proxy requests to the Lumina Node
async function proxyToNode(path: string, method: string, body?: any): Promise<any> {
  const url = `${NODE_RPC_URL}${path}`;
  const hasBody = body && method !== "GET" && method !== "HEAD" && Object.keys(body).length > 0;
  try {
    const res = await fetch(url, {
      method,
      headers: hasBody ? { "Content-Type": "application/json" } : {},
      body: hasBody ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) {
      throw new Error(`Node responded with status ${res.status}`);
    }
    return await res.json();
  } catch (err) {
    console.error(`Proxy error for path ${path}:`, err);
    throw err;
  }
}

type ParsedTokenInfo = {
  contract_id?: string;
  method?: string;
  token_name?: string;
  token_symbol?: string;
  token_recipient?: string;
  token_amount?: string;
  logo?: string;
  owner?: string;
  decimals?: string | number;
} | null;

function parseTokenInfo(raw: string | null | undefined): ParsedTokenInfo {
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function bigintString(value: string | number | bigint | null | undefined): bigint {
  try {
    if (value === null || value === undefined || value === "") return 0n;
    return BigInt(value);
  } catch {
    return 0n;
  }
}

async function getCachedJson<T>(key: string): Promise<T | null> {
  return safeJsonParse<T>(await redis.get(key));
}

async function contractBytecodeExists(address: string): Promise<boolean> {
  const count = await prisma.contract.count({
    where: { address },
  });
  return count > 0;
}

async function buildContractSummary(address: string) {
  const cacheKey = `lumina:contract:summary:${address}`;
  const cached = await getCachedJson<any>(cacheKey);
  if (cached) {
    return cached;
  }

  const contract = await prisma.contract.findUnique({
    where: { address },
  });

  if (!contract) {
    return { error: "Not a contract address" };
  }

  const metadata: Record<string, string> = {};
  if (contract.name) metadata.name = contract.name;
  if (contract.symbol) metadata.symbol = contract.symbol;
  if (contract.logo) metadata.logo = contract.logo;
  if (contract.owner) metadata.owner = contract.owner;
  if (contract.decimals !== null && contract.decimals !== undefined) {
    metadata.decimals = String(contract.decimals);
  }
  if (contract.total_supply) metadata.total_supply = contract.total_supply;

  const summary = {
    address,
    contract_type: contract.symbol ? "Token (LTS-20)" : "Smart Contract / DApp",
    metadata,
    abi: [],
  };

  await redis.set(cacheKey, JSON.stringify(summary), "EX", CONTRACT_SUMMARY_CACHE_TTL_SECONDS);
  return summary;
}

async function hasIndexedAccountState(): Promise<boolean> {
  const result = await prisma.$queryRaw<Array<{ has_state: boolean }>>`
    SELECT EXISTS (
      SELECT 1
      FROM "Account"
      WHERE balance <> '0'
         OR staked <> '0'
         OR nonce <> 0
         OR name <> ''
         OR is_validator = true
         OR validator_status <> 'None'
    ) AS has_state
  `;

  return Boolean(result[0]?.has_state);
}

let lastKnownTotalAccounts = 250000;
let isCountingAccounts = false;

// Helper to fetch total accounts dynamically from local DB/cache only.
async function getTotalAccounts(): Promise<number> {
  const cached = await redis.get(TOTAL_ACCOUNTS_KEY);
  if (cached) {
    const parsed = parseInt(cached, 10);
    if (!isNaN(parsed) && parsed > 0) {
      lastKnownTotalAccounts = parsed;
      return parsed;
    }
  }

  // If not cached and not currently counting, trigger count in the background
  if (!isCountingAccounts) {
    isCountingAccounts = true;
    (async () => {
      try {
        console.log("📊 Starting background account count query...");
        const count = await prisma.account.count();
        if (count > 0) {
          lastKnownTotalAccounts = count;
          await redis.set(TOTAL_ACCOUNTS_KEY, count.toString(), "EX", 7200); // Cache for 2 hours
          console.log(`📊 Background account count query finished: ${count}`);
        }
      } catch (err) {
        console.error("Error fetching total accounts in background:", err);
      } finally {
        isCountingAccounts = false;
      }
    })();
  }

  return lastKnownTotalAccounts;
}

// Endpoint: /total_accounts
app.get("/total_accounts", async (req, res) => {
  try {
    const total_accounts = await getTotalAccounts();
    res.json({ total_accounts });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Explicit /balance proxy with short-lived cache to avoid repeated hot-address bursts to the node.
app.get("/balance/:address", async (req, res) => {
  const { address } = req.params;
  const cacheKey = `lumina:balance:${address}`;

  try {
    const cached = safeJsonParse<any>(await redis.get(cacheKey));
    if (cached) {
      return res.json(cached);
    }

    const data = await proxyToNode(`/balance/${address}`, "GET");
    await redis.set(cacheKey, JSON.stringify(data), "EX", BALANCE_CACHE_TTL_SECONDS);
    res.json(data);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 1. Endpoint: /network/stats (100% database / Redis-backed, no node proxy)
app.get("/network/stats", async (req, res) => {
  try {
    const cachedStats = safeJsonParse<any>(await redis.get(LATEST_STATS_KEY));
    const nodeStats = await fetchNetworkStatsFromNode();
    
    const totalAccounts = await getTotalAccounts();
    const totalTransactions = Number(nodeStats?.total_transactions ?? cachedStats?.total_transactions ?? 0);

    if (cachedStats) {
      const response = toNetworkStatsResponse(cachedStats, nodeStats);
      return res.json({ ...response, total_transactions: totalTransactions, total_accounts: totalAccounts });
    }

    const latestBlock = await prisma.block.findFirst({ orderBy: { height: "desc" } });

    if (latestBlock) {
      const response = toNetworkStatsResponse({
        circulating_supply: latestBlock.circulating_supply,
        total_supply: latestBlock.total_supply,
        confirmed_tps: latestBlock.confirmed_tps,
        inbound_tps: latestBlock.inbound_tps,
        consensus_time_ms: Number(latestBlock.consensus_time_ms),
        commit_time_ms: latestBlock.commit_time_ms,
        block_time_ms: latestBlock.block_time_ms,
        aups: latestBlock.aups,
        persistence_lag: Number(latestBlock.persistence_lag),
        height: Number(latestBlock.height),
        avg_fee: "0",
        active_nodes: 0,
        chain_id: process.env.CHAIN_ID || "lumina-testnet-1",
      }, nodeStats);

      return res.json({
        ...response,
        total_transactions: totalTransactions,
        total_accounts: totalAccounts,
      });
    }

    if (nodeStats) {
      const response = toNetworkStatsResponse(nodeStats, nodeStats);
      return res.json({ ...response, total_transactions: totalTransactions, total_accounts: totalAccounts });
    }

    res.status(503).json({ error: "No cached network stats available", total_accounts: totalAccounts });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 2. Endpoint: /recent_blocks
app.get("/recent_blocks", async (req, res) => {
  try {
    const cachedBlocks = await redis.lrange(LATEST_BLOCKS_KEY, 0, 9);
    if (cachedBlocks && cachedBlocks.length > 0) {
      const blocks = cachedBlocks
        .map((b) => safeJsonParse<any>(b))
        .filter((block): block is any => block !== null);
      return res.json(blocks);
    }

    // Fallback to database
    const blocksDb = await prisma.block.findMany({
      orderBy: { height: "desc" },
      take: 10,
    });
    const blocks = blocksDb.map((b) => ({
      height: Number(b.height),
      hash: b.hash,
      tx_count: b.tx_count,
      timestamp: Number(b.timestamp),
      leader: b.leader,
      leader_name: b.leader_name,
      reward: b.reward,
      parent_qc: b.parent_qc ? JSON.parse(b.parent_qc) : null,
    }));
    res.json(blocks);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 3. Endpoint: /recent_txs
app.get("/recent_txs", async (req, res) => {
  try {
    const cachedTxs = await redis.lrange(LATEST_TXS_KEY, 0, 49);
    if (cachedTxs && cachedTxs.length > 0) {
      const transactions = cachedTxs
        .map((tx) => safeJsonParse<any>(tx))
        .filter((tx): tx is any => tx !== null);
      return res.json(transactions);
    }

    const txsDb = await prisma.transaction.findMany({
      orderBy: [{ block_height: "desc" }, { hash: "desc" }],
      take: 50,
      include: { block: true },
    });
    const txs = txsDb.map((t) => ({
      hash: t.hash,
      from: t.from,
      from_name: t.from_name,
      to: t.to,
      to_name: t.to_name,
      value: t.value,
      status: t.status,
      method: t.method,
      timestamp: Number(t.block.timestamp),
      block_height: Number(t.block_height),
      token_info: t.token_info ? JSON.parse(t.token_info) : null,
    }));
    res.json(txs);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 4. Endpoint: /blocks (Paginated)
app.get("/blocks", async (req, res) => {
  try {
    const page = parseInt(req.query.page as string) || 0;
    const limit = Math.min(100, parseInt(req.query.limit as string) || 10);

    const total = await prisma.block.count();
    const blocksDb = await prisma.block.findMany({
      orderBy: { height: "desc" },
      skip: page * limit,
      take: limit,
    });

    const blocks = blocksDb.map((b) => ({
      height: Number(b.height),
      hash: b.hash,
      tx_count: b.tx_count,
      timestamp: Number(b.timestamp),
      leader: b.leader,
      leader_name: b.leader_name,
      reward: b.reward,
      parent_qc: b.parent_qc ? JSON.parse(b.parent_qc) : null,
    }));

    res.json({ blocks, total });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 5. Endpoint: /txs_all (Paginated)
app.get("/txs_all", async (req, res) => {
  try {
    const page = parseInt(req.query.page as string) || 0;
    const limit = Math.min(100, parseInt(req.query.limit as string) || 10);

    const txsDb = await prisma.transaction.findMany({
      orderBy: [{ block_height: "desc" }, { hash: "desc" }],
      skip: page * limit,
      take: limit,
      include: { block: true },
    });

    const transactions = txsDb.map((t) => ({
      hash: t.hash,
      from: t.from,
      from_name: t.from_name,
      to: t.to,
      to_name: t.to_name,
      value: t.value,
      status: t.status,
      method: t.method,
      token_info: t.token_info ? JSON.parse(t.token_info) : null,
      block_height: Number(t.block_height),
      timestamp: Number(t.block.timestamp),
    }));

    // Dynamic total estimation to bypass expensive COUNT(*) queries
    const hasMore = txsDb.length === limit;
    const total = hasMore ? (page + 2) * limit : (page * limit) + txsDb.length;

    res.json({ transactions, total });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 6. Endpoint: /block/:id (By Height or Hash)
app.get("/block/:id", async (req, res) => {
  const { id } = req.params;
  try {
    let blockDb;
    if (/^\d+$/.test(id)) {
      blockDb = await prisma.block.findUnique({
        where: { height: BigInt(id) },
        include: { transactions: true },
      });
    } else {
      blockDb = await prisma.block.findUnique({
        where: { hash: id },
        include: { transactions: true },
      });
    }

    if (blockDb) {
      let parent_hash = "";
      if (blockDb.parent_qc) {
        try {
          const parsed = JSON.parse(blockDb.parent_qc);
          if (parsed && parsed.block_hash) {
            parent_hash = parsed.block_hash;
          }
        } catch (e) {
          // ignore
        }
      }

      // Fallback: if parent_hash is still empty and height > 0, query the database for the block at height - 1
      if (!parent_hash && blockDb.height > 0n) {
        const prevBlock = await prisma.block.findUnique({
          where: { height: blockDb.height - 1n },
          select: { hash: true },
        });
        if (prevBlock) {
          parent_hash = prevBlock.hash;
        }
      }

      return res.json({
        height: Number(blockDb.height),
        hash: blockDb.hash,
        parent_hash,
        timestamp: Number(blockDb.timestamp),
        tx_count: blockDb.tx_count,
        leader: blockDb.leader,
        leader_name: blockDb.leader_name,
        reward: blockDb.reward,
        fees: blockDb.fees,
        total_reward: (BigInt(blockDb.reward) + BigInt(blockDb.fees)).toString(),
        parent_qc: blockDb.parent_qc ? JSON.parse(blockDb.parent_qc) : null,
        transactions: blockDb.transactions.map((t) => ({
          hash: t.hash,
          from: t.from,
          from_name: t.from_name,
          to: t.to,
          to_name: t.to_name,
          value: t.value,
          nonce: Number(t.nonce),
          status: t.status,
          method: t.method,
          token_info: t.token_info ? JSON.parse(t.token_info) : null,
        })),
      });
    }

    // Do not proxy fallback, return 404
    return res.status(404).json({ error: "Block not found" });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/tx/:hash", async (req, res) => {
  const requestedHash = req.params.hash;
  const normalizedHash = normalizeTxHash(requestedHash);
  try {
    const txDb = await prisma.transaction.findFirst({
      where: {
        OR: [
          { hash: requestedHash },
          { hash: normalizedHash },
          { hash: `0x${normalizedHash}` },
          { hash: `Hash(0x${normalizedHash})` },
        ],
      },
      include: { block: true },
    });

    if (txDb) {
      // Fetch latest indexed block height for confirmation count
      const latestBlock = await prisma.block.findFirst({
        orderBy: { height: "desc" },
        select: { height: true },
      });
      const latestHeight = latestBlock ? Number(latestBlock.height) : Number(txDb.block_height);

      return res.json({
        hash: txDb.hash,
        from: txDb.from,
        from_name: txDb.from_name,
        to: txDb.to,
        to_name: txDb.to_name,
        value: txDb.value,
        nonce: Number(txDb.nonce),
        data: txDb.data,
        signature: txDb.signature,
        pubkey: txDb.pubkey,
        fee: txDb.fee,
        fee_payer: txDb.fee_payer,
        fee_payer_name: txDb.fee_payer_name,
        fee_payer_signature: txDb.fee_payer_signature,
        fee_payer_pubkey: txDb.fee_payer_pubkey,
        status: txDb.status,
        method: txDb.method,
        block_height: Number(txDb.block_height),
        latest_height: latestHeight,
        timestamp: Number(txDb.block.timestamp),
        token_info: txDb.token_info ? JSON.parse(txDb.token_info) : null,
      });
    }

    // Fallback: Check Redis mempool cache for pending tx
    const mempoolTx = await redis.get(`lumina:mempool:${normalizedHash}`);
    if (mempoolTx) {
      return res.json(JSON.parse(mempoolTx));
    }

    return res.status(404).json({ error: "Transaction not found" });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 8. Endpoint: /txs/:address (Address transactions list)
app.get("/txs/:address", async (req, res) => {
  const { address } = req.params;
  const page = parseInt(req.query.page as string) || 0;
  const limit = Math.min(100, parseInt(req.query.limit as string) || 10);

  try {
    // Fetch up to (page + 1) * limit rows from both directions
    const fetchLimit = (page + 1) * limit;

    const txsFrom = await prisma.transaction.findMany({
      where: { from: address },
      orderBy: { block_height: "desc" },
      take: fetchLimit,
      include: { block: true },
    });

    const txsTo = await prisma.transaction.findMany({
      where: { to: address },
      orderBy: { block_height: "desc" },
      take: fetchLimit,
      include: { block: true },
    });

    // Merge and sort in memory
    const combined = [...txsFrom, ...txsTo]
      .sort((a, b) => Number(b.block_height - a.block_height))
      .slice(page * limit, (page + 1) * limit);

    // Dynamic total estimation to bypass expensive COUNT(*) queries
    const hasMore = combined.length === limit;
    const total = hasMore ? (page + 2) * limit : (page * limit) + combined.length;

    const transactions = combined.map((t) => ({
      hash: t.hash,
      from: t.from,
      from_name: t.from_name,
      to: t.to,
      to_name: t.to_name,
      value: t.value,
      nonce: Number(t.nonce),
      timestamp: Number(t.block.timestamp),
      status: t.status,
      method: t.method,
      token_info: t.token_info ? JSON.parse(t.token_info) : null,
    }));

    res.json({
      address,
      page,
      limit,
      total,
      transactions,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 9. Endpoint: /search (Smart Redirector)
app.get("/search", async (req, res) => {
  const query = (req.query.q as string || "").trim();
  try {
    if (/^\d+$/.test(query)) {
      return res.json({ type: "Block", url: `/block/${query}` });
    }

    // Cari block hash
    const block = await prisma.block.findUnique({ where: { hash: query } });
    if (block) {
      return res.json({ type: "Block", url: `/block/${query}` });
    }

    // Cari tx hash
    const tx = await prisma.transaction.findUnique({ where: { hash: query } });
    if (tx) {
      return res.json({ type: "Transaction", url: `/tx/${query}` });
    }

    // Jika format address (Bech32 dimulai dengan 'lumina1' atau '0x')
    if (query.startsWith("lumina1") || query.startsWith("0x")) {
      return res.json({ type: "Address", url: `/address/${query}` });
    }

    // Do not proxy fallback, return "not found"
    return res.status(404).json({ error: "Not found" });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

let cachedTopAccounts: any[] = [];
let isUpdatingTopAccounts = false;

async function updateTopAccountsCache() {
  if (isUpdatingTopAccounts) return;
  isUpdatingTopAccounts = true;
  try {
    console.log("📊 Starting background top accounts query...");
    // Fetch top 1000 accounts: prioritizing non-zero balance/staked accounts,
    // and filling the rest with recently active accounts.
    const accounts = await prisma.$queryRaw<Array<{
      address: string;
      balance: string;
      staked: string;
      nonce: bigint;
      name: string;
      is_validator: boolean;
      validator_status: string;
    }>>`
      WITH non_zero AS (
        SELECT address, balance, staked, nonce, name, is_validator, validator_status, updated_at_block
        FROM "Account"
        WHERE balance <> '0' OR staked <> '0'
        ORDER BY CAST(balance AS NUMERIC) DESC, CAST(staked AS NUMERIC) DESC, nonce DESC, address ASC
        LIMIT 1000
      ),
      recent_active AS (
        SELECT address, balance, staked, nonce, name, is_validator, validator_status, updated_at_block
        FROM "Account"
        ORDER BY updated_at_block DESC, address ASC
        LIMIT 1000
      )
      SELECT address, balance, staked, nonce, name, is_validator, validator_status
      FROM (
        SELECT address, balance, staked, nonce, name, is_validator, validator_status, updated_at_block, 1 as sort_priority
        FROM non_zero
        UNION ALL
        SELECT address, balance, staked, nonce, name, is_validator, validator_status, updated_at_block, 2 as sort_priority
        FROM recent_active
        WHERE address NOT IN (SELECT address FROM non_zero)
      ) AS combined
      ORDER BY sort_priority ASC, CAST(balance AS NUMERIC) DESC, CAST(staked AS NUMERIC) DESC, updated_at_block DESC, address ASC
      LIMIT 1000
    `;

    const formattedAccounts = accounts.map((acc) => ({
      ...acc,
      nonce: Number(acc.nonce),
    }));

    cachedTopAccounts = formattedAccounts;
    await redis.set("lumina:top_accounts_cache", JSON.stringify(formattedAccounts), "EX", 1800); // Cache in redis for 30 minutes
    console.log(`📊 Background top accounts query finished: cached ${formattedAccounts.length} accounts.`);
  } catch (err) {
    console.error("❌ Error updating top accounts cache in background:", err);
  } finally {
    isUpdatingTopAccounts = false;
  }
}

// Start background task (every 10 minutes)
setInterval(updateTopAccountsCache, 600000);
// Warm up cache 5 seconds after startup
setTimeout(updateTopAccountsCache, 5000);

// 10. Endpoint: /accounts/top
app.get("/accounts/top", async (req, res) => {
  try {
    const page = parseInt(req.query.page as string) || 0;
    const limit = Math.min(100, parseInt(req.query.limit as string) || 20);
    const offset = page * limit;

    let accountsList = cachedTopAccounts;
    if (accountsList.length === 0) {
      const cached = await redis.get("lumina:top_accounts_cache");
      if (cached) {
        accountsList = JSON.parse(cached);
        cachedTopAccounts = accountsList;
      }
    }

    if (accountsList.length === 0) {
      updateTopAccountsCache();
      return res.json({
        total: 0,
        accounts: [],
        page,
        limit,
        ranking_mode: "observed_recent",
      });
    }

    const total = await getTotalAccounts();
    const slicedAccounts = accountsList.slice(offset, offset + limit);

    // Fetch live balance, staked, nonce from the node for the active page
    const enrichedAccounts = await Promise.all(
      slicedAccounts.map(async (acc: any) => {
        try {
          const cacheKey = `lumina:balance:${acc.address}`;
          let data = safeJsonParse<any>(await redis.get(cacheKey));
          if (!data) {
            data = await proxyToNode(`/balance/${acc.address}`, "GET");
            await redis.set(cacheKey, JSON.stringify(data), "EX", 120); // cache in redis for 2 minutes
          }
          if (data) {
            // Asynchronously update database if values differ
            if (
              acc.balance !== data.balance ||
              acc.staked !== data.staked ||
              acc.nonce !== Number(data.nonce ?? 0) ||
              acc.name !== (data.name || "") ||
              acc.is_validator !== Boolean(data.is_validator) ||
              acc.validator_status !== (data.validator_status || "None")
            ) {
              prisma.account.update({
                where: { address: acc.address },
                data: {
                  balance: data.balance || "0",
                  staked: data.staked || "0",
                  nonce: BigInt(data.nonce ?? 0),
                  is_validator: Boolean(data.is_validator),
                  validator_status: data.validator_status || "None",
                  name: data.name || "",
                },
              }).catch((err) => {
                console.error(`Failed to update DB account status for ${acc.address}:`, err);
              });
            }

            return {
              ...acc,
              balance: data.balance || "0",
              staked: data.staked || "0",
              nonce: Number(data.nonce ?? 0),
              is_validator: Boolean(data.is_validator),
              validator_status: data.validator_status || "None",
              name: data.name || acc.name,
            };
          }
        } catch (err) {
          console.warn(`⚠️ Failed to fetch balance from node for ${acc.address}:`, err);
        }
        return acc;
      })
    );

    // Determine ranking mode dynamically based on whether actual balances exist in the enriched list
    const hasBalances = enrichedAccounts.some(acc => acc.balance !== "0" || acc.staked !== "0");
    const ranking_mode = hasBalances ? "balance" : "observed_recent";

    res.json({
      total,
      accounts: enrichedAccounts,
      page,
      limit,
      ranking_mode,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/internal_txs/:address", async (req, res) => {
  const { address } = req.params;
  const page = parseInt(req.query.page as string) || 0;
  const limit = Math.min(MAX_INTERNAL_TX_PAGE_SIZE, parseInt(req.query.limit as string) || 10);

  try {
    const where = {
      leader: address,
      OR: [
        { reward: { not: "0" } },
        { fees: { not: "0" } },
      ],
    };

    const total = await prisma.block.count({ where });
    const blocks = await prisma.block.findMany({
      where,
      orderBy: { height: "desc" },
      skip: page * limit,
      take: limit,
      select: {
        height: true,
        hash: true,
        reward: true,
        fees: true,
        timestamp: true,
      },
    });

    const transactions = blocks.map((block) => ({
      height: Number(block.height),
      hash: block.hash,
      amount: (bigintString(block.reward) + bigintString(block.fees)).toString(),
      type: "BLOCK_REWARD",
      timestamp: Number(block.timestamp),
    }));

    res.json({ total, transactions });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/tokens/:address", async (req, res) => {
  const { address } = req.params;

  try {
    // Fetch from, to, and token_recipient transactions separately
    const txsFrom = await prisma.transaction.findMany({
      where: { from: address },
      orderBy: { block_height: "desc" },
      take: MAX_TOKEN_DISCOVERY_TXS,
    });

    const txsTo = await prisma.transaction.findMany({
      where: { to: address },
      orderBy: { block_height: "desc" },
      take: MAX_TOKEN_DISCOVERY_TXS,
    });

    const txsRecipient = await prisma.transaction.findMany({
      where: { token_recipient: address },
    });

    // Merge and sort in memory
    const txs = [...txsFrom, ...txsTo, ...txsRecipient]
      .sort((a, b) => Number(b.block_height - a.block_height))
      .slice(0, MAX_TOKEN_DISCOVERY_TXS);

    const tokenMap = new Map<string, { contract: string; name: string; symbol: string; logo: string; balance: bigint }>();

    for (const tx of txs) {
      const tokenInfo = parseTokenInfo(tx.token_info);
      const contractId = tokenInfo?.contract_id;
      if (!contractId) continue;

      const entry = tokenMap.get(contractId) ?? {
        contract: contractId,
        name: tokenInfo?.token_name || "",
        symbol: tokenInfo?.token_symbol || "TOKEN",
        logo: tokenInfo?.logo || "",
        balance: 0n,
      };

      const amount = bigintString(tokenInfo?.token_amount);
      const recipient = tokenInfo?.token_recipient;

      if (recipient === address) {
        entry.balance += amount;
      } else if (tx.from === address) {
        entry.balance -= amount;
      } else if (tx.to === address) {
        entry.balance += amount;
      }

      if (!entry.name && tokenInfo?.token_name) entry.name = tokenInfo.token_name;
      if ((!entry.symbol || entry.symbol === "TOKEN") && tokenInfo?.token_symbol) entry.symbol = tokenInfo.token_symbol;
      if (!entry.logo && tokenInfo?.logo) entry.logo = tokenInfo.logo;

      tokenMap.set(contractId, entry);
    }

    const tokens = Array.from(tokenMap.values())
      .filter((token) => token.balance > 0n)
      .sort((a, b) => (a.balance === b.balance ? a.contract.localeCompare(b.contract) : a.balance > b.balance ? -1 : 1))
      .map((token) => ({
        contract: token.contract,
        name: token.name,
        symbol: token.symbol,
        logo: token.logo,
        balance: token.balance.toString(),
      }));

    res.json({ address, tokens, source: "indexed_history" });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/contract/:address/events", async (req, res) => {
  const { address } = req.params;
  const limit = Math.min(MAX_CONTRACT_EVENT_LIMIT, parseInt(req.query.limit as string) || 50);

  try {
    const txs = await prisma.transaction.findMany({
      where: { to: address },
      orderBy: { block_height: "desc" },
      take: limit,
      include: { block: true },
    });

    const events = txs.map((tx) => {
      const tokenInfo = parseTokenInfo(tx.token_info);
      const recipient = tokenInfo?.token_recipient || tx.to;
      const data: Record<string, string | number> = {
        from: tx.from,
        to: recipient,
        amount: tokenInfo?.token_amount || tx.value,
        block_height: Number(tx.block_height),
        timestamp: Number(tx.block.timestamp),
      };

      if (tokenInfo?.token_symbol) data.symbol = tokenInfo.token_symbol;

      return {
        tx_hash: tx.hash,
        event: tokenInfo?.method || tx.method || "CALL",
        data,
      };
    });

    res.json({ address, events, source: "indexed_history" });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/contract/:id/metadata", async (req, res) => {
  try {
    const summary = await buildContractSummary(req.params.id);
    if (summary?.error) {
      return res.status(404).json(summary);
    }
    res.json(summary);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/contract/:address", async (req, res) => {
  try {
    const summary = await buildContractSummary(req.params.address);
    if (summary?.error) {
      return res.status(404).json(summary);
    }
    res.json(summary);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 11. Endpoint: /mempool/recent (100% Redis-backed offline query)
app.get("/mempool/recent", async (req, res) => {
  try {
    const hashes = await redis.smembers("lumina:mempool_hashes");
    if (!hashes || hashes.length === 0) {
      return res.json([]);
    }
    const pipeline = redis.pipeline();
    for (const hash of hashes) {
      pipeline.get(`lumina:mempool:${hash}`);
    }
    const results = await pipeline.exec();
    const txs: any[] = [];
    for (const r of results || []) {
      if (r[0] === null && r[1]) {
        try {
          txs.push(JSON.parse(r[1] as string));
        } catch {
          // ignore
        }
      }
    }
    txs.sort((a, b) => b.timestamp - a.timestamp);
    res.json(txs.slice(0, 50));
  } catch (err: any) {
    console.error("⚠️ Failed to read mempool from Redis:", err);
    res.json([]);
  }
});

// 12. Endpoint: /network/validators (100% Redis-backed)
app.get("/network/validators", async (req, res) => {
  try {
    const cached = await redis.get("lumina:network:validators");
    if (cached) {
      return res.json(JSON.parse(cached));
    }
  } catch (err) {
    console.warn("⚠️ Network validators cache read failed:", err);
  }
  try {
    const data = await proxyToNode("/network/validators", "GET");
    return res.json(data);
  } catch {
    res.json({ validators: [] });
  }
});

// 13. Endpoint: /network/peers (100% Redis-backed)
app.get("/network/peers", async (req, res) => {
  try {
    const cached = await redis.get("lumina:network:peers");
    if (cached) {
      return res.json(JSON.parse(cached));
    }
  } catch (err) {
    console.warn("⚠️ Network peers cache read failed:", err);
  }
  try {
    const data = await proxyToNode("/network/peers", "GET");
    return res.json(data);
  } catch {
    res.json({ peers: [] });
  }
});

// 14. Endpoint: /contract/:address/bytecode (Retrieve bytecode offline from postgres)
app.get("/contract/:address/bytecode", async (req, res) => {
  const { address } = req.params;
  try {
    const contract = await prisma.contract.findUnique({
      where: { address },
      select: { bytecode: true },
    });
    if (contract) {
      return res.json({ bytecode: contract.bytecode });
    }
    return res.status(404).json({ error: "Contract bytecode not found" });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 15. Endpoint: /contract/:id/call/:method (Smart contract VM read call with tight 2s abort signal)
app.get("/contract/:id/call/:method", async (req, res) => {
  const { id, method } = req.params;
  const cacheKey = `lumina:contract:call:${id}:${method}:${JSON.stringify(req.query)}`;
  try {
    const cached = await redis.get(cacheKey);
    if (cached) {
      return res.json(JSON.parse(cached));
    }
  } catch (err) {
    // ignore
  }

  try {
    const url = `${NODE_RPC_URL}${req.originalUrl}`;
    const response = await fetch(url, {
      method: "GET",
      signal: AbortSignal.timeout(2000),
    });
    if (!response.ok) {
      throw new Error(`Node responded with status ${response.status}`);
    }
    const data = await response.json();
    try {
      await redis.set(cacheKey, JSON.stringify(data), "EX", 10);
    } catch {
      // ignore
    }
    return res.json(data);
  } catch (err: any) {
    console.warn(`⚠️ Contract call fallback failed or timed out: ${err.message}`);
    return res.status(504).json({ error: "Contract call timeout or node unavailable" });
  }
});

// Catch-all fallback proxy — hanya untuk path API yang dikenal oleh lumina node.
// Path asing (dari bot scanner seperti /php/login.php, /wiki, dll.) langsung 404
// tanpa diteruskan ke node untuk menghindari log error spam.
const NODE_PROXY_PREFIXES = [
  "/contract",
  "/token",
  "/nonce",
  "/fee",
  "/vm/",
];

app.all("*", async (req, res) => {
  const path = req.originalUrl.split("?")[0];
  const isKnownPath = NODE_PROXY_PREFIXES.some((prefix) => path.startsWith(prefix));

  if (!isKnownPath) {
    // Tolak diam-diam — ini kemungkinan bot scanner, bukan permintaan API valid
    return res.status(404).json({ error: "Not found" });
  }

  try {
    const data = await proxyToNode(req.originalUrl, req.method, req.body);
    res.json(data);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// WS Server Client Connections (Next.js clients WSS)
const clients = new Set<WebSocket>();

wss.on("connection", (ws) => {
  clients.add(ws);
  console.log(`🔌 Client connected to Explorer Server WebSocket. Total: ${clients.size}`);

  ws.on("close", () => {
    clients.delete(ws);
    console.log(`🔌 Client disconnected. Total: ${clients.size}`);
  });
});

// Subscribe ke Redis Pub/Sub untuk menyiarkan new_block secara instan ke semua connected clients
redisSub.subscribe("lumina:block_channel", (err) => {
  if (err) {
    console.error("❌ Failed to subscribe to Redis block channel:", err);
  } else {
    console.log("🔊 Subscribed to Redis lumina:block_channel for WebSocket broadcasting");
  }
});

redisSub.on("message", (channel, message) => {
  if (channel === "lumina:block_channel") {
    // Siarkan message mentah langsung ke semua client Next.js explorer
    for (const client of clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    }
  }
});

server.listen(PORT, () => {
  console.log(`🚀 Explorer Backend Server running on http://localhost:${PORT}`);
  console.log(`🚀 Explorer WebSocket Server running on ws://localhost:${PORT}/explorer/ws`);
});
