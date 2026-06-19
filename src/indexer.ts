import WebSocket from "ws";
import dotenv from "dotenv";
import redis, { redisPub } from "./redis";
import prisma from "./db";
import { producer, consumer, connectKafka, ensureTopicExists } from "./kafka";

dotenv.config();

const NODE_WSS_URL = process.env.NODE_WSS_URL || "ws://127.0.0.1:9103/explorer/ws";
const NODE_RPC_URL = process.env.NODE_RPC_URL || "http://127.0.0.1:9103";
const BLOCKS_TOPIC = "lumina-blocks";
const LATEST_STATS_KEY = "lumina:latest_stats";
const LATEST_BLOCKS_KEY = "lumina:latest_blocks";
const LATEST_TXS_KEY = "lumina:latest_txs";
const TOTAL_ACCOUNTS_KEY = "lumina:total_accounts";
const RECENT_BLOCKS_LIMIT = 10;
const RECENT_TXS_LIMIT = 50;
const ACCOUNT_INSERT_BATCH_SIZE = 1_000;
const ACCOUNT_STATE_SYNC_MODE = (process.env.EXPLORER_ACCOUNT_STATE_SYNC_MODE || "observed_only").toLowerCase();
const LIVE_NODE_ACCOUNT_SYNC_ENABLED =
  ACCOUNT_STATE_SYNC_MODE === "node" || ACCOUNT_STATE_SYNC_MODE === "live";

type NodeTransactionPayload = {
  hash: string;
  from: string;
  from_name?: string;
  to: string;
  to_name?: string;
  value?: string;
  nonce?: number | string;
  status?: string;
  method?: string;
  token_info?: unknown;
};

type NodeBlockPayload = {
  type?: string;
  height: number | string;
  hash: string;
  tx_count?: number;
  timestamp: number | string;
  leader: string;
  leader_name?: string;
  circulating_supply?: string;
  total_supply?: string;
  confirmed_tps?: number;
  inbound_tps?: number;
  total_transactions?: number | string;
  consensus_time_ms?: number | string;
  commit_time_ms?: number;
  block_time_ms?: number;
  aups?: number;
  persistence_lag?: number | string;
  reward?: string;
  fees?: string;
  parent_qc?: unknown;
  transactions?: NodeTransactionPayload[];
  avg_fee?: string;
  active_nodes?: number;
  chain_id?: string;
  total_height?: number;
};

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchNodeJson<T>(path: string): Promise<T> {
  const res = await fetch(`${NODE_RPC_URL}${path}`);
  if (!res.ok) {
    throw new Error(`Node responded with status ${res.status} for ${path}`);
  }
  return (await res.json()) as T;
}

type NodeAccountState = {
  address: string;
  balance: string;
  staked: string;
  nonce: number;
  name: string;
  is_validator: boolean;
  validator_status: string;
};

function normalizeAddress(address: string | null | undefined): string | null {
  if (!address) return null;
  const trimmed = address.trim();
  return trimmed.length >= 5 ? trimmed : null;
}

async function upsertObservedAccounts(addresses: Iterable<string>, currentBlockHeight: bigint) {
  const uniqueAddresses = Array.from(
    new Set(
      Array.from(addresses)
        .map((address) => normalizeAddress(address))
        .filter((address): address is string => address !== null),
    ),
  );

  if (uniqueAddresses.length === 0) return;

  await prisma.$transaction([
    prisma.account.createMany({
      data: uniqueAddresses.map((address) => ({
        address,
        balance: "0",
        staked: "0",
        nonce: 0n,
        name: "",
        is_validator: false,
        validator_status: "None",
        updated_at_block: currentBlockHeight,
      })),
      skipDuplicates: true,
    }),
    prisma.account.updateMany({
      where: { address: { in: uniqueAddresses } },
      data: { updated_at_block: currentBlockHeight },
    }),
  ]);

  await redis.del(TOTAL_ACCOUNTS_KEY);
}

async function upsertAccountStateFromNode(address: string, currentBlockHeight: bigint) {
  if (!address || address.length < 5) return;
  try {
    const acc = await fetchNodeJson<NodeAccountState>(`/balance/${address}`);
    await prisma.account.upsert({
      where: { address },
      update: {
        balance: acc.balance,
        staked: acc.staked,
        nonce: BigInt(acc.nonce),
        name: acc.name || "",
        is_validator: acc.is_validator,
        validator_status: acc.validator_status || "None",
        updated_at_block: currentBlockHeight,
      },
      create: {
        address,
        balance: acc.balance,
        staked: acc.staked,
        nonce: BigInt(acc.nonce),
        name: acc.name || "",
        is_validator: acc.is_validator,
        validator_status: acc.validator_status || "None",
        updated_at_block: currentBlockHeight,
      },
    });
  } catch (err: any) {
    console.warn(`⚠️ Failed to update account info for ${address}: ${err.message || err}`);
  }
}

async function bootstrapObservedAccounts() {
  console.log("👤 Bootstrapping observed account registry from PostgreSQL...");
  try {
    const txAddresses = await prisma.$queryRaw<Array<{ address: string }>>`
      SELECT DISTINCT address FROM (
        SELECT "from" AS address FROM "Transaction"
        UNION
        SELECT "to" AS address FROM "Transaction"
      ) AS unique_tx_addresses
    `;

    const blockLeaders = await prisma.block.findMany({
      select: { leader: true },
      distinct: ['leader'],
    });

    const addresses = new Set<string>();
    for (const row of txAddresses) {
      if (row.address) addresses.add(row.address);
    }
    for (const row of blockLeaders) {
      if (row.leader) addresses.add(row.leader);
    }

    console.log(`👤 Found ${addresses.size} unique addresses to bootstrap locally.`);
    const addressList = Array.from(addresses);
    const latestIndexedBlock = await prisma.block.findFirst({
      orderBy: { height: "desc" },
      select: { height: true },
    });
    const bootstrapHeight = latestIndexedBlock?.height ?? 0n;

    for (let i = 0; i < addressList.length; i += ACCOUNT_INSERT_BATCH_SIZE) {
      const batch = addressList.slice(i, i + ACCOUNT_INSERT_BATCH_SIZE);
      await upsertObservedAccounts(batch, bootstrapHeight);
      const count = Math.min(i + batch.length, addressList.length);
      if (count % ACCOUNT_INSERT_BATCH_SIZE === 0 || count === addressList.length) {
        console.log(`👤 Bootstrapped ${count}/${addressList.length} observed accounts...`);
      }
    }

    console.log("✅ Observed account bootstrap completed!");

  } catch (err) {
    console.error("❌ Failed to bootstrap observed accounts:", err);
  }
}

async function fetchBlockWithRetry(height: number, retries = 3): Promise<NodeBlockPayload> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fetchNodeJson<NodeBlockPayload>(`/block/${height}`);
    } catch (err) {
      lastError = err;
      if (attempt < retries) {
        await delay(250 * attempt);
      }
    }
  }
  throw lastError;
}

function toStatsCache(payload: NodeBlockPayload, fallback?: Partial<NodeBlockPayload>) {
  return {
    circulating_supply: payload.circulating_supply || fallback?.circulating_supply || "0",
    total_supply: payload.total_supply || fallback?.total_supply || "0",
    confirmed_tps: payload.confirmed_tps ?? fallback?.confirmed_tps ?? 0,
    inbound_tps: payload.inbound_tps ?? fallback?.inbound_tps ?? 0,
    total_transactions: payload.total_transactions ?? fallback?.total_transactions ?? 0,
    consensus_time_ms: payload.consensus_time_ms ?? fallback?.consensus_time_ms ?? 0,
    commit_time_ms: payload.commit_time_ms ?? fallback?.commit_time_ms ?? 0,
    block_time_ms: payload.block_time_ms ?? fallback?.block_time_ms ?? 0,
    aups: payload.aups ?? fallback?.aups ?? 0,
    persistence_lag: payload.persistence_lag ?? fallback?.persistence_lag ?? 0,
    height: payload.height,
    timestamp: payload.timestamp,
    avg_fee: payload.avg_fee || fallback?.avg_fee || "0",
    active_nodes: payload.active_nodes ?? fallback?.active_nodes ?? 0,
    chain_id: payload.chain_id || fallback?.chain_id || "lumina-testnet-1",
  };
}

function toBlockSummary(payload: NodeBlockPayload) {
  return {
    height: Number(payload.height),
    hash: payload.hash,
    tx_count: payload.tx_count ?? payload.transactions?.length ?? 0,
    timestamp: Number(payload.timestamp),
    leader: payload.leader,
    leader_name: payload.leader_name || "",
    reward: payload.reward || "0",
    parent_qc: payload.parent_qc ?? null,
  };
}

function toTxSummary(payload: NodeBlockPayload, txData: NodeTransactionPayload) {
  return {
    hash: txData.hash,
    from: txData.from,
    from_name: txData.from_name || "",
    to: txData.to,
    to_name: txData.to_name || "",
    value: txData.value || "0",
    status: txData.status || "SUCCESS",
    method: txData.method || "TRANSFER",
    token_info: txData.token_info ?? null,
    timestamp: Number(payload.timestamp),
    block_height: Number(payload.height),
  };
}

async function cacheBlockPayload(payload: NodeBlockPayload, statsFallback?: Partial<NodeBlockPayload>) {
  const stats = toStatsCache(payload, statsFallback);
  await redis.set(LATEST_STATS_KEY, JSON.stringify(stats));

  const blockSummaryStr = JSON.stringify(toBlockSummary(payload));
  await redis.lrem(LATEST_BLOCKS_KEY, 0, blockSummaryStr);
  await redis.lpush(LATEST_BLOCKS_KEY, blockSummaryStr);
  await redis.ltrim(LATEST_BLOCKS_KEY, 0, RECENT_BLOCKS_LIMIT - 1);

  if (payload.transactions && payload.transactions.length > 0) {
    for (const txData of payload.transactions) {
      const txSummaryStr = JSON.stringify(toTxSummary(payload, txData));
      await redis.lrem(LATEST_TXS_KEY, 0, txSummaryStr);
      await redis.lpush(LATEST_TXS_KEY, txSummaryStr);
    }
    await redis.ltrim(LATEST_TXS_KEY, 0, RECENT_TXS_LIMIT - 1);
  }
}

function dbBlockToPayload(block: {
  height: bigint;
  hash: string;
  tx_count: number;
  timestamp: bigint;
  leader: string;
  leader_name: string;
  circulating_supply: string;
  total_supply: string;
  confirmed_tps: number;
  inbound_tps: number;
  total_transactions: bigint;
  consensus_time_ms: bigint;
  commit_time_ms: number;
  block_time_ms: number;
  aups: number;
  persistence_lag: bigint;
  reward: string;
  parent_qc: string | null;
}): NodeBlockPayload {
  return {
    type: "sync_block",
    height: Number(block.height),
    hash: block.hash,
    tx_count: block.tx_count,
    timestamp: Number(block.timestamp),
    leader: block.leader,
    leader_name: block.leader_name,
    circulating_supply: block.circulating_supply,
    total_supply: block.total_supply,
    confirmed_tps: block.confirmed_tps,
    inbound_tps: block.inbound_tps,
    total_transactions: block.total_transactions.toString(),
    consensus_time_ms: block.consensus_time_ms.toString(),
    commit_time_ms: block.commit_time_ms,
    block_time_ms: block.block_time_ms,
    aups: block.aups,
    persistence_lag: block.persistence_lag.toString(),
    reward: block.reward,
    parent_qc: block.parent_qc ? JSON.parse(block.parent_qc) : null,
  };
}

async function hydrateCachesFromDatabase() {
  const [latestBlock, recentBlocks, recentTxs] = await Promise.all([
    prisma.block.findFirst({ orderBy: { height: "desc" } }),
    prisma.block.findMany({
      orderBy: { height: "desc" },
      take: RECENT_BLOCKS_LIMIT,
    }),
    prisma.transaction.findMany({
      orderBy: [{ block_height: "desc" }, { hash: "desc" }],
      take: RECENT_TXS_LIMIT,
      include: { block: true },
    }),
  ]);

  await redis.del(LATEST_BLOCKS_KEY, LATEST_TXS_KEY);

  if (recentBlocks.length > 0) {
    await redis.rpush(
      LATEST_BLOCKS_KEY,
      ...recentBlocks.map((block) =>
        JSON.stringify({
          height: Number(block.height),
          hash: block.hash,
          tx_count: block.tx_count,
          timestamp: Number(block.timestamp),
          leader: block.leader,
          leader_name: block.leader_name,
          reward: block.reward,
          parent_qc: block.parent_qc ? JSON.parse(block.parent_qc) : null,
        }),
      ),
    );
  }

  if (recentTxs.length > 0) {
    await redis.rpush(
      LATEST_TXS_KEY,
      ...recentTxs.map((txData) =>
        JSON.stringify({
          hash: txData.hash,
          from: txData.from,
          from_name: txData.from_name,
          to: txData.to,
          to_name: txData.to_name,
          value: txData.value,
          status: txData.status,
          method: txData.method,
          token_info: txData.token_info ? JSON.parse(txData.token_info) : null,
          timestamp: Number(txData.block.timestamp),
          block_height: Number(txData.block_height),
        }),
      ),
    );
  }

  if (latestBlock) {
    let nodeStats: Partial<NodeBlockPayload> | undefined;
    try {
      nodeStats = await fetchNodeJson<Partial<NodeBlockPayload>>("/network/stats");
    } catch (err) {
      console.warn("⚠️ Unable to refresh stats cache from node during hydration:", err);
    }
    await redis.set(
      LATEST_STATS_KEY,
      JSON.stringify(toStatsCache(dbBlockToPayload(latestBlock), nodeStats)),
    );
  }
}

async function indexBlockPayload(payload: NodeBlockPayload, skipAccountUpdate = false) {
  const height = BigInt(payload.height);
  const hash = payload.hash;
  const txCount = payload.tx_count ?? payload.transactions?.length ?? 0;
  const timestamp = BigInt(payload.timestamp);
  const leader = payload.leader;
  const leaderName = payload.leader_name || "";
  const circulatingSupply = payload.circulating_supply || "0";
  const totalSupply = payload.total_supply || "0";
  const confirmedTps = payload.confirmed_tps ?? 0;
  const inboundTps = payload.inbound_tps ?? 0;
  const totalTransactions = BigInt(payload.total_transactions ?? 0);
  const consensusTimeMs = BigInt(payload.consensus_time_ms ?? 0);
  const commitTimeMs = payload.commit_time_ms ?? 0;
  const blockTimeMs = payload.block_time_ms ?? 0;
  const aups = payload.aups ?? 0;
  const persistenceLag = BigInt(payload.persistence_lag ?? 0);
  const reward = payload.reward || "0";
  const fees = payload.fees || "0";
  const parentQcJson = payload.parent_qc ? JSON.stringify(payload.parent_qc) : null;
 
  await prisma.$transaction(async (tx) => {
    await tx.block.upsert({
      where: { height },
      update: {
        hash,
        tx_count: txCount,
        timestamp,
        leader,
        leader_name: leaderName,
        circulating_supply: circulatingSupply,
        total_supply: totalSupply,
        confirmed_tps: confirmedTps,
        inbound_tps: inboundTps,
        total_transactions: totalTransactions,
        consensus_time_ms: consensusTimeMs,
        commit_time_ms: commitTimeMs,
        block_time_ms: blockTimeMs,
        aups,
        persistence_lag: persistenceLag,
        reward,
        fees,
        parent_qc: parentQcJson,
      },
      create: {
        height,
        hash,
        tx_count: txCount,
        timestamp,
        leader,
        leader_name: leaderName,
        circulating_supply: circulatingSupply,
        total_supply: totalSupply,
        confirmed_tps: confirmedTps,
        inbound_tps: inboundTps,
        total_transactions: totalTransactions,
        consensus_time_ms: consensusTimeMs,
        commit_time_ms: commitTimeMs,
        block_time_ms: blockTimeMs,
        aups,
        persistence_lag: persistenceLag,
        reward,
        fees,
        parent_qc: parentQcJson,
      },
    });
 
    if (payload.transactions && Array.isArray(payload.transactions)) {
      for (const txData of payload.transactions) {
        const tokenInfoJson = txData.token_info ? JSON.stringify(txData.token_info) : null;
        const nonce = BigInt(txData.nonce ?? 0);
        await tx.transaction.upsert({
          where: { hash: txData.hash },
          update: {
            block_height: height,
            from: txData.from,
            from_name: txData.from_name || "",
            to: txData.to,
            to_name: txData.to_name || "",
            value: txData.value || "0",
            nonce,
            status: txData.status || "SUCCESS",
            method: txData.method || "TRANSFER",
            token_info: tokenInfoJson,
          },
          create: {
            hash: txData.hash,
            block_height: height,
            from: txData.from,
            from_name: txData.from_name || "",
            to: txData.to,
            to_name: txData.to_name || "",
            value: txData.value || "0",
            nonce,
            status: txData.status || "SUCCESS",
            method: txData.method || "TRANSFER",
            token_info: tokenInfoJson,
          },
        });
      }
    }
  });

  console.log(`📥 Indexed Block #${height} successfully into PostgreSQL`);

  // Keep the explorer's local account registry in sync without hammering /balance on the node.
  if (!skipAccountUpdate) {
    const addresses = new Set<string>();
    if (payload.leader) addresses.add(payload.leader);
    if (payload.transactions && Array.isArray(payload.transactions)) {
      for (const txData of payload.transactions) {
        if (txData.from) addresses.add(txData.from);
        if (txData.to) addresses.add(txData.to);
      }
    }

    (async () => {
      await upsertObservedAccounts(addresses, height);

      if (!LIVE_NODE_ACCOUNT_SYNC_ENABLED) {
        return;
      }

      for (const addr of addresses) {
        await upsertAccountStateFromNode(addr, height);
      }
    })().catch(err => {
      console.error("❌ Background account status update error:", err);
    });
  }
}

async function bootstrapHistoricalBlocks() {
  let networkStats: Partial<NodeBlockPayload>;
  try {
    networkStats = await fetchNodeJson<Partial<NodeBlockPayload>>("/network/stats");
  } catch (err) {
    console.warn("⚠️ Skipping startup backfill because node stats are unavailable:", err);
    return;
  }

  const latestIndexedBlock = await prisma.block.findFirst({
    orderBy: { height: "desc" },
    select: { height: true },
  });
  const startHeight = latestIndexedBlock ? Number(latestIndexedBlock.height) + 1 : 0;
  const latestNodeHeight = Number(networkStats.total_height ?? 0);

  if (startHeight > latestNodeHeight) {
    console.log("✅ Explorer index is already at the latest known height");
    return;
  }

  console.log(`🧱 Backfilling blocks #${startHeight}..#${latestNodeHeight} from node RPC...`);
  for (let height = startHeight; height <= latestNodeHeight; height++) {
    try {
      const block = await fetchBlockWithRetry(height);
      const payload: NodeBlockPayload = {
        ...block,
        type: "sync_block",
        avg_fee: networkStats.avg_fee,
        active_nodes: networkStats.active_nodes,
        chain_id: networkStats.chain_id,
      };
      await indexBlockPayload(payload, true); // skip account updates during backfill
      await cacheBlockPayload(payload, networkStats);
    } catch (err) {
      console.error(`❌ Failed to backfill block #${height}:`, err);
      break;
    }
  }
}

async function main() {
  console.log("⚡ Starting Lumina Indexer service...");
  console.log(
    `👤 Explorer account sync mode: ${LIVE_NODE_ACCOUNT_SYNC_ENABLED ? "live_node_balance" : "observed_only"}`,
  );

  // Connect to Kafka producer & consumer
  try {
    await connectKafka();
    await ensureTopicExists(BLOCKS_TOPIC);
  } catch (err) {
    console.error("❌ Failed to connect to Kafka. Make sure Kafka is running:", err);
    process.exit(1);
  }

  // Subscribe consumer to block ingestion topic
  await consumer.subscribe({ topic: BLOCKS_TOPIC, fromBeginning: true });

  // Run Kafka Consumer to save block data to PostgreSQL
  consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const payloadStr = message.value.toString();
        const payload = JSON.parse(payloadStr) as NodeBlockPayload;

        if (payload.type === "new_block" || payload.type === "sync_block") {
          await indexBlockPayload(payload);
          await cacheBlockPayload(payload);
        }
      } catch (err) {
        console.error("❌ Kafka Consumer Error:", err);
      }
    },
  });

  await hydrateCachesFromDatabase();
  await bootstrapObservedAccounts();

  // Start WebSocket client to ingest data from Lumina Node
  startWebSocketIngestion();

  await bootstrapHistoricalBlocks();
  await hydrateCachesFromDatabase();
}

function startWebSocketIngestion() {
  console.log(`🔌 Connecting to Lumina Node WSS at ${NODE_WSS_URL}...`);
  const ws = new WebSocket(NODE_WSS_URL);

  ws.on("open", () => {
    console.log("🟢 Connected to Lumina Node Explorer WSS stream");
  });

  ws.on("message", async (data) => {
    try {
      const msg = JSON.parse(data.toString()) as NodeBlockPayload;
      if (msg.type === "new_block" || msg.type === "sync_block") {
        console.log(`📦 Received block #${msg.height} from Node. Ingesting...`);

        await cacheBlockPayload(msg);

        // 2. Kirim block lengkap ke Kafka untuk di-index secara asinkron
        await producer.send({
          topic: BLOCKS_TOPIC,
          messages: [{ value: JSON.stringify(msg) }],
        });

        // 3. Pub ke Redis untuk Express WebSocket server menyiarkan ke frontend
        if (msg.type === "new_block") {
          await redisPub.publish("lumina:block_channel", JSON.stringify(msg));
        }
      }
    } catch (err) {
      console.error("❌ Failed to process WS message:", err);
    }
  });

  ws.on("close", () => {
    console.warn("⚠️ WebSocket connection closed. Reconnecting in 5 seconds...");
    setTimeout(startWebSocketIngestion, 5000);
  });

  ws.on("error", (err) => {
    console.error("❌ WebSocket error:", err);
  });
}

main().catch((err) => console.error("❌ Critical Indexer Error:", err));
