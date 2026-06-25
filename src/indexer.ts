import WebSocket from "ws";
import dotenv from "dotenv";
import fs from "fs";
import path from "path";
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
  data?: string;
  signature?: string;
  pubkey?: string;
  fee?: string;
  fee_payer?: string;
  fee_payer_name?: string;
  fee_payer_signature?: string;
  fee_payer_pubkey?: string;
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

function parseTokenInfo(raw: any): any {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
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
}

async function upsertAccountStateFromNode(address: string, currentBlockHeight: bigint) {
  if (!address || address.length < 5) return;
  try {
    const acc = await fetchNodeJson<any>(`/balance/${address}`);
    const vesting = acc.vesting_schedule;
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
        stake_height: BigInt(acc.stake_height ?? 0),
        vesting_initial_amount: vesting?.initial_amount || "0",
        vesting_lock_height: BigInt(vesting?.lock_height ?? 0),
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
        stake_height: BigInt(acc.stake_height ?? 0),
        vesting_initial_amount: vesting?.initial_amount || "0",
        vesting_lock_height: BigInt(vesting?.lock_height ?? 0),
      },
    });
  } catch (err: any) {
    console.warn(`⚠️ Failed to update account info for ${address}: ${err.message || err}`);
  }
}

async function registerGenesisAccounts() {
  console.log("📂 Registering genesis accounts in database...");
  const searchPaths = [
    process.env.GENESIS_PATH,
    path.join(__dirname, "../genesis.json"),
    path.join(__dirname, "../../genesis.json"),
    "/home/ubuntu/lumina-node/genesis.json",
    "./genesis.json"
  ].filter(Boolean) as string[];

  let genesisData: any = null;
  for (const p of searchPaths) {
    try {
      const resolvedPath = path.resolve(p);
      if (fs.existsSync(resolvedPath)) {
        console.log(`📖 Found genesis file at: ${resolvedPath}`);
        const content = fs.readFileSync(resolvedPath, "utf-8");
        genesisData = JSON.parse(content);
        break;
      }
    } catch (err) {
      // ignore and try next path
    }
  }

  if (!genesisData) {
    console.warn("⚠️ genesis.json not found in any search path. Skipping genesis accounts registration.");
    return;
  }

  const genesisAddresses = new Set<string>();

  // Extract from initial_balances
  if (Array.isArray(genesisData.initial_balances)) {
    for (const entry of genesisData.initial_balances) {
      if (entry.address) genesisAddresses.add(entry.address);
    }
  }

  // Extract from validators
  if (Array.isArray(genesisData.validators)) {
    for (const validator of genesisData.validators) {
      if (validator.address) genesisAddresses.add(validator.address);
    }
  }

  // Extract from vesting_schedules
  if (Array.isArray(genesisData.vesting_schedules)) {
    for (const schedule of genesisData.vesting_schedules) {
      if (schedule.address) genesisAddresses.add(schedule.address);
    }
  }

  console.log(`👤 Found ${genesisAddresses.size} unique genesis accounts.`);

  if (genesisAddresses.size === 0) return;

  const bootstrapHeight = BigInt(0);
  const addressList = Array.from(genesisAddresses);
  
  try {
    const enrichedGenesisAccounts = await Promise.all(
      addressList.map(async (addr) => {
        try {
          const acc = await fetchNodeJson<any>(`/balance/${addr}`);
          return {
            address: addr,
            balance: acc.balance || "0",
            staked: acc.staked || "0",
            nonce: BigInt(acc.nonce ?? 0),
            name: acc.name || "",
            is_validator: Boolean(acc.is_validator),
            validator_status: acc.validator_status || "None",
            updated_at_block: bootstrapHeight,
            stake_height: BigInt(acc.stake_height ?? 0),
            vesting_initial_amount: acc.vesting_schedule?.initial_amount || "0",
            vesting_lock_height: BigInt(acc.vesting_schedule?.lock_height ?? 0),
          };
        } catch (err: any) {
          console.warn(`⚠️ Failed to fetch balance for genesis account ${addr}:`, err.message || err);
          return {
            address: addr,
            balance: "0",
            staked: "0",
            nonce: BigInt(0),
            name: "",
            is_validator: false,
            validator_status: "None",
            updated_at_block: bootstrapHeight,
            stake_height: BigInt(0),
            vesting_initial_amount: "0",
            vesting_lock_height: BigInt(0),
          };
        }
      })
    );

    // Upsert each enriched account into database
    for (const acc of enrichedGenesisAccounts) {
      await prisma.account.upsert({
        where: { address: acc.address },
        update: {
          balance: acc.balance,
          staked: acc.staked,
          nonce: acc.nonce,
          name: acc.name,
          is_validator: acc.is_validator,
          validator_status: acc.validator_status,
          updated_at_block: acc.updated_at_block,
          stake_height: acc.stake_height,
          vesting_initial_amount: acc.vesting_initial_amount,
          vesting_lock_height: acc.vesting_lock_height,
        },
        create: acc,
      });
    }
    console.log("✅ Genesis accounts registered and sync'ed with live node states in database.");
  } catch (err: any) {
    console.error("❌ Failed to register genesis accounts in database:", err.message || err);
  }
}

async function bootstrapObservedAccounts() {
  console.log("👤 Checking observed account registry in PostgreSQL...");
  try {
    const hasAccounts = await prisma.account.findFirst();
    if (hasAccounts) {
      console.log("👤 Account registry already bootstrapped. Skipping bootstrap registry query.");
      return;
    }

    console.log("👤 Bootstrapping observed account registry from PostgreSQL (first time setup)...");
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
    nonce: Number(txData.nonce ?? 0),
    status: txData.status || "SUCCESS",
    method: txData.method || "TRANSFER",
    token_info: txData.token_info ?? null,
    timestamp: Number(payload.timestamp),
    block_height: Number(payload.height),
    data: txData.data || "",
    signature: txData.signature || "",
    pubkey: txData.pubkey || "",
    fee: txData.fee || "0",
    fee_payer: txData.fee_payer || null,
    fee_payer_name: txData.fee_payer_name || null,
    fee_payer_signature: txData.fee_payer_signature || null,
    fee_payer_pubkey: txData.fee_payer_pubkey || null,
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
      // Clean from mempool cache
      await redis.del(`lumina:mempool:${txData.hash}`);
      await redis.srem("lumina:mempool_hashes", txData.hash);
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
        let tokenRecipient: string | null = null;
        if (txData.token_info) {
          const parsed = parseTokenInfo(txData.token_info);
          if (parsed && parsed.token_recipient) {
            tokenRecipient = parsed.token_recipient;
          }
        }
        const rawMethod = txData.method || "";
        const rawData = txData.data || "";
        // Flexible method detection: use explicit method first, fallback to data sniff
        let resolvedMethod = rawMethod;
        if (!resolvedMethod || resolvedMethod === "TRANSFER") {
          const dataUpper = rawData.toUpperCase();
          if (dataUpper.startsWith("4445504c4f593a") || dataUpper.startsWith("DEPLOY:")) resolvedMethod = "DEPLOY";
          else if (dataUpper.startsWith("5354414b453a") || dataUpper.startsWith("STAKE:")) resolvedMethod = "STAKE";
          else if (dataUpper.startsWith("5553544b3a") || dataUpper.startsWith("UNSTAKE:") || dataUpper.startsWith("554e5354414b45")) resolvedMethod = "UNSTAKE";
          else if (dataUpper.startsWith("43414c4c3a") || dataUpper.startsWith("CALL:")) resolvedMethod = "CALL";
          else if (rawData.length > 0) resolvedMethod = "TRANSFER"; // has data but unknown
          else resolvedMethod = "TRANSFER";
        }
        // Also detect DEPLOY from status format
        const statusUpper = (txData.status || "").toUpperCase();
        if (statusUpper.startsWith("SUCCESS:CID:") || statusUpper.startsWith("SUCCESS_CID:")) {
          resolvedMethod = "DEPLOY";
        }
        const nonce = BigInt(txData.nonce ?? 0);
        // Extract contract ID from status field flexibly
        let deployedContractId: string | null = null;
        const statusRaw = txData.status || "";
        const cidMatch = statusRaw.match(/(?:SUCCESS|success):CID:([^:]+)/i) || statusRaw.match(/(?:SUCCESS|success)_CID:([^:]+)/i);
        if (cidMatch && cidMatch[1]) {
          deployedContractId = cidMatch[1].trim();
        }
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
            method: resolvedMethod,
            token_info: tokenInfoJson,
            token_recipient: tokenRecipient,
            data: txData.data || "",
            signature: txData.signature || "",
            pubkey: txData.pubkey || "",
            fee: txData.fee || "0",
            fee_payer: txData.fee_payer || null,
            fee_payer_name: txData.fee_payer_name || null,
            fee_payer_signature: txData.fee_payer_signature || null,
            fee_payer_pubkey: txData.fee_payer_pubkey || null,
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
            method: resolvedMethod,
            token_info: tokenInfoJson,
            token_recipient: tokenRecipient,
            data: txData.data || "",
            signature: txData.signature || "",
            pubkey: txData.pubkey || "",
            fee: txData.fee || "0",
            fee_payer: txData.fee_payer || null,
            fee_payer_name: txData.fee_payer_name || null,
            fee_payer_signature: txData.fee_payer_signature || null,
            fee_payer_pubkey: txData.fee_payer_pubkey || null,
          },
        });

        // Index contract if deployed — support multiple status formats
        if (deployedContractId) {
          const cid = deployedContractId;
          let bytecode = "";
          if (txData.data) {
            const hexPrefix = "4445504c4f593a"; // "DEPLOY:" in hex
            const dataLower = txData.data.toLowerCase();
            if (dataLower.startsWith(hexPrefix)) {
              bytecode = txData.data.substring(hexPrefix.length);
            } else if (txData.data.toUpperCase().startsWith("DEPLOY:")) {
              bytecode = txData.data.substring(7);
            } else {
              bytecode = txData.data;
            }
          }

          const tokenInfo = parseTokenInfo(txData.token_info);
          await tx.contract.upsert({
            where: { address: cid },
            update: {
              tx_hash: txData.hash,
              block_height: height,
              bytecode,
              name: tokenInfo?.token_name || null,
              symbol: tokenInfo?.token_symbol || null,
              decimals: tokenInfo?.decimals !== undefined ? Number(tokenInfo.decimals) : null,
              total_supply: tokenInfo?.token_amount || null,
              owner: tokenInfo?.owner || txData.from,
              logo: tokenInfo?.logo || null,
              created_at: timestamp,
            },
            create: {
              address: cid,
              tx_hash: txData.hash,
              block_height: height,
              bytecode,
              name: tokenInfo?.token_name || null,
              symbol: tokenInfo?.token_symbol || null,
              decimals: tokenInfo?.decimals !== undefined ? Number(tokenInfo.decimals) : null,
              total_supply: tokenInfo?.token_amount || null,
              owner: tokenInfo?.owner || txData.from,
              logo: tokenInfo?.logo || null,
              created_at: timestamp,
            }
          });
          console.log(`📄 Indexed contract ${cid} from tx ${txData.hash.substring(0, 12)}...`);
        }
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
  await registerGenesisAccounts();

  // Start WebSocket client to ingest data from Lumina Node
  startWebSocketIngestion();

  await bootstrapHistoricalBlocks();
  await hydrateCachesFromDatabase();

  // Periodically fetch and cache network validators and peers
  setInterval(async () => {
    try {
      const validators = await fetchNodeJson<any>("/network/validators");
      if (validators && validators.validators) {
        await redis.set("lumina:network:validators", JSON.stringify(validators), "EX", 30);
      }
    } catch (err) {
      console.warn("⚠️ Background validator sync failed:", err);
    }

    try {
      const peers = await fetchNodeJson<any>("/network/peers");
      if (peers && peers.peers) {
        await redis.set("lumina:network:peers", JSON.stringify(peers), "EX", 30);
      }
    } catch (err) {
      console.warn("⚠️ Background peer sync failed:", err);
    }
  }, 10000); // every 10 seconds
}

function startWebSocketIngestion() {
  console.log(`🔌 Connecting to Lumina Node WSS at ${NODE_WSS_URL}...`);
  const ws = new WebSocket(NODE_WSS_URL);

  ws.on("open", () => {
    console.log("🟢 Connected to Lumina Node Explorer WSS stream");
  });

  ws.on("message", async (data) => {
    try {
      const msg = JSON.parse(data.toString()) as any;
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
      } else if (msg.type === "batch_transactions" && Array.isArray(msg.transactions)) {
        // Cache these transactions in Redis
        for (const tx of msg.transactions) {
          const txKey = `lumina:mempool:${tx.hash}`;
          const method = tx.method || (tx.data && tx.data.startsWith("STAKE") ? "STAKE" : "TRANSFER");
          const txSummary = {
            hash: tx.hash,
            from: tx.from,
            from_name: tx.from_name || "",
            to: tx.to,
            to_name: tx.to_name || "",
            value: tx.value || "0",
            nonce: Number(tx.nonce ?? 0),
            status: "Pending",
            method,
            token_info: tx.token_info || null,
            timestamp: Math.floor(Date.now() / 1000)
          };
          await redis.set(txKey, JSON.stringify(txSummary), "EX", 300); // 5 minutes TTL
          await redis.sadd("lumina:mempool_hashes", tx.hash);
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
