import fs from "fs";
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

async function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.error("Usage: npx ts-node src/seed-offline.ts <path_to_dump.json>");
    process.exit(1);
  }

  const dumpPath = args[0];
  console.log(`📖 Reading dump file from ${dumpPath}...`);
  const rawData = fs.readFileSync(dumpPath, "utf-8");
  const data = JSON.parse(rawData);

  console.log(`📦 Loaded ${data.blocks.length} blocks and ${data.accounts.length} accounts.`);

  // 1. Clear database
  console.log("🧹 Clearing old blocks, transactions, and accounts from PostgreSQL...");
  await prisma.$executeRawUnsafe(`TRUNCATE TABLE "Transaction", "Block", "Account" CASCADE;`);
  console.log("🧹 Old data cleared.");

  // 2. Insert blocks & transactions
  console.log("🧱 Inserting blocks and transactions in transactions...");
  let totalTxs = 0;
  
  const chunkSize = 100;
  for (let i = 0; i < data.blocks.length; i += chunkSize) {
    const chunk = data.blocks.slice(i, i + chunkSize);
    
    await prisma.$transaction(async (tx) => {
      for (const block of chunk) {
        await tx.block.create({
          data: {
            height: BigInt(block.height),
            hash: block.hash,
            tx_count: block.tx_count,
            timestamp: BigInt(block.timestamp),
            leader: block.leader,
            leader_name: "",
            circulating_supply: block.circulating_supply,
            total_supply: block.total_supply,
            confirmed_tps: 0,
            inbound_tps: 0,
            total_transactions: BigInt(0),
            consensus_time_ms: BigInt(0),
            commit_time_ms: 0,
            block_time_ms: 0,
            aups: 0,
            persistence_lag: BigInt(0),
            reward: block.reward,
            fees: block.fees,
            parent_qc: null,
            transactions: {
              create: block.transactions.map((txData: any) => {
                totalTxs++;
                return {
                  hash: txData.hash,
                  from: txData.from,
                  from_name: "",
                  to: txData.to,
                  to_name: "",
                  value: txData.value,
                  nonce: BigInt(txData.nonce),
                  status: txData.status,
                  method: txData.method,
                  token_info: null,
                };
              }),
            },
          },
        });
      }
    });
    console.log(`🧱 Progress: Block #${chunk[chunk.length - 1].height} inserted...`);
  }
  console.log(`✅ Blocks and transactions inserted! Total: ${data.blocks.length} blocks, ${totalTxs} transactions.`);

  // 3. Insert accounts
  console.log("👤 Inserting accounts state...");
  for (let i = 0; i < data.accounts.length; i += chunkSize) {
    const chunk = data.accounts.slice(i, i + chunkSize);
    await prisma.account.createMany({
      data: chunk.map((acc: any) => ({
        address: acc.address,
        balance: acc.balance,
        staked: acc.staked,
        nonce: BigInt(acc.nonce),
        name: acc.name,
        is_validator: acc.is_validator,
        validator_status: acc.validator_status,
        updated_at_block: BigInt(data.latest_height),
      })),
      skipDuplicates: true,
    });
  }
  console.log(`✅ Accounts inserted! Total: ${data.accounts.length} accounts.`);
  console.log("✨ Seeding completed successfully!");
}

main()
  .catch((e) => {
    console.error("❌ Seeding failed:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
