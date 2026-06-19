import prisma from "./db";

async function main() {
  const blockCount = await prisma.block.count();
  const txCount = await prisma.transaction.count();
  console.log(`📊 DB Block Count: ${blockCount}`);
  console.log(`📊 DB Transaction Count: ${txCount}`);
  
  const blocksWithTxCount = await prisma.block.count({
    where: { tx_count: { gt: 0 } }
  });
  console.log(`📊 DB Blocks with tx_count > 0: ${blocksWithTxCount}`);

  if (blocksWithTxCount > 0) {
    const sample = await prisma.block.findFirst({
      where: { tx_count: { gt: 0 } },
      orderBy: { height: "desc" },
    });
    console.log(`📦 Sample Block with TXs: Height #${sample?.height.toString()}, Hash: ${sample?.hash}, TxCount: ${sample?.tx_count}`);
  }
}

main().catch(console.error).finally(() => prisma.$disconnect());
