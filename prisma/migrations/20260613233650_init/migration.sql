-- CreateTable
CREATE TABLE "Block" (
    "height" BIGINT NOT NULL,
    "hash" TEXT NOT NULL,
    "tx_count" INTEGER NOT NULL,
    "timestamp" BIGINT NOT NULL,
    "leader" TEXT NOT NULL,
    "leader_name" TEXT NOT NULL,
    "circulating_supply" TEXT NOT NULL,
    "total_supply" TEXT NOT NULL,
    "confirmed_tps" DOUBLE PRECISION NOT NULL,
    "inbound_tps" DOUBLE PRECISION NOT NULL,
    "total_transactions" BIGINT NOT NULL,
    "consensus_time_ms" BIGINT NOT NULL,
    "commit_time_ms" DOUBLE PRECISION NOT NULL,
    "block_time_ms" DOUBLE PRECISION NOT NULL,
    "aups" DOUBLE PRECISION NOT NULL,
    "persistence_lag" BIGINT NOT NULL,
    "reward" TEXT NOT NULL,
    "parent_qc" TEXT,

    CONSTRAINT "Block_pkey" PRIMARY KEY ("height")
);

-- CreateTable
CREATE TABLE "Transaction" (
    "hash" TEXT NOT NULL,
    "block_height" BIGINT NOT NULL,
    "from" TEXT NOT NULL,
    "from_name" TEXT NOT NULL,
    "to" TEXT NOT NULL,
    "to_name" TEXT NOT NULL,
    "value" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "method" TEXT NOT NULL,
    "token_info" TEXT,

    CONSTRAINT "Transaction_pkey" PRIMARY KEY ("hash")
);

-- CreateIndex
CREATE UNIQUE INDEX "Block_hash_key" ON "Block"("hash");

-- AddForeignKey
ALTER TABLE "Transaction" ADD CONSTRAINT "Transaction_block_height_fkey" FOREIGN KEY ("block_height") REFERENCES "Block"("height") ON DELETE CASCADE ON UPDATE CASCADE;
