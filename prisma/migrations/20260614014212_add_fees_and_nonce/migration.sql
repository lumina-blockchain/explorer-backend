-- AlterTable
ALTER TABLE "Block" ADD COLUMN     "fees" TEXT NOT NULL DEFAULT '0';

-- AlterTable
ALTER TABLE "Transaction" ADD COLUMN     "nonce" BIGINT NOT NULL DEFAULT 0;
