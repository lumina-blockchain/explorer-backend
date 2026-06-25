#!/bin/bash
# ============================================================
# reset_db.sh — Reset semua data indexer explorer dari nol
# Usage: bash reset_db.sh
# ============================================================
set -e

CONTAINER="lumina-postgres"
DB_USER="postgres"
DB_NAME="lumina_explorer"
REDIS_CONTAINER="lumina-redis"   # ganti jika nama container Redis berbeda

echo "========================================="
echo "  BIGCHAIN Explorer — DB Reset Script"
echo "========================================="

# --- 1. Truncate semua tabel PostgreSQL ---
echo ""
echo "[1/3] Truncating semua tabel di database '$DB_NAME'..."

sudo docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" <<'SQL'
DO $$
BEGIN
  -- Disable semua FK constraint sementara
  EXECUTE (
    SELECT 'TRUNCATE TABLE ' || string_agg(format('"%s"', tablename), ', ') || ' RESTART IDENTITY CASCADE'
    FROM pg_tables
    WHERE schemaname = 'public'
      AND tablename IN ('Transaction', 'Block', 'Account', 'Contract')
  );
  RAISE NOTICE 'Semua tabel berhasil di-truncate.';
END $$;
SQL

echo "  ✅ PostgreSQL: Semua tabel (Transaction, Block, Account, Contract) berhasil di-truncate."

# --- 2. Reset Redis state keys ---
echo ""
echo "[2/3] Resetting Redis state keys..."

# Cek apakah container Redis tersedia
if sudo docker ps --format '{{.Names}}' | grep -q "^${REDIS_CONTAINER}$"; then
  sudo docker exec -i "$REDIS_CONTAINER" redis-cli DEL \
    "lumina:latest_stats" \
    "lumina:latest_blocks" \
    "lumina:latest_txs" \
    "lumina:total_accounts" \
    > /dev/null 2>&1 && echo "  ✅ Redis: State keys berhasil di-reset." || echo "  ⚠️  Redis: Gagal reset (mungkin key belum ada, tidak masalah)."
else
  echo "  ⚠️  Redis container '$REDIS_CONTAINER' tidak ditemukan — skip Redis reset."
  echo "      Jika Redis berjalan tanpa Docker, jalankan manual:"
  echo "      redis-cli DEL lumina:latest_stats lumina:latest_blocks lumina:latest_txs lumina:total_accounts"
fi

# --- 3. Restart indexer ---
echo ""
echo "[3/3] Restarting indexer service..."

if command -v pm2 &>/dev/null; then
  pm2 restart indexer 2>/dev/null || pm2 restart all 2>/dev/null || echo "  ⚠️  pm2: Tidak ada proses 'indexer' — start manual jika perlu."
  echo "  ✅ pm2: Indexer di-restart."
elif sudo docker ps --format '{{.Names}}' | grep -q "indexer"; then
  sudo docker restart indexer
  echo "  ✅ Docker: Container indexer di-restart."
else
  echo "  ⚠️  Indexer tidak terdeteksi (pm2/docker). Restart manual jika perlu:"
  echo "      pm2 restart indexer  ATAU  sudo docker restart <container_name>"
fi

echo ""
echo "========================================="
echo "  ✅ Reset selesai! Indexer akan mulai"
echo "     indexing dari blok terbaru."
echo "========================================="
