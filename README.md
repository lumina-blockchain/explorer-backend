# Lumina Block Explorer Backend

Layanan backend untuk Lumina L1 Blockchain Explorer. Repositori ini berisi **Block Indexer** (untuk menyerap data block dari Node RPC ke database) dan **REST/WebSocket API Server** (untuk menyajikan data transaksi, address, block, validator, dan metrik jaringan ke frontend Explorer).

## Fitur Utama

- **Real-time Block Indexing:** Menangkap setiap block baru yang diproduksi di blockchain Lumina dan menyimpannya secara terstruktur ke PostgreSQL.
- **REST API Server:** Menyediakan endpoint publik untuk queries data address, saldo, transaksi, block, metadata contract, dan statistik validator.
- **WebSocket Gateway:** Mendukung pembaruan data real-time via WebSocket (misalnya, block terbaru, transaksi terbaru, dan data telemetry visualizer).
- **Event Streaming & Caching:** Mengintegrasikan **Apache Kafka** untuk pemrosesan event antrean transaksi dan **Redis** untuk caching performa tinggi guna mereduksi beban kueri database.
- **Database ORM:** Menggunakan **Prisma ORM** untuk manajemen schema database dan migrasi PostgreSQL.

---

## Persyaratan Sistem

Pastikan Anda telah menginstal dependensi berikut:
- **Node.js** (v18 atau lebih baru)
- **PostgreSQL** (Database utama)
- **Redis** (Untuk caching)
- **Apache Kafka** (Untuk event streaming)

*Tip: Anda bisa menggunakan `docker-compose.yml` di repositori core `lumina-node` untuk menyalakan PostgreSQL, Redis, dan Kafka secara instan via Docker.*

---

## Memulai Pengembangan

### 1. Instalasi Dependensi
Jalankan perintah berikut di direktori root backend:
```bash
npm install
```

### 2. Konfigurasi Environment Variables
Buat file `.env` di root direktori dengan menyalin format berikut:
```env
PORT=4000
DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:5432/lumina_explorer?schema=public"
REDIS_URL="redis://127.0.0.1:6379"
KAFKA_BROKERS="127.0.0.1:9092"

# URL ke RPC Node Lumina
NODE_RPC_URL="https://rpc1.bariscode.my.id"
NODE_WSS_URL="wss://rpc1.bariscode.my.id/explorer/ws"
```

### 3. Inisialisasi Database
Jalankan perintah berikut untuk meng-generate Prisma client dan menjalankan migrasi database:
```bash
# Generate Prisma Client
npm run prisma:generate

# Jalankan Migrasi
npm run prisma:migrate
```

### 4. Menjalankan Layanan

Buka dua terminal terpisah:

- **Terminal 1: Menjalankan Indexer** (Menyinkronkan block dari blockchain)
  ```bash
  npm run dev:indexer
  ```

- **Terminal 2: Menjalankan API Server** (Menyajikan data ke Frontend)
  ```bash
  npm run dev:server
  ```
  Layanan API Server akan berjalan di `http://localhost:4000`.

---

## Struktur Folder

```text
├── prisma/             # Schema Prisma & berkas migrasi database
├── src/
│   ├── indexer.ts      # Logika utama sinkronisasi block dari node RPC
│   ├── server.ts       # Express/Node API Server & WebSocket Handler
│   ├── db.ts           # Koneksi Prisma Client
│   ├── redis.ts        # Client & Wrapper Caching Redis
│   └── kafka.ts        # Produser & Konsumer Event Kafka
├── tsconfig.json       # Konfigurasi TypeScript
└── package.json
```
