import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const redisUrl = process.env.REDIS_URL || "redis://127.0.0.1:6379";

const redis = new Redis(redisUrl);
export const redisPub = new Redis(redisUrl);
export const redisSub = new Redis(redisUrl);

redis.on("error", (err) => console.error("Redis Error:", err));
redisPub.on("error", (err) => console.error("Redis Pub Error:", err));
redisSub.on("error", (err) => console.error("Redis Sub Error:", err));

export default redis;
