import { Kafka } from "kafkajs";
import dotenv from "dotenv";

dotenv.config();

const brokers = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");

export const kafka = new Kafka({
  clientId: "lumina-indexer",
  brokers,
});

export const producer = kafka.producer();
export const consumer = kafka.consumer({ groupId: "lumina-indexer-group" });

export async function ensureTopicExists(topic: string) {
  const admin = kafka.admin();
  try {
    await admin.connect();
    const topics = await admin.listTopics();
    if (!topics.includes(topic)) {
      console.log(`Creating Kafka topic: ${topic}...`);
      await admin.createTopics({
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      });
      console.log(`Kafka topic ${topic} created successfully.`);
    }
  } catch (err) {
    console.error(`Failed to ensure topic ${topic} exists:`, err);
  } finally {
    await admin.disconnect();
  }
}

export async function connectKafka() {
  await producer.connect();
  console.log("🚀 Kafka Producer Connected");
  await consumer.connect();
  console.log("🚀 Kafka Consumer Connected");
}
export async function disconnectKafka() {
  await producer.disconnect();
  await consumer.disconnect();
}
