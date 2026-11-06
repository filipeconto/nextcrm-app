import { createHash } from "crypto";
import OpenAI from "openai";

export function buildEmbeddingText(fields: (string | null | undefined)[]): string {
  return fields
    .filter((f): f is string => typeof f === "string" && f.trim().length > 0)
    .join(" ");
}

export function computeContentHash(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

export async function generateEmbedding(text: string): Promise<number[]> {
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) {
    throw new Error("Embeddings unavailable: configure OPENAI_API_KEY to enable this feature.");
  }
  const openai = new OpenAI({ apiKey });
  const response = await openai.embeddings.create({
    model: "text-embedding-3-small",
    input: text,
  });
  return response.data[0].embedding;
}

export function toVectorLiteral(embedding: number[]): string {
  return `[${embedding.join(",")}]`;
}
