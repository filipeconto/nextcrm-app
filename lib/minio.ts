import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

if (!process.env.MINIO_ENDPOINT) throw new Error("MINIO_ENDPOINT is not defined");
if (!process.env.MINIO_ACCESS_KEY) throw new Error("MINIO_ACCESS_KEY is not defined");
if (!process.env.MINIO_SECRET_KEY) throw new Error("MINIO_SECRET_KEY is not defined");
if (!process.env.MINIO_BUCKET) throw new Error("MINIO_BUCKET is not defined");

export function minioPublicEndpoint(env: NodeJS.ProcessEnv = process.env): string {
  return (
    env.MINIO_PUBLIC_ENDPOINT ||
    env.NEXT_PUBLIC_MINIO_ENDPOINT ||
    env.MINIO_ENDPOINT ||
    ""
  ).replace(/\/+$/, "");
}

function createClient(endpoint: string) {
  return new S3Client({
    endpoint,
    region: process.env.MINIO_REGION || "us-east-1",
    credentials: {
      accessKeyId: process.env.MINIO_ACCESS_KEY!,
      secretAccessKey: process.env.MINIO_SECRET_KEY!,
    },
    forcePathStyle: true,
  });
}

export const MINIO_BUCKET = process.env.MINIO_BUCKET;
export const MINIO_ENDPOINT = process.env.MINIO_ENDPOINT;
export const MINIO_PUBLIC_URL = minioPublicEndpoint();
export const minioClient = createClient(process.env.MINIO_ENDPOINT);
export const minioPresignClient =
  MINIO_PUBLIC_URL === process.env.MINIO_ENDPOINT ? minioClient : createClient(MINIO_PUBLIC_URL);

export function presignGet(key: string, expiresIn = 300): Promise<string> {
  return getSignedUrl(
    minioPresignClient,
    new GetObjectCommand({ Bucket: MINIO_BUCKET, Key: key }),
    { expiresIn },
  );
}

export function presignPut(key: string, contentType: string, expiresIn = 600): Promise<string> {
  return getSignedUrl(
    minioPresignClient,
    new PutObjectCommand({ Bucket: MINIO_BUCKET, Key: key, ContentType: contentType }),
    { expiresIn, signableHeaders: new Set(["content-type"]) },
  );
}
