import { Redis } from '@upstash/redis';

export const DUNE_QUERY_URL =
  'https://api.dune.com/api/v1/query/6728582/results?limit=1000';

const ROWS_KEY = 'basecoin:dune_rows';
const AT_KEY = 'basecoin:dune_updated_at';

export function createRedis() {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  return new Redis({ url, token });
}

export function rowsFromDuneBody(data) {
  return (data?.result?.rows ?? []).map((r) => ({
    symbol: r.symbol,
    name: r.name,
    address: (r.token_address || '').toLowerCase(),
    source: (r.source || '').toLowerCase(),
  }));
}

export async function pullDuneRows(apiKey) {
  const res = await fetch(DUNE_QUERY_URL, {
    headers: { 'X-Dune-API-Key': apiKey },
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Dune HTTP ${res.status}: ${t.slice(0, 200)}`);
  }
  const data = await res.json();
  return rowsFromDuneBody(data);
}

export async function readCache(redis) {
  if (!redis) return null;
  const raw = await redis.get(ROWS_KEY);
  if (raw == null) return null;
  const rows = typeof raw === 'string' ? JSON.parse(raw) : raw;
  const updatedAt = await redis.get(AT_KEY);
  return { rows, updatedAt: updatedAt ?? null };
}

export async function writeCache(redis, rows) {
  if (!redis) return;
  await redis.set(ROWS_KEY, JSON.stringify(rows));
  await redis.set(AT_KEY, String(Date.now()));
}

export async function pullAndCache(apiKey, redis) {
  const rows = await pullDuneRows(apiKey);
  await writeCache(redis, rows);
  return rows;
}
