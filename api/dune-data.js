import {
  createRedis,
  readCache,
  pullDuneRows,
  writeCache,
} from '../lib/dune-cache.js';

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    res.status(405).json({ error: 'Method not allowed' });
    return;
  }

  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader(
    'Cache-Control',
    'public, s-maxage=60, stale-while-revalidate=300'
  );

  const redis = createRedis();

  try {
    if (redis) {
      const cached = await readCache(redis);
      if (cached?.rows?.length) {
        res.status(200).json({
          rows: cached.rows,
          updatedAt: cached.updatedAt,
        });
        return;
      }
    }

    const apiKey = process.env.DUNE_API_KEY;
    if (!apiKey) {
      res.status(503).json({
        error: 'Server missing DUNE_API_KEY',
        rows: [],
      });
      return;
    }

    const rows = await pullDuneRows(apiKey);
    if (redis) {
      await writeCache(redis, rows);
    }

    res.status(200).json({
      rows,
      updatedAt: String(Date.now()),
      fromLive: true,
    });
  } catch (e) {
    console.error(e);
    res.status(502).json({ error: e.message, rows: [] });
  }
}
