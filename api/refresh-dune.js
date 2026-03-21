import { createRedis, pullAndCache } from '../lib/dune-cache.js';

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    res.status(405).json({ error: 'Method not allowed' });
    return;
  }

  const secret = process.env.CRON_SECRET;
  const auth = req.headers.authorization || '';
  if (!secret || auth !== `Bearer ${secret}`) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }

  const apiKey = process.env.DUNE_API_KEY;
  if (!apiKey) {
    res.status(500).json({ error: 'DUNE_API_KEY not set' });
    return;
  }

  const redis = createRedis();
  if (!redis) {
    res.status(200).json({
      ok: true,
      skipped: true,
      message:
        'Redis not set; hourly cache disabled. dune-data still calls Dune per request.',
    });
    return;
  }

  try {
    const rows = await pullAndCache(apiKey, redis);
    res.status(200).json({ ok: true, count: rows.length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
}
