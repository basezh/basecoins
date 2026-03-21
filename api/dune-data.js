const DUNE_QUERY_URL =
  'https://api.dune.com/api/v1/query/6728582/results?limit=1000';

function rowsFromDuneBody(data) {
  return (data?.result?.rows ?? []).map((r) => ({
    symbol: r.symbol,
    name: r.name,
    address: (r.token_address || '').toLowerCase(),
    source: (r.source || '').toLowerCase(),
  }));
}

async function pullDuneRows(apiKey) {
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

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    res.status(405).json({ error: 'Method not allowed' });
    return;
  }

  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'private, no-store');

  const apiKey = process.env.DUNE_API_KEY;
  if (!apiKey) {
    res.status(503).json({
      error: 'Server missing DUNE_API_KEY',
      rows: [],
    });
    return;
  }

  try {
    const rows = await pullDuneRows(apiKey);
    res.status(200).json({
      rows,
      updatedAt: String(Date.now()),
    });
  } catch (e) {
    console.error(e);
    res.status(502).json({ error: e.message, rows: [] });
  }
}
