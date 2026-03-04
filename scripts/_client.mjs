import fs from 'node:fs';

export function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i];
    if (!k.startsWith('--')) continue;
    const key = k.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[++i] : 'true';
    out[key] = v;
  }
  return out;
}

export function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function pickCookieHeader(setCookieValues = []) {
  return setCookieValues
    .map((v) => String(v || '').split(';')[0])
    .filter(Boolean)
    .join('; ');
}

export async function createSession(baseUrl, cfg = {}) {
  const jar = { cookie: '' };

  async function request(path, init = {}) {
    const headers = { ...(init.headers || {}) };
    if (jar.cookie) headers.cookie = [headers.cookie, jar.cookie].filter(Boolean).join('; ');
    const res = await fetch(`${baseUrl}${path}`, { ...init, headers });
    const getSetCookie = res.headers.getSetCookie?.bind(res.headers);
    const setCookies = getSetCookie ? getSetCookie() : (res.headers.get('set-cookie') ? [res.headers.get('set-cookie')] : []);
    const merged = pickCookieHeader(setCookies);
    if (merged) {
      jar.cookie = [jar.cookie, merged].filter(Boolean).join('; ');
    }
    return res;
  }

  async function postJson(path, body) {
    const res = await request(path, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body || {})
    });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = { raw: text }; }
    if (!res.ok) throw new Error(`${path} failed: ${res.status} ${JSON.stringify(json)}`);
    return json;
  }

  async function putJson(path, body) {
    const res = await request(path, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body || {})
    });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = { raw: text }; }
    if (!res.ok) throw new Error(`${path} failed: ${res.status} ${JSON.stringify(json)}`);
    return json;
  }

  async function getJson(path) {
    const res = await request(path, { method: 'GET' });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = { raw: text }; }
    if (!res.ok) throw new Error(`${path} failed: ${res.status} ${JSON.stringify(json)}`);
    return json;
  }

  if (cfg.username && cfg.password) {
    await postJson('/api/auth/login', { username: cfg.username, password: cfg.password });
  }
  if (cfg.tenant_id) {
    await postJson('/api/tenant/select', { tenantId: cfg.tenant_id });
  }

  return { postJson, putJson, getJson };
}
