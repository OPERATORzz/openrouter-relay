const express = require('express');
const http = require('http');
const https = require('https');
const { WebSocketServer } = require('ws');
const net = require('net');
const tls = require('tls');
const dns = require('dns');

const app = express();
app.use(express.json({ limit: '50mb' }));

const GEMINI_KEY = process.env.GEMINI_API_KEY || '';
const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const GEMINI_HOST = 'generativelanguage.googleapis.com';

// 🚨 СПИСОК МОДЕЛЕЙ ОТ НОВОЙ К СТАРОЙ (ЕСЛИ ОДНА ЗАНЯТА)
const GEMINI_MODELS = ['gemini-3.8-flash', 'gemini-2.5-flash', 'gemini-1.5-flash-latest'];

const CHUNK_SIZE = 1;
const MAX_TOKENS = 2048;

function dohResolve(hostname) {
    return new Promise((resolve, reject) => {
        const servers = [ { ip: '1.1.1.1', name: 'cloudflare-dns.com' }, { ip: '8.8.8.8', name: 'dns.google' } ];
        let i = 0;
        const tryNext = () => {
            if (i >= servers.length) return reject(new Error('DoH failed: ' + hostname));
            const s = servers[i++];
            const req = https.get({ host: s.ip, servername: s.name, path: '/dns-query?name=' + hostname + '&type=A', headers: { accept: 'application/dns-json', host: s.name }, timeout: 8000 }, (r) => {
                let d = ''; r.on('data', c => d += c);
                r.on('end', () => {
                    try {
                        const j = JSON.parse(d); const ans = (j.Answer || []); const a = ans.find(x => x.type === 1);
                        if (a) return resolve(a.data);
                        const cn = ans.find(x => x.type === 5);
                        if (cn) return dohResolve(cn.data.replace(/\.$/, '')).then(resolve, reject); tryNext();
                    } catch (e) { tryNext(); }
                });
            });
            req.on('error', tryNext); req.on('timeout', () => { req.destroy(); tryNext(); });
        };
        tryNext();
    });
}

async function resolveAny(hostname) {
    try { return await dns.promises.lookup(hostname); } catch (e) { const ip = await dohResolve(hostname); return { address: ip }; }
}

function rawHttp(host, path, method, headers, body) {
    method = method || 'GET'; headers = headers || [];
    return new Promise((resolve, reject) => {
        resolveAny(host).then(({ address }) => {
            const sock = net.connect(443, address, () => {
                const t = tls.connect({ socket: sock, servername: host }, () => {
                    let h = method + ' ' + path + ' HTTP/1.1\r\nHost: ' + host + '\r\nUser-Agent: relay\r\nConnection: close\r\n';
                    for (const k of headers) h += k[0] + ': ' + k[1] + '\r\n';
                    if (body) h += 'Content-Length: ' + Buffer.byteLength(body) + '\r\n';
                    h += '\r\n'; t.write(h); if (body) t.write(body);
                });
                let raw = Buffer.alloc(0); t.on('data', c => { raw = Buffer.concat([raw, c]); });
                t.on('end', () => {
                    const idx = raw.indexOf('\r\n\r\n'); if (idx === -1) return reject(new Error('no headers'));
                    const head = raw.slice(0, idx).toString('latin1'); const status = parseInt(head.split(' ')[1], 10) || 502;
                    let b = raw.slice(idx + 4);
                    if (/transfer-encoding:\s*chunked/i.test(head)) {
                        let out = Buffer.alloc(0); let p = 0;
                        while (p < b.length) {
                            const eol = b.indexOf('\r\n', p); if (eol === -1) break;
                            const size = parseInt(b.slice(p, eol).toString(), 16); if (!size) break;
                            out = Buffer.concat([out, b.slice(eol + 2, eol + 2 + size)]); p = eol + 2 + size + 2;
                        }
                        b = out;
                    }
                    resolve({ status, body: b });
                });
                t.on('error', reject);
            });
            sock.on('error', reject);
        }).catch(reject);
    });
}
const httpGet = (host, path) => rawHttp(host, path);

let tgOffset = 0, tgCurrentPhotos = [], tgLots = [], tgPendingCreds = {};
function finishLot(chatId, creds) {
    if (!tgCurrentPhotos.length) return;
    tgLots.push({ id: Date.now() + '_' + Math.random().toString(36).slice(2, 7), chatId, creds, photos: tgCurrentPhotos });
    tgCurrentPhotos = [];
}

app.get('/tg/poll', async (req, res) => {
    if (!TG_TOKEN) return res.status(500).json({ error: 'no TELEGRAM_BOT_TOKEN' });
    try {
        const r = await httpGet('api.telegram.org', '/bot' + TG_TOKEN + '/getUpdates?offset=' + tgOffset + '&timeout=0&limit=100');
        const j = JSON.parse(r.body.toString());
        if (j.ok && j.result.length) {
            for (const u of j.result) {
                tgOffset = u.update_id + 1; const msg = u.message || u.channel_post; if (!msg) continue;
                const chatId = msg.chat.id;
                if (msg.photo && msg.photo.length) {
                    tgCurrentPhotos.push(msg.photo[msg.photo.length - 1].file_id);
                    if (msg.caption && msg.caption.includes(':')) finishLot(chatId, msg.caption.trim());
                } else if (msg.text && !msg.text.startsWith('/')) {
                    const txt = msg.text.trim();
                    if (/^(готово|done)\b/i.test(txt)) {
                        let creds = tgPendingCreds[chatId] || '';
                        if (txt.includes(':')) creds = txt.replace(/^(готово|done)\s*:?\s*/i, '').trim();
                        if (creds) finishLot(chatId, creds);
                    } else if (txt.includes(':')) {
                        if (tgCurrentPhotos.length) finishLot(chatId, txt); else tgPendingCreds[chatId] = txt;
                    }
                }
            }
        }
        res.json({ lots: tgLots.map(l => ({ id: l.id, creds: l.creds, photos: l.photos })) });
    } catch (e) { res.status(500).json({ error: String(e.message) }); }
});

app.get('/tg/ack', async (req, res) => {
    const id = String(req.query.id || ''); const ok = String(req.query.ok || '1') === '1'; const info = String(req.query.info || '').slice(0, 180);
    const lot = tgLots.find(l => l.id === id);
    if (lot) {
        tgLots = tgLots.filter(l => l.id !== id);
        if (String(req.query.silent || '') !== '1') {
            const text = ok ? ('✅ Лот создан: ' + info) : ('❌ Ошибка: ' + info);
            httpGet('api.telegram.org', '/bot' + TG_TOKEN + '/sendMessage?chat_id=' + lot.chatId + '&text=' + encodeURIComponent(text)).catch(() => {});
        }
    }
    res.json({ ok: true });
});

app.get('/tg/file', async (req, res) => {
    const fileId = String(req.query.id || '');
    try {
        const r1 = await httpGet('api.telegram.org', '/bot' + TG_TOKEN + '/getFile?file_id=' + encodeURIComponent(fileId));
        const j1 = JSON.parse(r1.body.toString()); if (!j1.ok) return res.status(404).send('getFile failed');
        const r2 = await httpGet('api.telegram.org', '/file/bot' + TG_TOKEN + '/' + j1.result.file_path);
        res.set('Content-Type', 'image/jpeg'); res.send(r2.body);
    } catch (e) { res.status(500).send(String(e.message)); }
});

app.get('/', (req, res) => res.send('Relay OK (Gemini-Fallback)'));
app.get('/api/v1/models', (req, res) => res.json({ data: [{ id: 'gemini-vision' }] }));

function extractImages(oai) {
    const imgs = []; for (const m of (oai.messages || [])) { if (Array.isArray(m.content)) { for (const c of m.content) if (c.type === 'image_url') imgs.push(c); } } return imgs;
}
function replaceImages(oai, imgs) {
    const o = JSON.parse(JSON.stringify(oai));
    for (const m of o.messages) { if (Array.isArray(m.content)) { const texts = m.content.filter(c => c.type === 'text'); m.content = [...texts, ...imgs]; return o; } } return o;
}
function normalizeTank(s) { return String(s || '').toLowerCase().replace(/[\s\-_.]/g, '').replace(/[«»"'`]/g, '').trim(); }
function mergeResults(results) {
    const topsMap = new Map(); const premsMap = new Map();
    const pushUnique = (map, name) => {
        const s = String(name).trim(); if (!s || s.length < 2 || s.length > 60) return;
        const key = normalizeTank(s); if (!key) return; if (!map.has(key)) map.set(key, s);
    };
    for (const r of results) {
        for (const t of (r.топы || [])) pushUnique(topsMap, t);
        for (const p of (r.премы || [])) pushUnique(premsMap, p);
    }
    return { 'топы': [...topsMap.values()], 'премы': [...premsMap.values()] };
}

function sendGemini(oai, res, done) {
    if (!GEMINI_KEY) return done(false, 401, Buffer.from('no GEMINI_API_KEY'));
    const imgs = extractImages(oai);
    const chunks = []; for (let i = 0; i < imgs.length; i += CHUNK_SIZE) chunks.push(imgs.slice(i, i + CHUNK_SIZE));
    if (!chunks.length) chunks.push([]);
    const results = [];
    const t0 = Date.now();
    let modelIdx = 0;

    const tryChunk = (ci, attempt) => {
        attempt = attempt || 0;
        if (ci >= chunks.length) {
            const merged = mergeResults(results);
            const dt = ((Date.now() - t0) / 1000).toFixed(1);
            console.log('<- GEMINI OK: пачек', chunks.length, '| топов', merged.топы.length, '| премов', merged.премы.length, '| за', dt, 'с');
            const oaiResp = JSON.stringify({ choices: [{ message: { role: 'assistant', content: JSON.stringify(merged) }, index: 0 }], model: 'gemini-chunked' });
            return done(true, 200, Buffer.from(oaiResp));
        }

        const sub = replaceImages(oai, chunks[ci]);
        
        const parts = [];
        for (const msg of sub.messages) {
            if (typeof msg.content === 'string') {
                parts.push({ text: msg.content });
            } else if (Array.isArray(msg.content)) {
                for (const item of msg.content) {
                    if (item.type === 'text') parts.push({ text: item.text });
                    else if (item.type === 'image_url') {
                        const b64 = item.image_url.url.split(',')[1];
                        parts.push({ inline_data: { mime_type: 'image/jpeg', data: b64 } });
                    }
                }
            }
        }

        const body = JSON.stringify({
            contents: [{ role: 'user', parts: parts }],
            generationConfig: { temperature: 0.1, maxOutputTokens: MAX_TOKENS }
        });

        const currentModel = GEMINI_MODELS[Math.min(modelIdx, GEMINI_MODELS.length - 1)];
        console.log(`-> GEMINI пачка ${ci + 1}/${chunks.length} | модель: ${currentModel} | попытка: ${attempt + 1}`);

        const path = `/v1beta/models/${currentModel}:generateContent`;
        const headers = [
            ['Content-Type', 'application/json'],
            ['x-goog-api-key', GEMINI_KEY]
        ];

        rawHttp(GEMINI_HOST, path, 'POST', headers, body).then(r => {
            if (r.status === 503) {
                console.log('<- GEMINI 503 (Перегрузка). Жду 10 сек...');
                if (attempt < 5) {
                    return setTimeout(() => tryChunk(ci, attempt + 1), 10000);
                }
                // Если 5 раз не вышло, меняем модель
                modelIdx++;
                return setTimeout(() => tryChunk(ci, 0), 2000);
            }
            
            if (r.status !== 200) {
                console.log('<- GEMINI ошибка', r.status);
                // Если модель не найдена, берем следующую
                if (r.status === 404) {
                    modelIdx++;
                    return setTimeout(() => tryChunk(ci, 0), 1000);
                }
                return tryChunk(ci + 1, 0);
            }
            
            try {
                const j = JSON.parse(r.body.toString());
                if (!j.candidates || !j.candidates[0] || !j.candidates[0].content) throw new Error('no content');
                let text = j.candidates[0].content.parts[0].text;
                
                const m = String(text).match(/\{[\s\S]*\}/);
                if (m) {
                    const parsed = JSON.parse(m[0]);
                    const tops = parsed.топы || parsed.tanks || [];
                    const prems = parsed.премы || parsed.premiums || [];
                    
                    if (tops.length === 0 && prems.length === 0 && attempt < 2) {
                        console.log('<- GEMINI вернула пустоту, пробую еще раз...');
                        return setTimeout(() => tryChunk(ci, attempt + 1), 2000);
                    }
                    
                    results.push({ топы: tops, премы: prems });
                }
                console.log(`<- GEMINI пачка ${ci + 1} OK`);
            } catch (e) { console.log('<- GEMINI: мусор в ответе'); }
            
            setTimeout(() => tryChunk(ci + 1, 0), 1000);
        }).catch(e => {
            console.log('<- GEMINI сеть (', e.message, ')');
            setTimeout(() => tryChunk(ci + 1, 0), 2000);
        });
    };
    tryChunk(0, 0);
}

app.post('/api/v1/chat/completions', (req, res) => {
    const oai = req.body;
    console.log('-> запрос | фото:', extractImages(oai).length);
    sendGemini(oai, res, (ok, status, payload) => {
        if (ok) return res.status(200).type('application/json').send(payload);
        res.status(status).type('application/json').send(payload);
    });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws) => {
    let upstream = null;
    ws.on('message', (data) => {
        if (!upstream) {
            try {
                const r = JSON.parse(data.toString());
                const [host, port] = r.target.split(':');
                const s = net.connect(parseInt(port), host, () => {
                    upstream = tls.connect({ socket: s, servername: host }, () => upstream.write(r.data));
                    upstream.on('data', d => { if (ws.readyState === 1) ws.send(d.toString('base64')); });
                    upstream.on('end', () => ws.close());
                    upstream.on('error', () => ws.close());
                });
                s.on('error', () => ws.close());
            } catch (e) { ws.close(); }
        } else {
            upstream.write(Buffer.from(data.toString(), 'base64'));
        }
    });
    ws.on('close', () => { if (upstream) upstream.destroy(); });
});

server.listen(3000, () => {
    console.log('Relay ready (GEMINI FALLBACK)');
    console.log('MODELS:', GEMINI_MODELS.join(', '));
    console.log('CHUNK=1 | MAX_TOKENS=2048');
});
