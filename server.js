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
const GITHUB_TOKEN = process.env.GITHUB_TOKEN || '';
const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const GEMINI_HOST = 'generativelanguage.googleapis.com';
const GH_HOST = 'models.github.ai';

// ── DNS: системный, при провале — DoH через Cloudflare/Google ──
function dohResolve(hostname) {
    return new Promise((resolve, reject) => {
        const servers = [
            { ip: '1.1.1.1', name: 'cloudflare-dns.com' },
            { ip: '8.8.8.8', name: 'dns.google' },
        ];
        let i = 0;
        const tryNext = () => {
            if (i >= servers.length) return reject(new Error('DoH failed: ' + hostname));
            const s = servers[i++];
            const req = https.get({
                host: s.ip,
                servername: s.name,
                path: '/dns-query?name=' + hostname + '&type=A',
                headers: { accept: 'application/dns-json', host: s.name },
                timeout: 8000,
            }, (r) => {
                let d = '';
                r.on('data', c => d += c);
                r.on('end', () => {
                    try {
                        const j = JSON.parse(d);
                        const ans = (j.Answer || []);
                        const a = ans.find(x => x.type === 1);
                        if (a) return resolve(a.data);
                        const cn = ans.find(x => x.type === 5);
                        if (cn) return dohResolve(cn.data.replace(/\.$/, '')).then(resolve, reject);
                        tryNext();
                    } catch (e) { tryNext(); }
                });
            });
            req.on('error', tryNext);
            req.on('timeout', () => { req.destroy(); tryNext(); });
        };
        tryNext();
    });
}

async function resolveAny(hostname) {
    try { return await dns.promises.lookup(hostname); }
    catch (e) { const ip = await dohResolve(hostname); return { address: ip }; }
}

// ── Универсальный HTTPS-запрос через TLS ──
function rawHttp(host, path, method, headers, body) {
    method = method || 'GET';
    headers = headers || [];
    return new Promise((resolve, reject) => {
        resolveAny(host).then(({ address }) => {
            const sock = net.connect(443, address, () => {
                const t = tls.connect({ socket: sock, servername: host }, () => {
                    let h = method + ' ' + path + ' HTTP/1.1\r\nHost: ' + host + '\r\nUser-Agent: relay\r\nConnection: close\r\n';
                    for (const k of headers) h += k[0] + ': ' + k[1] + '\r\n';
                    if (body) h += 'Content-Length: ' + Buffer.byteLength(body) + '\r\n';
                    h += '\r\n';
                    t.write(h);
                    if (body) t.write(body);
                });
                let raw = Buffer.alloc(0);
                t.on('data', c => { raw = Buffer.concat([raw, c]); });
                t.on('end', () => {
                    const idx = raw.indexOf('\r\n\r\n');
                    if (idx === -1) return reject(new Error('no headers'));
                    const head = raw.slice(0, idx).toString('latin1');
                    const status = parseInt(head.split(' ')[1], 10) || 502;
                    let b = raw.slice(idx + 4);
                    if (/transfer-encoding:\s*chunked/i.test(head)) {
                        let out = Buffer.alloc(0); let p = 0;
                        while (p < b.length) {
                            const eol = b.indexOf('\r\n', p);
                            if (eol === -1) break;
                            const size = parseInt(b.slice(p, eol).toString(), 16);
                            if (!size) break;
                            out = Buffer.concat([out, b.slice(eol + 2, eol + 2 + size)]);
                            p = eol + 2 + size + 2;
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

// ── Telegram intake ──
let tgOffset = 0;
let tgCurrentPhotos = [];
let tgLots = [];

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
                tgOffset = u.update_id + 1;
                const msg = u.message || u.channel_post;
                if (!msg) continue;
                const chatId = msg.chat.id;
                if (msg.photo && msg.photo.length) {
                    tgCurrentPhotos.push(msg.photo[msg.photo.length - 1].file_id);
                    if (msg.caption && msg.caption.includes(':')) finishLot(chatId, msg.caption.trim());
                } else if (msg.text && msg.text.includes(':') && !msg.text.startsWith('/')) {
                    finishLot(chatId, msg.text.trim());
                }
            }
        }
        res.json({ lots: tgLots.map(l => ({ id: l.id, creds: l.creds, photos: l.photos })) });
    } catch (e) { res.status(500).json({ error: String(e.message) }); }
});

app.get('/tg/ack', async (req, res) => {
    const id = String(req.query.id || '');
    const ok = String(req.query.ok || '1') === '1';
    const info = String(req.query.info || '').slice(0, 180);
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
        const j1 = JSON.parse(r1.body.toString());
        if (!j1.ok) return res.status(404).send('getFile failed');
        const r2 = await httpGet('api.telegram.org', '/file/bot' + TG_TOKEN + '/' + j1.result.file_path);
        res.set('Content-Type', 'image/jpeg');
        res.send(r2.body);
    } catch (e) { res.status(500).send(String(e.message)); }
});

// ── Мультипровайдер: GITHUB models.github.ai (основной) → GEMINI (запасной) ──
app.get('/', (req, res) => res.send('Relay OK (models.github.ai mode)'));
app.get('/api/v1/models', (req, res) => {
    res.json({ data: [{ id: 'gpt-4o-mini' }, { id: 'gemini-3.8-flash' }] });
});

function sendGithub(oai, res, done) {
    if (!GITHUB_TOKEN) return done(false, 401, Buffer.from('no GITHUB_TOKEN'));
    const tryModelName = (modelName) => {
        const body = JSON.stringify({
            model: modelName,
            temperature: (oai.temperature != null ? oai.temperature : 0.2),
            messages: oai.messages,
        });
        return rawHttp(GH_HOST, '/inference/chat/completions', 'POST',
            [['Content-Type', 'application/json'], ['Authorization', 'Bearer ' + GITHUB_TOKEN]], body)
            .then(r => {
                if (r.status === 404 && modelName !== 'gpt-4o-mini') return tryModelName('gpt-4o-mini');
                if (r.status !== 200) return done(false, r.status, r.body);
                done(true, 200, r.body);
            });
    };
    tryModelName('openai/gpt-4o-mini').catch(e => done(false, 502, Buffer.from('conn: ' + e.message)));
}

function sendGemini(oai, res, done) {
    if (!GEMINI_KEY) return done(false, 401, Buffer.from('no GEMINI_API_KEY'));
    let model = String(oai.model || 'gemini-3.8-flash').replace(':free', '');
    if (!model.startsWith('gemini')) model = 'gemini-3.8-flash';
    const parts = [];
    for (const m of (oai.messages || [])) {
        if (typeof m.content === 'string') parts.push({ text: m.content });
        else if (Array.isArray(m.content)) {
            for (const c of m.content) {
                if (c.type === 'text') parts.push({ text: c.text });
                else if (c.type === 'image_url') {
                    const url = (c.image_url && c.image_url.url) || '';
                    const b64 = url.includes(',') ? url.split(',').pop() : url;
                    parts.push({ inline_data: { mime_type: 'image/jpeg', data: b64 } });
                }
            }
        }
    }
    const body = JSON.stringify({
        contents: [{ role: 'user', parts }],
        generationConfig: { temperature: (oai.temperature != null ? oai.temperature : 0.2) },
    });
    const path = '/v1beta/models/' + model + ':generateContent?key=' + GEMINI_KEY;
    rawHttp(GEMINI_HOST, path, 'POST', [['Content-Type', 'application/json']], body).then(r => {
        if (r.status !== 200) return done(false, r.status, r.body);
        try {
            const g = JSON.parse(r.body.toString());
            if (!g.candidates || !g.candidates[0]) return done(false, 502, Buffer.from('no candidates'));
            const text = g.candidates[0].content.parts.map(p => p.text || '').join('');
            const oaiResp = JSON.stringify({ choices: [{ message: { role: 'assistant', content: text }, index: 0 }], model: model });
            done(true, 200, Buffer.from(oaiResp));
        } catch (e) { done(false, 502, Buffer.from('parse: ' + e.message)); }
    }).catch(e => done(false, 502, Buffer.from('conn: ' + e.message)));
}

app.post('/api/v1/chat/completions', (req, res) => {
    const oai = req.body;
    console.log('-> запрос | фото:', Array.isArray(oai.messages?.[1]?.content) ? oai.messages[1].content.filter(c => c.type === 'image_url').length : 0);
    const providers = [sendGithub, sendGemini];
    let pi = 0;
    const tryNext = () => {
        if (pi >= providers.length) {
            return res.status(429).type('application/json')
                .send(Buffer.from(JSON.stringify({ error: { message: 'оба провайдера исчерпаны' } })));
        }
        const p = providers[pi++];
        p(oai, res, (ok, status, payload) => {
            if (ok) {
                console.log('<- ПРОВАЙДЕР', pi === 1 ? 'GITHUB' : 'GEMINI', 'OK, ответ:', payload.length, 'байт');
                return res.status(200).type('application/json').send(payload);
            }
            if (status === 429 || status === 503 || status === 401 || status === 402) {
                console.log('<- провайдер', pi, 'вернул', status, '→ пробую следующего');
                return tryNext();
            }
            res.status(status).type('application/json').send(payload);
        });
    };
    tryNext();
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

server.listen(3000, () => console.log('Relay ready (models.github.ai primary + DoH)'));
