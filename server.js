const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const net = require('net');
const tls = require('tls');
const dns = require('dns');

const app = express();
app.use(express.json({ limit: '50mb' }));

const GEMINI_KEY = process.env.GEMINI_API_KEY || '';
const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const GEMINI_HOST = 'generativelanguage.googleapis.com';
const TG_HOST = 'api.telegram.org';

// ── Универсальный HTTPS GET через TLS (обход fetch) ──
function rawHttp(host, path) {
    return new Promise((resolve, reject) => {
        dns.promises.lookup(host).then(({ address }) => {
            const sock = net.connect(443, address, () => {
                const t = tls.connect({ socket: sock, servername: host }, () => {
                    t.write('GET ' + path + ' HTTP/1.1\r\nHost: ' + host + '\r\nUser-Agent: relay\r\nConnection: close\r\n\r\n');
                });
                let raw = Buffer.alloc(0);
                t.on('data', c => { raw = Buffer.concat([raw, c]); });
                t.on('end', () => {
                    const idx = raw.indexOf('\r\n\r\n');
                    if (idx === -1) return reject(new Error('no headers'));
                    const head = raw.slice(0, idx).toString('latin1');
                    const status = parseInt(head.split(' ')[1], 10) || 502;
                    let body = raw.slice(idx + 4);
                    if (/transfer-encoding:\s*chunked/i.test(head)) {
                        let out = Buffer.alloc(0); let p = 0;
                        while (p < body.length) {
                            const eol = body.indexOf('\r\n', p);
                            if (eol === -1) break;
                            const size = parseInt(body.slice(p, eol).toString(), 16);
                            if (!size) break;
                            out = Buffer.concat([out, body.slice(eol + 2, eol + 2 + size)]);
                            p = eol + 2 + size + 2;
                        }
                        body = out;
                    }
                    resolve({ status, body });
                });
                t.on('error', reject);
            });
            sock.on('error', reject);
        }).catch(reject);
    });
}

// ── Telegram intake ──
let tgOffset = 0;
let tgCurrentPhotos = [];   // фото текущего (незакрытого) лота
let tgLots = [];            // готовые лоты: {id, chatId, creds, photos:[file_id]}

function finishLot(chatId, creds) {
    if (!tgCurrentPhotos.length) return;
    tgLots.push({
        id: Date.now() + '_' + Math.random().toString(36).slice(2, 7),
        chatId, creds, photos: tgCurrentPhotos,
    });
    tgCurrentPhotos = [];
    console.log('📲 TG: лот готов,', tgLots[tgLots.length - 1].photos.length, 'фото');
}

app.get('/tg/poll', async (req, res) => {
    if (!TG_TOKEN) return res.status(500).json({ error: 'no TELEGRAM_BOT_TOKEN' });
    try {
        const r = await rawHttp(TG_HOST, '/bot' + TG_TOKEN + '/getUpdates?offset=' + tgOffset + '&timeout=0&limit=100');
        const j = JSON.parse(r.body.toString());
        if (j.ok && j.result.length) {
            for (const u of j.result) {
                tgOffset = u.update_id + 1;
                const msg = u.message || u.channel_post;
                if (!msg) continue;
                const chatId = msg.chat.id;
                if (msg.photo && msg.photo.length) {
                    const best = msg.photo[msg.photo.length - 1];
                    tgCurrentPhotos.push(best.file_id);
                    if (msg.caption && msg.caption.includes(':')) finishLot(chatId, msg.caption.trim());
                } else if (msg.text && msg.text.includes(':') && !msg.text.startsWith('/')) {
                    finishLot(chatId, msg.text.trim());
                }
            }
        }
        res.json({ lots: tgLots.map(l => ({ id: l.id, creds: l.creds, photos: l.photos })) });
    } catch (e) {
        res.status(500).json({ error: String(e.message) });
    }
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
            rawHttp(TG_HOST, '/bot' + TG_TOKEN + '/sendMessage?chat_id=' + lot.chatId + '&text=' + encodeURIComponent(text)).catch(() => {});
        }
    }
    res.json({ ok: true });
});

app.get('/tg/file', async (req, res) => {
    const fileId = String(req.query.id || '');
    try {
        const r1 = await rawHttp(TG_HOST, '/bot' + TG_TOKEN + '/getFile?file_id=' + encodeURIComponent(fileId));
        const j1 = JSON.parse(r1.body.toString());
        if (!j1.ok) return res.status(404).send('getFile failed');
        const r2 = await rawHttp(TG_HOST, '/file/bot' + TG_TOKEN + '/' + j1.result.file_path);
        res.set('Content-Type', 'image/jpeg');
        res.send(r2.body);
    } catch (e) { res.status(500).send(String(e.message)); }
});

// ── Gemini proxy (OpenAI-формат <-> Gemini) ──
app.get('/', (req, res) => res.send('Relay OK (Gemini + Telegram mode)'));
app.get('/api/v1/models', (req, res) => {
    res.json({ data: [{ id: 'gemini-3.8-flash' }] });
});

app.post('/api/v1/chat/completions', (req, res) => {
    if (!GEMINI_KEY) return res.status(500).send('no GEMINI_API_KEY in env');
    try {
        const oai = req.body;
        let model = String(oai.model || 'gemini-3.8-flash').replace(':free', '');
        if (!model.startsWith('gemini')) model = 'gemini-3.8-flash';

        const parts = [];
        for (const m of (oai.messages || [])) {
            if (typeof m.content === 'string') {
                parts.push({ text: m.content });
            } else if (Array.isArray(m.content)) {
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

        const geminiBody = JSON.stringify({
            contents: [{ role: 'user', parts }],
            generationConfig: { temperature: (oai.temperature != null ? oai.temperature : 0.2) },
        });
        const path = '/v1beta/models/' + model + ':generateContent?key=' + GEMINI_KEY;

        dns.promises.lookup(GEMINI_HOST).then(({ address }) => {
            const sock = net.connect(443, address, () => {
                const t = tls.connect({ socket: sock, servername: GEMINI_HOST }, () => {
                    console.log('-> Gemini | запрос:', geminiBody.length, 'байт');
                    t.write('POST ' + path + ' HTTP/1.1\r\n' +
                        'Host: ' + GEMINI_HOST + '\r\n' +
                        'Content-Type: application/json\r\n' +
                        'Content-Length: ' + Buffer.byteLength(geminiBody) + '\r\n' +
                        'Connection: close\r\n\r\n' + geminiBody);
                });
                let raw = Buffer.alloc(0);
                t.on('data', c => { raw = Buffer.concat([raw, c]); });
                t.on('end', () => {
                    const idx = raw.indexOf('\r\n\r\n');
                    if (idx === -1) return res.status(502).send('bad upstream');
                    const head = raw.slice(0, idx).toString('latin1');
                    const status = parseInt(head.split(' ')[1], 10) || 502;
                    let payload = raw.slice(idx + 4);
                    if (/transfer-encoding:\s*chunked/i.test(head)) {
                        let out = Buffer.alloc(0); let p = 0;
                        while (p < payload.length) {
                            const eol = payload.indexOf('\r\n', p);
                            if (eol === -1) break;
                            const size = parseInt(payload.slice(p, eol).toString(), 16);
                            if (!size) break;
                            out = Buffer.concat([out, payload.slice(eol + 2, eol + 2 + size)]);
                            p = eol + 2 + size + 2;
                        }
                        payload = out;
                    }
                    console.log('<- Статус:', head.split('\r\n')[0]);
                    if (status !== 200) {
                        return res.status(status).type('application/json').send(payload);
                    }
                    try {
                        const g = JSON.parse(payload.toString());
                        if (!g.candidates || !g.candidates[0]) return res.status(502).send('no candidates');
                        const text = g.candidates[0].content.parts.map(p => p.text || '').join('');
                        res.json({ choices: [{ message: { role: 'assistant', content: text }, index: 0 }], model: model });
                    } catch (e) {
                        res.status(502).send('parse: ' + e.message);
                    }
                });
                t.on('error', e => res.status(502).send('tls: ' + e.message));
            });
            sock.on('error', e => res.status(502).send('conn: ' + e.message));
        }).catch(e => res.status(500).send('dns: ' + e.message));
    } catch (e) {
        res.status(500).send('convert: ' + e.message);
    }
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

server.listen(3000, () => console.log('Relay ready (GEMINI + TELEGRAM mode)'));
