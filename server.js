const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const net = require('net');
const tls = require('tls');
const dns = require('dns');

const app = express();
app.use(express.json({ limit: '50mb' }));

const GEMINI_KEY = process.env.GEMINI_API_KEY || '';
const TARGET = 'generativelanguage.googleapis.com';

app.get('/', (req, res) => res.send('Relay OK (Gemini mode)'));
app.get('/api/v1/models', (req, res) => {
    res.json({ data: [{ id: 'gemini-2.5-flash' }, { id: 'gemini-2.5-flash-lite' }] });
});

app.post('/api/v1/chat/completions', (req, res) => {
    if (!GEMINI_KEY) return res.status(500).send('no GEMINI_API_KEY in env');
    try {
        const oai = req.body;
        let model = String(oai.model || 'gemini-2.5-flash').replace(':free', '');
        if (!model.startsWith('gemini')) model = 'gemini-2.5-flash';

        // OpenAI-формат -> Gemini-формат
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

        dns.promises.lookup(TARGET).then(({ address }) => {
            const sock = net.connect(443, address, () => {
                const t = tls.connect({ socket: sock, servername: TARGET }, () => {
                    console.log('-> Gemini, модель:', model, '| запрос:', geminiBody.length, 'байт');
                    t.write('POST ' + path + ' HTTP/1.1\r\n' +
                        'Host: ' + TARGET + '\r\n' +
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
                    console.log('<- Статус:', head.split('\r\n')[0]);
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
                    console.log('<- Тело:', payload.slice(0, 200).toString());
                    if (status !== 200) {
                        return res.status(status).type('application/json').send(payload);
                    }
                    // Gemini-формат -> OpenAI-формат
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

server.listen(3000, () => console.log('Relay ready (GEMINI mode)'));
