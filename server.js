const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const net = require('net');
const tls = require('tls');

const app = express();
app.use(express.json({ limit: '50mb' }));

const API_KEY = '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';

app.get('/', (req, res) => res.send('Relay OK'));
app.get('/api/v1/models', (req, res) => {
    res.json({ data: [{ id: 'qwen/qwen2.5-vl-72b-instruct' }, { id: 'openai/gpt-4o-mini' }] });
});

app.post('/api/v1/chat/completions', (req, res) => {
    const body = JSON.stringify(req.body);
    const sock = net.connect(443, 'api.openrouter.ai', () => {
        const t = tls.connect({ socket: sock, servername: 'api.openrouter.ai' }, () => {
            t.write('POST /api/v1/chat/completions HTTP/1.1\r\n' +
                'Host: api.openrouter.ai\r\n' +
                'Content-Type: application/json\r\n' +
                'Authorization: Bearer ' + API_KEY + '\r\n' +
                'Content-Length: ' + Buffer.byteLength(body) + '\r\n' +
                'Connection: close\r\n\r\n' + body);
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
            res.status(status).type('application/json').send(payload);
        });
        t.on('error', e => res.status(502).send('tls: ' + e.message));
    });
    sock.on('error', e => res.status(502).send('conn: ' + e.message));
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

server.listen(3000, () => console.log('Relay ready (HTTP-proxy mode)'));
