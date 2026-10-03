const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const net = require('net');
const tls = require('tls');

const app = express();
app.get('/', (req, res) => res.send('Relay OK'));
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
    let upstream = null;
    ws.on('message', (data) => {
        if (!upstream) {
            try {
                const req = JSON.parse(data.toString());
                const [host, port] = req.target.split(':');
                const socket = net.connect(parseInt(port), host, () => {
                    upstream = tls.connect({ socket, servername: host }, () => {
                        upstream.write(req.data);
                    });
                    upstream.on('data', (d) => ws.send(d.toString('base64')));
                    upstream.on('end', () => ws.close());
                    upstream.on('error', () => ws.close());
                });
                socket.on('error', () => ws.close());
            } catch(e) { ws.close(); }
        } else {
            upstream.write(Buffer.from(data.toString(), 'base64'));
        }
    });
    ws.on('close', () => { if (upstream) upstream.destroy(); });
});

server.listen(3000, () => console.log('Relay ready'));
