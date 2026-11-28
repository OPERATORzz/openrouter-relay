const express = require('express');
const http = require('http');
const https = require('https');
const { WebSocketServer } = require('ws');
const net = require('net');
const tls = require('tls');
const dns = require('dns');

const app = express();
app.use(express.json({ limit: '50mb' }));

const ROUTERAPI_KEY = process.env.ROUTERAPI_API_KEY || '';
const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const ROUTERAPI_HOST = 'routerapi.ru';

// 🔐 Необязательный токен доступа объявлен ВВЕРХУ, а страж — до маршрутов.
// Раньше app.use(['/api/v1','/tg']) регистрировался в конце файла, то есть ПОСЛЕ
// /tg/poll, /tg/file и /api/v1/models. Express применяет middleware по порядку,
// поэтому все эти маршруты оставались открытыми даже при заданном RELAY_TOKEN —
// включая чтение фото чужого лота из Telegram.
const RELAY_TOKEN = process.env.RELAY_TOKEN || '';
const SERVER_VERSION = '2.2.0';

function tokenFrom(req) {
    return String(req.get('x-relay-token') || req.query.token || '').trim();
}

function authOk(req) {
    if (!RELAY_TOKEN) return true;
    const t = tokenFrom(req);
    if (!t) return false;
    // сравнение постоянного времени, чтобы не давать угадывать токен по таймингам
    const a = Buffer.from(t), b = Buffer.from(RELAY_TOKEN);
    if (a.length !== b.length) return false;
    return require('crypto').timingSafeEqual(a, b);
}

// Страж ставится ДО маршрутов: только на «дорогие» и «чувствительные» адреса.
app.use(['/api/v1', '/tg'], (req, res, next) => {
    if (authOk(req)) return next();
    console.log('-> 401 | нет/неверный токен | ' + req.method + ' ' + req.path);
    return res.status(401).json({ error: { message: 'Неверный или отсутствующий токен доступа' } });
});

// 🧠 Модель распознавания. 25.11 сравнил на реальных скриншотах (5 фото, лот Blitz):
//   gpt-4o-mini  — 25 названий за 1,35 ₽, часть выдумана («M60A1», «Panzer IV», «Hellcat»),
//                  а на ПК-скриншоте вообще выдал ряд T-34, T-43, T-44, T-55 — галлюцинация;
//   gemini-2.5-flash — 28 названий за 0,16 ₽, все реальные, и на ПК прочитал 37 из 38.
// Gemini и в 8 раз дешевле, и не додумывает. Поэтому основная модель — она.
const MODEL = process.env.MODEL_NAME || 'google/gemini-2.5-flash';
// 🚨 CHUNK_SIZE = 1 — ОДНО фото на запрос. 25.11 замерено на реальном лоте ПК:
//   пачка из 2 фото в одном запросе -> модель читала 15 из 20 названий с одного снимка;
//   то же фото отдельным запросом   -> 20 из 20, и три прогона подряд дали 20/20/20.
// Когда в запросе несколько скриншотов, модель «размазывает» внимание и теряет мелкий
// текст. Цена почти не меняется (накладные ~330 токенов на запрос), а точность растёт.
// Бонус: если одна пачка упадёт, потеряется одно фото, а не половина лота.
const CHUNK_SIZE = 1;
const MAX_TOKENS = 2048;

// Сколько раз повторять ОДНУ пачку при разных типах сбоя.
// Раньше 429 повторялся бесконечно, а любой другой не-200 молча хоронил пачку.
const MAX_RETRY_429 = 6;
const MAX_RETRY_503 = 4;
const MAX_RETRY_OTHER = 3;
const MAX_RETRY_PARSE = 2;
const WAIT_429_MS = 15000;
const WAIT_503_MS = 10000;
const WAIT_OTHER_MS = 5000;

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
                sock.setTimeout(45000, () => { sock.destroy(); reject(new Error('Timeout 45s')); });

                const t = tls.connect({ socket: sock, servername: host }, () => {
                    let h = method + ' ' + path + ' HTTP/1.1\r\nHost: ' + host + '\r\nUser-Agent: relay\r\nConnection: close\r\n';
                    for (const k of headers) h += k[0] + ': ' + k[1] + '\r\n';
                    if (body) h += 'Content-Length: ' + Buffer.byteLength(body) + '\r\n';
                    h += '\r\n'; t.write(h); if (body) t.write(body);
                });
                let raw = Buffer.alloc(0);
                let settled = false;
                t.on('data', c => { raw = Buffer.concat([raw, c]); });

                // Раньше ответ собирался только по 'end'. Если соединение закрывалось
                // без него (обрыв на стороне RouterAPI, RST), промис не завершался
                // никогда: пачка висела до таймаута 45 с, а в логах было пусто.
                const finish = (why) => {
                    if (settled) return;
                    settled = true;
                    try { sock.setTimeout(0); } catch (e) {}
                    const idx = raw.indexOf('\r\n\r\n');
                    if (idx === -1) return reject(new Error(why + ': нет заголовков ответа'));
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
                };

                t.on('end', () => finish('end'));
                t.on('close', () => finish('close'));
                t.on('error', (e) => { if (settled) return; settled = true; try { sock.destroy(); } catch (_) {} reject(e); });
            });
            sock.on('error', reject);
        }).catch(reject);
    });
}
const httpGet = (host, path) => rawHttp(host, path);

let tgOffset = 0, tgLots = [], tgPendingCreds = {};

// Раньше фото лежали в одной глобальной переменной tgCurrentPhotos:
//  · фото из одного чата могли уехать в лот другого;
//  · присланные «впрок» фото не истекали и приклеивались к следующему аккаунту;
//  · лоты без подтверждения висели в памяти вечно.
// Теперь фото хранятся по чатам и стареют, а лоты ограничены TTL и количеством.
const TG_PHOTO_TTL = 30 * 60 * 1000;   // 30 минут — фото старше считаем брошенными
const TG_LOT_TTL = 2 * 60 * 60 * 1000; // 2 часа на обработку лота
const TG_MAX_LOTS = 50;                // больше 50 неотвеченных лотов не держим
const tgPhotosByChat = new Map();

function tgPhotos(chatId) {
    const rec = tgPhotosByChat.get(chatId);
    if (!rec) return [];
    if (Date.now() - rec.at > TG_PHOTO_TTL) { tgPhotosByChat.delete(chatId); return []; }
    return rec.list;
}

function tgPushPhoto(chatId, fileId) {
    let rec = tgPhotosByChat.get(chatId);
    if (!rec || Date.now() - rec.at > TG_PHOTO_TTL) rec = { list: [], at: Date.now() };
    rec.list.push(fileId);
    rec.at = Date.now();
    tgPhotosByChat.set(chatId, rec);
}

function tgPruneLots() {
    const now = Date.now();
    const before = tgLots.length;
    tgLots = tgLots.filter(l => now - (l.at || 0) < TG_LOT_TTL);
    if (tgLots.length > TG_MAX_LOTS) tgLots = tgLots.slice(-TG_MAX_LOTS);
    if (before !== tgLots.length) console.log('🧹 TG: убрано просроченных лотов: ' + (before - tgLots.length));
}

function finishLot(chatId, creds) {
    const photos = tgPhotos(chatId);
    if (!photos.length) return;
    tgPhotosByChat.delete(chatId);
    tgLots.push({ id: Date.now() + '_' + Math.random().toString(36).slice(2, 7), chatId, creds, photos, at: Date.now() });
    tgPruneLots();
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
                    tgPushPhoto(chatId, msg.photo[msg.photo.length - 1].file_id);
                    if (msg.caption && msg.caption.includes(':')) finishLot(chatId, msg.caption.trim());
                } else if (msg.text && !msg.text.startsWith('/')) {
                    const txt = msg.text.trim();
                    if (/^(готово|done)\b/i.test(txt)) {
                        let creds = tgPendingCreds[chatId] || '';
                        if (txt.includes(':')) creds = txt.replace(/^(готово|done)\s*:?\s*/i, '').trim();
                        if (creds) finishLot(chatId, creds);
                    } else if (txt.includes(':')) {
                        if (tgPhotos(chatId).length) finishLot(chatId, txt); else tgPendingCreds[chatId] = txt;
                    }
                }
            }
        }
        tgPruneLots();
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

app.get('/', (req, res) => res.send('Relay OK (RouterAPI + ' + MODEL + ', detail high, 1 фото на запрос) | partial-guard v2 | v' + SERVER_VERSION + ' | auth: ' + (RELAY_TOKEN ? 'on' : 'off')));
app.get('/api/v1/models', (req, res) => res.json({ data: [{ id: 'routerapi-vision' }] }));

function extractImages(oai) {
    const imgs = []; 
    for (const m of (oai.messages || [])) { 
        if (Array.isArray(m.content)) { 
            for (const c of m.content) {
                if (c.type === 'image_url') {
                    // 🔍 РЕЖИМ ВЫСОКОЙ ДЕТАЛИЗАЦИИ.
                    // 25.11 проверили на реальных скриншотах 1280×719: при 'low' картинка
                    // сжимается до 512 px, текст под иконками становится нечитаемым, и модель
                    // вместо чтения переписывает словарь из промпта — счёт танков скакал
                    // 54 / 173 / 60 на ОДНОМ И ТОМ ЖЕ лоте. При 'high' модель реально читает
                    // («T57 Heavy», «Jg.Pz. E 100», «TVP T 50/51»). Цена: ~0,54 ₽ за фото
                    // вместо 0,06 ₽, то есть около 2,7 ₽ за лот из 5 фото.
                    if (c.image_url && typeof c.image_url === 'object') {
                        c.image_url.detail = 'high';
                    }
                    imgs.push(c); 
                }
            } 
        } 
    } 
    return imgs;
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

function sendRouterAPI(oai, res, done) {
    if (!ROUTERAPI_KEY) return done(false, 401, Buffer.from('no ROUTERAPI_API_KEY'));
    const imgs = extractImages(oai);
    const chunks = []; for (let i = 0; i < imgs.length; i += CHUNK_SIZE) chunks.push(imgs.slice(i, i + CHUNK_SIZE));
    if (!chunks.length) chunks.push([]);
    const results = [];
    const failures = [];
    const t0 = Date.now();

    // Финал: считаем, сколько пачек реально прошло.
    // - ни одна не прошла  -> 503, клиент повторит весь запрос;
    // - часть не прошла    -> 200 + partial:true, клиент НЕ должен публиковать лот.
    const finish = () => {
        const merged = mergeResults(results);
        const dt = ((Date.now() - t0) / 1000).toFixed(1);
        const partial = failures.length > 0;
        console.log('<- ROUTERAPI ' + (partial ? 'НЕПОЛНЫЙ' : 'OK') +
            ' | пачек ' + chunks.length + ' | ок ' + results.length + ' | провалено ' + failures.length +
            ' | топов ' + merged.топы.length + ' | премов ' + merged.премы.length + ' | за ' + dt + ' с');

        if (results.length === 0 && failures.length > 0) {
            console.log('<- ROUTERAPI ВСЕ пачки провалены:', JSON.stringify(failures));
            return done(false, 503, Buffer.from(JSON.stringify({ error: { message: 'all chunks failed' }, partial: true, failures: failures })));
        }

        if (partial) console.log('<- ROUTERAPI провалы:', JSON.stringify(failures));

        const content = {
            'топы': merged.топы,
            'премы': merged.премы,
            'partial': partial,
            'chunks': chunks.length,
            'chunksOk': results.length,
            'failures': failures
        };
        const oaiResp = JSON.stringify({ choices: [{ message: { role: 'assistant', content: JSON.stringify(content) }, index: 0 }], model: 'routerapi-chunked' });
        return done(true, 200, Buffer.from(oaiResp));
    };

    const markFailed = (ci, status, reason) => {
        failures.push({ index: ci + 1, status: status, reason: String(reason || '').slice(0, 120) });
        console.log('<- ROUTERAPI пачка ' + (ci + 1) + '/' + chunks.length + ' ПРОВАЛЕНА (' + status + ': ' + reason + ')');
        setTimeout(() => tryChunk(ci + 1, 0), 1000);
    };

    const tryChunk = (ci, attempt) => {
        attempt = attempt || 0;
        if (ci >= chunks.length) return finish();

        const sub = replaceImages(oai, chunks[ci]);
        const body = JSON.stringify({
            model: MODEL,
            messages: sub.messages,
            max_tokens: MAX_TOKENS
        });

        console.log('-> ROUTERAPI пачка ' + (ci + 1) + '/' + chunks.length + ' | модель: ' + MODEL + ' | попытка: ' + (attempt + 1));

        const path = '/api/v1/chat/completions';
        const headers = [
            ['Content-Type', 'application/json'],
            ['Authorization', 'Bearer ' + ROUTERAPI_KEY]
        ];

        rawHttp(ROUTERAPI_HOST, path, 'POST', headers, body).then(r => {
            if (r.status === 429) {
                if (attempt >= MAX_RETRY_429) return markFailed(ci, 429, 'лимит запросов, попытки кончились');
                console.log('<- ROUTERAPI 429 (лимит). Жду ' + Math.round(WAIT_429_MS / 1000) + ' с...');
                return setTimeout(() => tryChunk(ci, attempt + 1), WAIT_429_MS);
            }
            if (r.status === 503 || r.status === 502 || r.status === 504) {
                if (attempt >= MAX_RETRY_503) return markFailed(ci, r.status, 'сервис недоступен, попытки кончились');
                console.log('<- ROUTERAPI ' + r.status + '. Жду ' + Math.round(WAIT_503_MS / 1000) + ' с, повторяю эту же пачку...');
                return setTimeout(() => tryChunk(ci, attempt + 1), WAIT_503_MS);
            }
            if (r.status !== 200) {
                if (attempt >= MAX_RETRY_OTHER) return markFailed(ci, r.status, 'неожиданный статус');
                console.log('<- ROUTERAPI ошибка ' + r.status + '. Повтор через ' + Math.round(WAIT_OTHER_MS / 1000) + ' с...');
                return setTimeout(() => tryChunk(ci, attempt + 1), WAIT_OTHER_MS);
            }

            let parsed;
            try {
                const j = JSON.parse(r.body.toString());
                if (!j.choices || !j.choices[0]) throw new Error('no content');
                const text = j.choices[0].message.content;
                const m = String(text).match(/\{[\s\S]*\}/);
                if (!m) throw new Error('нет JSON в ответе');
                parsed = JSON.parse(m[0]);
            } catch (e) {
                if (attempt >= MAX_RETRY_PARSE) return markFailed(ci, 200, 'мусор в ответе: ' + e.message);
                console.log('<- ROUTERAPI мусор в ответе (' + e.message + '). Повтор этой же пачки...');
                return setTimeout(() => tryChunk(ci, attempt + 1), 3000);
            }

            // Клиент может просить два формата: {"tanks":[...]} (мобилка, классифицирует
            // словарь) или {"tops":[...],"prems":[...]} (ПК, где словаря нет и делит модель).
            // Латиницу тоже понимаем — иначе ответ ПК-промпта молча терялся.
            const tops = parsed.топы || parsed.tops || parsed.tanks || parsed.танки || [];
            const prems = parsed.премы || parsed.prems || parsed.premiums || [];
            results.push({ топы: tops, премы: prems });
            console.log('<- ROUTERAPI пачка ' + (ci + 1) + ' OK | топов ' + tops.length + ' | премов ' + prems.length);

            setTimeout(() => tryChunk(ci + 1, 0), 2000);
        }).catch(e => {
            if (attempt >= MAX_RETRY_OTHER) return markFailed(ci, 0, 'сеть: ' + e.message);
            console.log('<- ROUTERAPI сеть (' + e.message + '). Повтор этой же пачки...');
            setTimeout(() => tryChunk(ci, attempt + 1), 3000);
        });
    };
    tryChunk(0, 0);
}

app.post('/api/v1/chat/completions', (req, res) => {
    const oai = req.body;
    console.log('-> запрос | фото:', extractImages(oai).length);
    sendRouterAPI(oai, res, (ok, status, payload) => {
        if (ok) return res.status(200).type('application/json').send(payload);
        return res.status(status).type('application/json').send(payload);
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

// Render (и любой другой хостинг) выдаёт свой порт через переменную PORT.
// Жёстко зашитый 3000 работал на старом сервисе случайно: Render угадывал порт.
// На новом сервисе это самая частая причина «No open ports detected».
const PORT = Number(process.env.PORT) || 3000;

server.listen(PORT, '0.0.0.0', () => {
    console.log('Relay ready (RouterAPI + Gemini, detail high) | v' + SERVER_VERSION + ' | порт ' + PORT);
    console.log('MODEL:', MODEL);
    console.log('CHUNK=' + CHUNK_SIZE + ' (одно фото на запрос) | MAX_TOKENS=' + MAX_TOKENS);
    console.log('auth: ' + (RELAY_TOKEN ? 'on' : 'off') + ' | telegram: ' + (TG_TOKEN ? 'on' : 'off') + ' | routerapi: ' + (ROUTERAPI_KEY ? 'on' : 'off'));
});
