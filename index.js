#!/usr/bin/env node
/**
 * 46elks <-> OpenAI Realtime API voice bridge (Node.js version)
 *
 * Baserad på 46elks officiella Python-exempel:
 * https://46elks.se/tutorials/real-time-two-way-voice-calls-with-websocket
 *
 * DEMO-SYFTE: Detta är en demo-AI-receptionist. Den bokar inga tider,
 * använder inga externa verktyg/function-calls och har inget minne
 * mellan samtal. Den svarar bara konversationellt på rösten.
 *
 * Körs som en enda HTTP-server (krav på Render, som bara exponerar
 * en (1) port via HTTPS/WSS - inte en godtycklig rå TCP-port som i
 * 46elks originalexempel). 46elks ansluter till servern via wss://
 * på Render-domänen istället för ws://IP:PORT.
 */

'use strict';

require('dotenv').config();

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { randomUUID } = require('crypto');

// ---------------------------------------------------------------------------
// Konfiguration
// ---------------------------------------------------------------------------

const PORT = process.env.PORT || 8095; // Render sätter PORT automatiskt
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_REALTIME_MODEL || 'gpt-4o-realtime-preview';
const VOICE = process.env.OPENAI_VOICE || 'shimmer';
const CODEC = 'pcm_24000'; // matchar OpenAI:s pcm16 @ 24kHz
const ELKS_PATH = process.env.ELKS_WS_PATH || '/voice'; // sökväg 46elks ansluter till

const INSTRUCTIONS =
  process.env.SYSTEM_INSTRUCTIONS ||
  [
    'Du är en AI-receptionist som demonstrerar hur en AI kan svara i telefon.',
    'Var kort, vänlig och tydlig i dina svar - detta är ett röstsamtal, inte text.',
    'Berätta för den som ringer, om det passar naturligt, att detta bara är en demo.',
    'Du kan INTE boka tider, INTE slå upp information och har INGA verktyg.',
    'Om personen ber om bokning, uppgifter eller åtgärder du inte kan utföra,',
    'förklara vänligt att detta endast är en teknisk demo av rösten och att',
    'ingen bokning eller ärendehantering sker på riktigt.',
  ].join(' ');

if (!OPENAI_API_KEY) {
  // eslint-disable-next-line no-console
  console.error('FATAL: Miljövariabeln OPENAI_API_KEY saknas. Sätt den innan start.');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Enkel loggning
// ---------------------------------------------------------------------------

function log(callId, ...args) {
  const prefix = `[${new Date().toISOString()}]${callId ? ` [${callId}]` : ''}`;
  // eslint-disable-next-line no-console
  console.log(prefix, ...args);
}

function logError(callId, ...args) {
  const prefix = `[${new Date().toISOString()}]${callId ? ` [${callId}]` : ''}`;
  // eslint-disable-next-line no-console
  console.error(prefix, ...args);
}

// ---------------------------------------------------------------------------
// Express-app (health check åt Render + valfri statussida)
// ---------------------------------------------------------------------------

const app = express();

app.get('/', (req, res) => {
  res.status(200).send('46elks AI-receptionist demo är igång.');
});

// Render (och 46elks) kan pinga denna för hälsokontroll.
app.get('/healthz', (req, res) => {
  res.status(200).json({ status: 'ok', uptime: process.uptime() });
});

const server = http.createServer(app);

// ---------------------------------------------------------------------------
// WebSocket-server för inkommande 46elks-anslutningar
// ---------------------------------------------------------------------------

const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const { pathname } = new URL(req.url, `http://${req.headers.host}`);

  if (pathname !== ELKS_PATH) {
    log(null, `Avvisar WS-uppgradering på okänd path: ${pathname}`);
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', (elksWs) => {
  handleCall(elksWs).catch((err) => {
    logError(null, 'Ohanterat fel i handleCall:', err);
    safeClose(elksWs);
  });
});

// ---------------------------------------------------------------------------
// Hjälpfunktioner
// ---------------------------------------------------------------------------

function safeSend(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function safeClose(ws) {
  try {
    if (ws && ws.readyState !== WebSocket.CLOSED) ws.close();
  } catch (_) {
    /* noop */
  }
}

// ---------------------------------------------------------------------------
// Kärnlogik: en instans per samtal
// ---------------------------------------------------------------------------

async function handleCall(elksWs) {
  let callId = randomUUID().slice(0, 8); // temporärt id tills hello anländer
  let openaiWs = null;
  let isSpeaking = false; // true medan AI:n pratar (för att undvika eko/självavbrott)
  let elksClosed = false;
  let openaiClosed = false;
  let heartbeat = null;

  // --- 1. Vänta på "hello" från 46elks -------------------------------------

  const helloMsg = await new Promise((resolve, reject) => {
    const onMessage = (raw) => {
      elksWs.off('message', onMessage);
      try {
        resolve(JSON.parse(raw.toString()));
      } catch (err) {
        reject(new Error(`Kunde inte tolka första meddelandet som JSON: ${err.message}`));
      }
    };
    elksWs.once('message', onMessage);
    elksWs.once('close', () => reject(new Error('46elks stängde anslutningen innan hello.')));
    elksWs.once('error', reject);
  });

  if (helloMsg.t !== 'hello') {
    logError(callId, 'Förväntade "hello", fick:', helloMsg);
    safeClose(elksWs);
    return;
  }

  callId = helloMsg.callid || callId;
  const from = helloMsg.from || '?';
  const to = helloMsg.to || '?';
  log(callId, `Samtal från ${from} till ${to}`);

  // --- 2. Anslut till OpenAI Realtime API ----------------------------------

  openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_MODEL)}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        'OpenAI-Beta': 'realtime=v1',
      },
    }
  );

  const cleanup = (reason) => {
    if (heartbeat) clearInterval(heartbeat);
    if (!elksClosed) {
      elksClosed = true;
      safeSend(elksWs, { t: 'bye', reason: reason || 'done' });
      safeClose(elksWs);
    }
    if (!openaiClosed) {
      openaiClosed = true;
      safeClose(openaiWs);
    }
  };

  await new Promise((resolve, reject) => {
    openaiWs.once('open', resolve);
    openaiWs.once('error', (err) => reject(new Error(`Kunde inte ansluta till OpenAI: ${err.message}`)));
  }).catch((err) => {
    logError(callId, err.message);
    cleanup('error');
    throw err;
  });

  log(callId, `Ansluten till OpenAI Realtime (${OPENAI_MODEL})`);

  // --- 3. Konfigurera OpenAI-sessionen -------------------------------------

  safeSend(openaiWs, {
    type: 'session.update',
    session: {
      modalities: ['audio', 'text'],
      instructions: INSTRUCTIONS,
      voice: VOICE,
      input_audio_format: 'pcm16',
      output_audio_format: 'pcm16',
      turn_detection: {
        type: 'server_vad',
        threshold: 0.9,
        silence_duration_ms: 800,
      },
    },
  });

  // Hälsning direkt när samtalet kopplas upp
  safeSend(openaiWs, {
    type: 'response.create',
    response: {
      modalities: ['audio', 'text'],
      instructions:
        'Hälsa kort på den som ringer och nämn att detta är en AI-receptionist-demo.',
    },
  });

  // --- 4. Deklarera ljudformat till 46elks ----------------------------------

  safeSend(elksWs, { t: 'sending', format: CODEC });
  safeSend(elksWs, { t: 'listening', format: CODEC });

  // Håll 46elks-anslutningen vid liv (undviker idle-timeouts hos proxyn på Render).
  heartbeat = setInterval(() => {
    if (elksWs.readyState === WebSocket.OPEN) {
      try {
        elksWs.ping();
      } catch (_) {
        /* noop */
      }
    }
  }, 30000);

  // --- 4a. Ljud från den som ringer -> OpenAI --------------------------------

  elksWs.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      logError(callId, 'Ogiltig JSON från 46elks:', err.message);
      return;
    }

    switch (msg.t) {
      case 'audio':
        // Skicka bara vidare ljud när AI:n inte pratar, för att undvika
        // att AI:n hör/reagerar på sin egen röst (eko/självavbrott).
        if (!isSpeaking && openaiWs.readyState === WebSocket.OPEN) {
          safeSend(openaiWs, {
            type: 'input_audio_buffer.append',
            audio: msg.data,
          });
        }
        break;

      case 'sync':
        // Buffer-checkpoint-bekräftelse från 46elks. Inget att göra i denna demo.
        break;

      case 'bye':
        log(callId, `Samtal avslutat av 46elks: ${msg.reason || 'okänd orsak'}`);
        cleanup(msg.reason);
        break;

      default:
        log(callId, 'Okänt meddelande från 46elks:', msg.t);
    }
  });

  elksWs.on('close', () => {
    log(callId, '46elks-anslutning stängd.');
    elksClosed = true;
    cleanup('hangup');
  });

  elksWs.on('error', (err) => {
    logError(callId, '46elks WS-fel:', err.message);
    cleanup('error');
  });

  // --- 4b. Ljud från OpenAI -> den som ringer --------------------------------

  openaiWs.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      logError(callId, 'Ogiltig JSON från OpenAI:', err.message);
      return;
    }

    switch (msg.type) {
      case 'response.created':
        isSpeaking = true;
        break;

      case 'response.audio.delta':
        safeSend(elksWs, { t: 'audio', data: msg.delta });
        break;

      case 'response.done':
        isSpeaking = false;
        break;

      case 'input_audio_buffer.speech_started':
        // Den som ringer börjar prata medan AI:n pratar -> avbryt AI:n (barge-in).
        if (isSpeaking) {
          log(callId, 'Avbryter AI-svar (barge-in).');
          safeSend(elksWs, { t: 'interrupt' });
          safeSend(openaiWs, { type: 'response.cancel' });
          isSpeaking = false;
          // Enligt protokollet måste "sending" skickas igen innan uppspelning
          // återupptas efter en interrupt.
          safeSend(elksWs, { t: 'sending', format: CODEC });
        }
        break;

      case 'error':
        logError(callId, 'OpenAI-fel:', JSON.stringify(msg.error || msg));
        break;

      default:
        // Övriga event (t.ex. transcript deltas) ignoreras i denna demo.
        break;
    }
  });

  openaiWs.on('close', () => {
    log(callId, 'OpenAI-anslutning stängd.');
    openaiClosed = true;
    cleanup('done');
  });

  openaiWs.on('error', (err) => {
    logError(callId, 'OpenAI WS-fel:', err.message);
    cleanup('error');
  });
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

server.listen(PORT, () => {
  log(null, `Server igång på port ${PORT}, WebSocket-endpoint: ${ELKS_PATH}`);
  log(null, `OpenAI-modell: ${OPENAI_MODEL}, ljudformat: ${CODEC}, röst: ${VOICE}`);
});

process.on('SIGTERM', () => {
  log(null, 'SIGTERM mottagen, stänger ner...');
  server.close(() => process.exit(0));
});
