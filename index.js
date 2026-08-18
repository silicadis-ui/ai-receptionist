#!/usr/bin/env node
/**
 * 46elks <-> OpenAI Realtime API (GA) voice bridge (Node.js)
 *
 * 46elks-protokoll: https://46elks.se/tutorials/real-time-two-way-voice-calls-with-websocket
 *
 * Denna version:
 *  - Har INGA hårdkodade system instructions och INGEN hårdkodad hälsning.
 *    All personlighet/instruktion styrs uteslutande av en server-lagrad
 *    OpenAI Prompt (process.env.OPENAI_PROMPT_ID), refererad via session.prompt.id.
 *  - Skickar alltid vidare uppringarens ljud till OpenAI (även medan AI:n
 *    pratar). Det är just detta som krävs för att server_vad ska kunna
 *    upptäcka att användaren börjat prata mitt i ett AI-svar - filtrerar man
 *    bort ljudet under uppspelning (vilket den tidigare versionen gjorde)
 *    upptäcks aldrig avbrott, vilket är den vanligaste orsaken till att
 *    AI:n "pratar på" och verkar lyssna dåligt.
 *  - Barge-in: vid input_audio_buffer.speech_started skickas response.cancel
 *    till OpenAI OCH interrupt till 46elks, så att både modellens pågående
 *    svar och den redan buffrade uppspelningen hos 46elks avbryts direkt.
 *  - turn_detection: server_vad med rimliga default-värden (går att justera
 *    via miljövariabler utan kodändring).
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
const OPENAI_PROMPT_ID = process.env.OPENAI_PROMPT_ID;
const OPENAI_PROMPT_VERSION = process.env.OPENAI_PROMPT_VERSION || undefined; // valfritt
const OPENAI_MODEL = process.env.OPENAI_REALTIME_MODEL || 'gpt-realtime-1.5';
const OPENAI_VOICE = process.env.OPENAI_REALTIME_VOICE || 'marin';
const OPENAI_NOISE_REDUCTION = process.env.OPENAI_NOISE_REDUCTION || 'near_field';
const ELKS_PATH = process.env.ELKS_WS_PATH || '/voice';

const AUDIO_FORMAT = { type: 'audio/pcm', rate: 24000 };
const ELKS_CODEC = 'pcm_24000'; // 46elks-motsvarighet till audio/pcm @ 24kHz

// Justerbar VAD-konfiguration (server_vad), utan att röra koden.
const VAD_THRESHOLD = Number(process.env.OPENAI_VAD_THRESHOLD || 0.75);
const VAD_PREFIX_PADDING_MS = Number(process.env.OPENAI_VAD_PREFIX_PADDING_MS || 300);
const VAD_SILENCE_DURATION_MS = Number(process.env.OPENAI_VAD_SILENCE_DURATION_MS || 530);

if (!OPENAI_API_KEY) {
  // eslint-disable-next-line no-console
  console.error('FATAL: Miljövariabeln OPENAI_API_KEY saknas.');
  process.exit(1);
}

if (!OPENAI_PROMPT_ID) {
  // eslint-disable-next-line no-console
  console.error('FATAL: Miljövariabeln OPENAI_PROMPT_ID saknas. Sätt den till din OpenAI Prompt-ID (pmpt_...).');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Loggning
// ---------------------------------------------------------------------------

function log(callId, ...args) {
  console.log(`[${new Date().toISOString()}]${callId ? ` [${callId}]` : ''}`, ...args);
}

function logError(callId, ...args) {
  console.error(`[${new Date().toISOString()}]${callId ? ` [${callId}]` : ''}`, ...args);
}

// ---------------------------------------------------------------------------
// Express-app (health check åt Render)
// ---------------------------------------------------------------------------

const app = express();

app.get('/', (req, res) => {
  res.status(200).send('46elks <-> OpenAI Realtime-brygga är igång.');
});

app.get('/healthz', (req, res) => {
  res.status(200).json({ status: 'ok', uptime: process.uptime() });
});

const server = http.createServer(app);

// ---------------------------------------------------------------------------
// WebSocket-server för 46elks på /voice
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
  let callId = randomUUID().slice(0, 8);
  let openaiWs = null;
  let isSpeaking = false; // true medan OpenAI har ett pågående/aktivt svar
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
  log(callId, `Samtal från ${helloMsg.from || '?'} till ${helloMsg.to || '?'}`);

  // --- 2. Anslut till OpenAI Realtime API (GA) -----------------------------

  openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_MODEL)}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
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

  // --- 3. Konfigurera sessionen ---------------------------------------------
  //
  // Ingen "instructions"-text sätts här. All personlighet/beteende kommer
  // uteslutande från den server-lagrade prompten (session.prompt.id).

  const promptRef = { id: OPENAI_PROMPT_ID };
  if (OPENAI_PROMPT_VERSION) promptRef.version = OPENAI_PROMPT_VERSION;

  safeSend(openaiWs, {
    type: 'session.update',
    session: {
      type: 'realtime',
      output_modalities: ['audio'],
      prompt: promptRef,
      audio: {
      input: {
  format: AUDIO_FORMAT,
  noise_reduction: {
    type: OPENAI_NOISE_REDUCTION,
  },
  turn_detection: {
            type: 'server_vad',
            threshold: VAD_THRESHOLD,
            prefix_padding_ms: VAD_PREFIX_PADDING_MS,
            silence_duration_ms: VAD_SILENCE_DURATION_MS,
            create_response: true,
            interrupt_response: true,
          },
        },
output: {
  format: AUDIO_FORMAT,
  voice: OPENAI_VOICE,
        },
      },
    },
  });

  // Trigga ett första svar så AI:n kan öppna samtalet - ingen hårdkodad text,
  // vad som sägs (om något) styrs helt av prompten kopplad till OPENAI_PROMPT_ID.
  safeSend(openaiWs, { type: 'response.create' });

  // --- 4. Deklarera ljudformat till 46elks ----------------------------------

  safeSend(elksWs, { t: 'sending', format: ELKS_CODEC });
  safeSend(elksWs, { t: 'listening', format: ELKS_CODEC });

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
  //
  // OBS: ljudet skickas ALLTID vidare, oavsett om AI:n pratar eller inte.
  // Detta är nödvändigt för att server_vad ska upptäcka att användaren
  // börjar prata mitt i ett pågående AI-svar (barge-in). Att filtrera bort
  // ljud under uppspelning var orsaken till att AI:n tidigare "pratade på"
  // och missade vad användaren sa.

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
        if (openaiWs.readyState === WebSocket.OPEN) {
          safeSend(openaiWs, {
            type: 'input_audio_buffer.append',
            audio: msg.data,
          });
        }
        break;

      case 'sync':
        // Buffer-checkpoint-bekräftelse från 46elks. Inget att göra här.
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

  // --- 4b. Ljud från OpenAI -> den som ringer, samt barge-in -----------------

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

      // GA-eventnamnet är "response.output_audio.delta". Vi lyssnar även på
      // det äldre namnet "response.audio.delta" ifall kontot/kontexten
      // fortfarande returnerar det, så ljudet aldrig tappas bort.
      case 'response.output_audio.delta':
      case 'response.audio.delta':
        safeSend(elksWs, { t: 'audio', data: msg.delta });
        break;

      case 'response.done':
        isSpeaking = false;
        break;

     case 'input_audio_buffer.speech_started':
  // Användaren har börjat prata - stoppa uppspelning mot uppringaren direkt.
  log(callId, 'Tal upptäckt hos uppringaren (speech_started).');

  // Stoppa alltid eventuell buffrad AI-röst hos 46elks direkt.
  safeSend(elksWs, { t: 'interrupt' });

  if (isSpeaking) {
    log(callId, 'Avbryter pågående AI-svar (barge-in).');
    isSpeaking = false;
  }

  // Enligt 46elks-protokollet måste "sending" skickas igen efter interrupt.
  safeSend(elksWs, { t: 'sending', format: ELKS_CODEC });

  break;

      case 'input_audio_buffer.speech_stopped':
        log(callId, 'Uppringaren slutade prata (speech_stopped).');
        break;

      case 'error':
        logError(callId, 'OpenAI-fel:', JSON.stringify(msg.error || msg));
        break;

      default:
        // Övriga event (transcript-deltas, conversation.item.*, etc.)
        // ignoreras i denna brygga.
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
  log(null, `OpenAI-modell: ${OPENAI_MODEL}, prompt: ${OPENAI_PROMPT_ID}, ljudformat: ${ELKS_CODEC}`);
});

process.on('SIGTERM', () => {
  log(null, 'SIGTERM mottagen, stänger ner...');
  server.close(() => process.exit(0));
});
