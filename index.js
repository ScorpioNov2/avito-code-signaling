import http from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';

const port = process.env.PORT || 443;
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS || "";

const MAX_TOPICS_PER_SOCKET = 16;
const MAX_TOPIC_LENGTH = 100;

// Manages the state of active rooms/topics subscribed by each connection
// Map structure: [ws] -> string[] (array of active subscribed topics)
const socketStates = new Map();

function isOriginAllowed(req, allowedOrigins) {
  const allowList = allowedOrigins.split(',').map((origin) => origin.trim()).filter(Boolean);
  if (allowList.length === 0) return true;
  const origin = req.headers.origin;
  return origin && allowList.includes(origin);
}

function validTopics(value) {
  return Array.isArray(value) 
    ? value.filter((topic) => typeof topic === 'string' && topic.length <= MAX_TOPIC_LENGTH) 
    : [];
}

const server = http.createServer((req, res) => {
  if (!isOriginAllowed(req, ALLOWED_ORIGINS)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end('Origin not allowed');
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Signaling server fallback is online!');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  // Initialize an empty topic array for the connection
  socketStates.set(ws, []);

  ws.on('message', (rawMessage) => {
    let message;
    try {
      message = JSON.parse(typeof rawMessage === 'string' ? rawMessage : new TextDecoder().decode(rawMessage));
    } catch {
      return;
    }
    if (!message || typeof message.type !== 'string') return;

    const currentTopics = socketStates.get(ws) || [];

    switch (message.type) {
      case 'subscribe': {
        const incoming = validTopics(message.topics);
        for (const topic of incoming) {
          if (!currentTopics.includes(topic) && currentTopics.length < MAX_TOPICS_PER_SOCKET) {
            currentTopics.push(topic);
          }
        }
        socketStates.set(ws, currentTopics);
        break;
      }

      case 'unsubscribe': {
        const incoming = validTopics(message.topics);
        const filtered = currentTopics.filter((topic) => !incoming.includes(topic));
        socketStates.set(ws, filtered);
        break;
      }

      case 'publish': {
        if (typeof message.topic === 'string') {
          const targetTopic = message.topic;
          
          // Filter all sockets that have subscribed to the targeted topic
          const receivers = [];
          for (const [client, topics] of socketStates.entries()) {
            if (topics.includes(targetTopic) && client.readyState === WebSocket.OPEN) {
              receivers.push(client);
            }
          }

          // Attach the total number of target clients in the room to the payload
          const payload = JSON.stringify({ ...message, clients: receivers.length });
          
          for (const receiver of receivers) {
            try {
              receiver.send(payload);
            } catch {
              receiver.close();
            }
          }
        }
        break;
      }

      case 'ping':
        ws.send(JSON.stringify({ type: 'pong' }));
        break;
    }
  });

  const cleanUp = (code) => {
    socketStates.delete(ws);
    // Standardize connection close codes
    const validCode = code >= 1000 && code < 1005 ? code : 1000;
    try {
      ws.close(validCode);
    } catch {}
  };

  ws.on('close', (code) => cleanUp(code));
  ws.on('error', () => cleanUp(1006));
});

server.listen(port, () => {
  console.log(`[READY] Node.js y-webrtc signaling server running on port: ${port}`);
});
