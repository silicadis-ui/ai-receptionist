import express from "express";
import http from "http";
import { WebSocketServer } from "ws";

const app = express();
app.use(express.json());

app.get("/", (req, res) => {
  res.send("AE Solutions demo-server fungerar");
});

const server = http.createServer(app);

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  console.log("46elks WebSocket ansluten");

  ws.on("message", (message) => {
    console.log("Meddelande från 46elks:", message.toString());

    try {
      const data = JSON.parse(message.toString());

      if (data.t === "hello") {
        console.log("Samtal startat:", data.callid);

        ws.send(JSON.stringify({
          t: "sending",
          format: "pcm_24000"
        }));

        ws.send(JSON.stringify({
          t: "listening",
          format: "pcm_24000"
        }));
      }

      if (data.t === "audio") {
        console.log("Tar emot ljud...");
      }

      if (data.t === "bye") {
        console.log("Samtal avslutat:", data.reason);
      }
    } catch (error) {
      console.error("Fel vid meddelande:", error);
    }
  });

  ws.on("close", () => {
    console.log("WebSocket stängd");
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(`Server körs på port ${PORT}`);
});
