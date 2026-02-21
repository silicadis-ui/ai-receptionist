import express from "express";

const app = express();
app.use(express.json());

app.get("/", (req, res) => {
  res.send("Server fungerar");
});

app.post("/incoming-call", async (req, res) => {
  try {
    const callId = req.body?.call_id || req.body?.id;
    if (!callId) return res.status(400).send("Missing call_id");

    const instructions = `
Du är en professionell AI-receptionist i Sverige och pratar naturlig svenska.

Du ska:
- Hälsa och säga vilket företag kunden ringt (om info finns).
- Svara på vanliga frågor (öppettider, adress, priser/tjänster).
- Ta bokningar: fråga datum/tid, tjänst, namn, telefonnummer, önskemål.
- Bekräfta alltid sammanfattning innan du avslutar.

Om kunden vill prata med personal:
- Ta ett meddelande (namn + nummer + ärende) och säg att personalen ringer upp.
    `.trim();

    const r = await fetch(`https://api.openai.com/v1/realtime/calls/${callId}/accept`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        session: { instructions }
      }),
    });

    if (!r.ok) {
      const txt = await r.text();
      return res.status(500).send(txt);
    }

    return res.sendStatus(200);
  } catch (e) {
    return res.status(500).send("Server error");
  }
});

app.listen(process.env.PORT || 3000);
