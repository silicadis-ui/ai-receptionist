import express from "express";

const app = express();
app.use(express.json());

app.get("/", (req, res) => {
  res.send("Server fungerar");
});

app.listen(process.env.PORT || 3000);
