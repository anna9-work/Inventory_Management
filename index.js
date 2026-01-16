import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';

const app = express();

const lineConfig = {
  channelSecret: process.env.LINE_CHANNEL_SECRET,
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
};

app.get('/health', (req, res) => {
  res.status(200).send('ok');
});

app.post('/webhook', line.middleware(lineConfig), (req, res) => {
  // 鐵律：立刻回 200，避免 LINE webhook 重送
  res.sendStatus(200);

  for (const ev of req.body.events ?? []) {
    console.log('[LINE EVENT]', JSON.stringify(ev));
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`LINE Bot server running on port ${port}`);
});
