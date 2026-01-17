// index.js
import 'dotenv/config';
import express from 'express';
import line, { middleware } from '@line/bot-sdk';

const app = express();

const lineConfig = {
  channelSecret: process.env.LINE_CHANNEL_SECRET,
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
};

const lineClient = new line.Client(lineConfig);

/* =========================
 * 出庫指令解析（只解析，不扣庫）
 * 支援：出3箱2件 / 出3箱 / 出2件 / 出庫3箱2件
 * 規則：整句必須是出庫指令（避免誤判）；箱/件不互轉
 * ========================= */

function normalizeText_(s) {
  if (typeof s !== 'string') return '';
  return s
    .replace(/\u3000/g, ' ') // 全形空白
    .replace(/[，,]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function parsePositiveInt_(raw) {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  if (!Number.isSafeInteger(n) || n <= 0) return null;
  return n;
}

function parseOutCommand(input) {
  const text = normalizeText_(input);
  if (!text) return { ok: false, error: 'empty' };

  const mPrefix = text.match(/^(出庫|出)\s*(.+)$/i);
  if (!mPrefix) return { ok: false, error: 'no_prefix' };

  const rest = mPrefix[2].trim();
  if (!rest) return { ok: false, error: 'no_amount' };

  // token：<數字><單位>
  const tokenRe = /(\d+)\s*(箱|件|散|個|pcs|pc)\b/gi;

  let boxQty = 0;
  let pieceQty = 0;
  let any = false;

  rest.replace(tokenRe, (_, numRaw, unitRaw) => {
    any = true;
    const n = parsePositiveInt_(numRaw);
    if (n == null) return _;
    const unit = String(unitRaw).toLowerCase();
    if (unit === '箱') boxQty += n;
    else pieceQty += n; // 件/散/個/pcs/pc -> piece
    return _;
  });

  // 不允許多餘文字（例如：出3箱2件給我）
  const restStripped = rest.replace(tokenRe, '').replace(/\s+/g, '').trim();
  if (!any) return { ok: false, error: 'no_tokens', normalized: text };
  if (restStripped.length > 0) return { ok: false, error: 'has_extra_text', normalized: text };
  if (boxQty <= 0 && pieceQty <= 0) return { ok: false, error: 'non_positive', normalized: text };

  return {
    ok: true,
    action: 'out',
    boxQty,
    pieceQty,
    normalized: `${mPrefix[1]} ${boxQty ? `${boxQty}箱` : ''}${boxQty && pieceQty ? ' ' : ''}${
      pieceQty ? `${pieceQty}件` : ''
    }`.trim(),
  };
}

function formatOutParseReply(r) {
  if (!r?.ok) return '指令格式：出3箱2件 / 出3箱 / 出2件（不要加多餘文字）';
  const b = r.boxQty || 0;
  const p = r.pieceQty || 0;
  if (b > 0 && p > 0) return `✅ 解析：出庫 ${b} 箱 + ${p} 件\n（下一步我會接出庫扣庫 RPC）`;
  if (b > 0) return `✅ 解析：出庫 ${b} 箱\n（下一步我會接出庫扣庫 RPC）`;
  return `✅ 解析：出庫 ${p} 件\n（下一步我會接出庫扣庫 RPC）`;
}

/* =========================
 * LINE handlers
 * ========================= */

async function handleEvent(ev) {
  try {
    if (ev.type !== 'message') return;
    if (!ev.message || ev.message.type !== 'text') return;

    const text = ev.message.text ?? '';
    const parsed = parseOutCommand(text);

    // 目前只做解析回覆（不扣庫）
    if (!parsed.ok) return;

    await lineClient.replyMessage(ev.replyToken, {
      type: 'text',
      text: formatOutParseReply(parsed),
    });
  } catch (err) {
    console.error('[handleEvent error]', err);
  }
}

/* =========================
 * Express routes
 * ========================= */

app.get('/health', (req, res) => {
  res.status(200).send('ok');
});

app.post('/webhook', middleware(lineConfig), (req, res) => {
  // 鐵律：立刻回 200，避免 LINE webhook 重送
  res.sendStatus(200);

  const events = req.body?.events ?? [];
  for (const ev of events) {
    console.log('[LINE EVENT]', JSON.stringify(ev));
    // 不阻塞 webhook 回應
    void handleEvent(ev);
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`LINE Bot server running on port ${port}`);
});
