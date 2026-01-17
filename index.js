// index.js
import 'dotenv/config';
import express from 'express';
import line, { middleware } from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

const app = express();

/* =========================
 * ENV
 * ========================= */
const {
  PORT = 3000,
  LINE_CHANNEL_SECRET,
  LINE_CHANNEL_ACCESS_TOKEN,
  SUPABASE_URL,
  SUPABASE_SERVICE_KEY,
  GROUP_CODE = 'catch_0001', // 單店 bot：預設這個群組代碼
} = process.env;

if (!LINE_CHANNEL_SECRET || !LINE_CHANNEL_ACCESS_TOKEN) {
  throw new Error('Missing LINE_CHANNEL_SECRET / LINE_CHANNEL_ACCESS_TOKEN');
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  throw new Error('Missing SUPABASE_URL / SUPABASE_SERVICE_KEY');
}

const lineConfig = {
  channelSecret: LINE_CHANNEL_SECRET,
  channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN,
};

const lineClient = new line.Client(lineConfig);
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

/* =========================
 * 鐵律：箱/件不互轉（只解析）
 * ========================= */

function normalizeText_(s) {
  if (typeof s !== 'string') return '';
  return s
    .replace(/\u3000/g, ' ')
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
    else pieceQty += n;
    return _;
  });

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
  if (b > 0 && p > 0) return `✅ 解析：出庫 ${b} 箱 + ${p} 件\n請輸入 SKU（例如：a564）`;
  if (b > 0) return `✅ 解析：出庫 ${b} 箱\n請輸入 SKU（例如：a564）`;
  return `✅ 解析：出庫 ${p} 件\n請輸入 SKU（例如：a564）`;
}

/* =========================
 * 方案A：台北當天日期（不做 05:00 分界）
 * ========================= */
function getTaipeiTodayDateString_() {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Taipei',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date());
  const y = parts.find(p => p.type === 'year')?.value;
  const m = parts.find(p => p.type === 'month')?.value;
  const d = parts.find(p => p.type === 'day')?.value;
  return `${y}-${m}-${d}`; // YYYY-MM-DD
}

/* =========================
 * webhook 去重（避免重送/併發）
 * ========================= */
const SEEN_EVENT = new Map(); // id -> ts
function seenEvent_(id) {
  if (!id) return false;
  const now = Date.now();
  // 清 10 分鐘前
  for (const [k, ts] of SEEN_EVENT.entries()) {
    if (now - ts > 10 * 60 * 1000) SEEN_EVENT.delete(k);
  }
  if (SEEN_EVENT.has(id)) return true;
  SEEN_EVENT.set(id, now);
  return false;
}

/* =========================
 * 狀態機（每個對話來源一份）
 * ========================= */
const STATE = new Map(); // key -> { step, out, sku }

function getActorKey_(ev) {
  const s = ev.source || {};
  return s.groupId || s.roomId || s.userId || 'unknown';
}

function getCreatedBy_(ev) {
  const s = ev.source || {};
  return s.userId || s.groupId || s.roomId || 'line';
}

function clearState_(key) {
  STATE.delete(key);
}

function setState_(key, next) {
  STATE.set(key, { ...next, updatedAt: Date.now() });
}

function getState_(key) {
  const st = STATE.get(key);
  if (!st) return null;
  // 30 分鐘過期
  if (Date.now() - (st.updatedAt || 0) > 30 * 60 * 1000) {
    STATE.delete(key);
    return null;
  }
  return st;
}

/* =========================
 * Supabase RPC
 * ========================= */
async function rpcGetBusinessDayStock_(groupCode, bizDateStr) {
  const { data, error } = await supabase.rpc('get_business_day_stock', {
    p_group: groupCode,
    p_biz_date: bizDateStr,
  });
  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

async function rpcFifoOutAndLog_({ groupCode, sku, warehouseCode, outBox, outPiece, atIso, createdBy }) {
  const { data, error } = await supabase.rpc('fifo_out_and_log', {
    p_group: groupCode,
    p_product_sku: sku,
    p_warehouse_name: warehouseCode, // 注意：這裡傳 warehouse_code（main/withdraw/swap）
    p_out_box: outBox,
    p_out_piece: outPiece,
    p_at: atIso, // timestamptz
    p_created_by: createdBy,
  });
  if (error) throw error;
  return data;
}

/* =========================
 * LINE 回覆工具
 * ========================= */
function buildWarehouseQuickReply_(items) {
  // items: [{ label, data }]
  return {
    items: items.slice(0, 13).map(it => ({
      type: 'action',
      action: {
        type: 'postback',
        label: it.label,
        data: it.data,
        displayText: it.label,
      },
    })),
  };
}

/* =========================
 * 主流程
 * ========================= */
async function handleTextMessage(ev) {
  const actorKey = getActorKey_(ev);
  const createdBy = getCreatedBy_(ev);
  const text = ev.message?.text ?? '';
  const msg = normalizeText_(text);

  // 允許取消
  if (msg === '取消' || msg === 'cancel' || msg === 'c') {
    clearState_(actorKey);
    await lineClient.replyMessage(ev.replyToken, { type: 'text', text: '已取消。' });
    return;
  }

  const st = getState_(actorKey);

  // step1：解析出庫指令
  const parsed = parseOutCommand(msg);
  if (parsed.ok) {
    setState_(actorKey, { step: 'await_sku', out: { boxQty: parsed.boxQty, pieceQty: parsed.pieceQty } });
    await lineClient.replyMessage(ev.replyToken, { type: 'text', text: formatOutParseReply(parsed) });
    return;
  }

  // step2：等 SKU
  if (st?.step === 'await_sku') {
    const sku = msg.toLowerCase();
    if (!/^[a-z0-9_]+$/.test(sku)) {
      await lineClient.replyMessage(ev.replyToken, { type: 'text', text: 'SKU 格式不對（例：a564）。或輸入「取消」。' });
      return;
    }

    const bizDate = getTaipeiTodayDateString_();
    const rows = await rpcGetBusinessDayStock_(GROUP_CODE, bizDate);

    const skuRows = rows.filter(r => String(r.product_sku || '').toLowerCase() === sku);
    if (!skuRows.length) {
      await lineClient.replyMessage(ev.replyToken, {
        type: 'text',
        text: `找不到此 SKU 的當日庫存：${sku}\n請確認 SKU，或輸入「取消」。`,
      });
      return;
    }

    // 只給有庫存的倉（box>0 或 piece>0）
    const available = skuRows
      .map(r => ({
        warehouse_code: r.warehouse_code,
        warehouse_name: r.warehouse_name,
        box: Number(r.box ?? 0),
        piece: Number(r.piece ?? 0),
      }))
      .filter(r => r.box > 0 || r.piece > 0);

    if (!available.length) {
      await lineClient.replyMessage(ev.replyToken, {
        type: 'text',
        text: `此 SKU 當日庫存為 0：${sku}\n或輸入「取消」。`,
      });
      return;
    }

    setState_(actorKey, { step: 'await_wh', out: st.out, sku });

    const lines = available.map(r => `- ${r.warehouse_code}（${r.warehouse_name ?? ''}）：${r.box}箱 ${r.piece}件`);
    const qr = buildWarehouseQuickReply_(
      available.map(r => ({
        label: r.warehouse_code,
        data: `act=pick_wh&wh=${encodeURIComponent(r.warehouse_code)}`,
      }))
    );

    await lineClient.replyMessage(ev.replyToken, {
      type: 'text',
      text: `✅ SKU：${sku}\n請選擇倉庫：\n${lines.join('\n')}\n（或輸入「取消」）`,
      quickReply: qr,
    });
    return;
  }

  // 其他文字不處理
}

async function handlePostback(ev) {
  const actorKey = getActorKey_(ev);
  const createdBy = getCreatedBy_(ev);
  const st = getState_(actorKey);
  if (!st?.step || st.step !== 'await_wh') return;

  const data = ev.postback?.data || '';
  const params = new URLSearchParams(data);
  const act = params.get('act');
  if (act !== 'pick_wh') return;

  const wh = (params.get('wh') || '').toLowerCase();
  if (!wh) return;

  const outBox = Number(st.out?.boxQty ?? 0);
  const outPiece = Number(st.out?.pieceQty ?? 0);

  // 直接扣庫（先不做 confirm，避免多一步；要 confirm 我再幫你加）
  const atIso = new Date().toISOString(); // timestamptz

  try {
    await rpcFifoOutAndLog_({
      groupCode: GROUP_CODE,
      sku: st.sku,
      warehouseCode: wh,
      outBox,
      outPiece,
      atIso,
      createdBy,
    });

    // 扣完立刻 requery 快照回覆最新庫存（方案A：台北當天）
    const bizDate = getTaipeiTodayDateString_();
    const rows = await rpcGetBusinessDayStock_(GROUP_CODE, bizDate);
    const skuRows = rows.filter(r => String(r.product_sku || '').toLowerCase() === st.sku);
    const picked = skuRows.find(r => String(r.warehouse_code || '').toLowerCase() === wh);

    clearState_(actorKey);

    const afterBox = Number(picked?.box ?? 0);
    const afterPiece = Number(picked?.piece ?? 0);

    await lineClient.replyMessage(ev.replyToken, {
      type: 'text',
      text:
        `✅ 出庫成功\n` +
        `SKU：${st.sku}\n` +
        `倉庫：${wh}\n` +
        `出庫：${outBox}箱 ${outPiece}件\n` +
        `最新庫存：${afterBox}箱 ${afterPiece}件`,
    });
  } catch (err) {
    console.error('[fifo_out_and_log error]', err);
    clearState_(actorKey);
    await lineClient.replyMessage(ev.replyToken, {
      type: 'text',
      text: `❌ 出庫失敗：${(err && err.message) ? err.message : 'unknown error'}`,
    });
  }
}

async function handleEvent(ev) {
  try {
    // 去重（若 LINE 有給 webhookEventId）
    const wid = ev.webhookEventId || ev?.deliveryContext?.eventId;
    if (seenEvent_(wid)) return;

    if (ev.type === 'message' && ev.message?.type === 'text') {
      await handleTextMessage(ev);
      return;
    }

    if (ev.type === 'postback') {
      await handlePostback(ev);
      return;
    }
  } catch (err) {
    console.error('[handleEvent error]', err);
  }
}

/* =========================
 * Routes
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
    void handleEvent(ev);
  }
});

app.listen(PORT, () => {
  console.log(`LINE Bot server running on port ${PORT}`);
});
