import 'dotenv/config';
import express from 'express';
import { middleware, Client } from '@line/bot-sdk';
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
  GROUP_CODE = 'catch_0001',
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

const lineClient = new Client(lineConfig);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

/* =========================
 * Reply helper（reply 失敗就 push）
 * ========================= */
async function safeReplyText_(ev, text) {
  const to = ev?.source?.userId;
  try {
    await lineClient.replyMessage(ev.replyToken, { type: 'text', text });
  } catch (e) {
    console.error('[LINE replyMessage failed]', e?.message || e, {
      replyToken: ev?.replyToken,
      to,
    });
    if (to) {
      try {
        await lineClient.pushMessage(to, { type: 'text', text });
      } catch (e2) {
        console.error('[LINE pushMessage failed]', e2?.message || e2, { to });
      }
    }
  }
}

/* =========================
 * 出庫指令解析（只解析，不互轉）
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

function isOutPrefix_(text) {
  return /^(出庫|出)\b/i.test(text) || /^(出庫|出)\s*/i.test(text);
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
  const b = r.boxQty || 0;
  const p = r.pieceQty || 0;
  if (b > 0 && p > 0) return `✅ 解析：出庫 ${b} 箱 + ${p} 件\n請輸入 SKU（例如：a564）`;
  if (b > 0) return `✅ 解析：出庫 ${b} 箱\n請輸入 SKU（例如：a564）`;
  return `✅ 解析：出庫 ${p} 件\n請輸入 SKU（例如：a564）`;
}

function formatOutHelp_() {
  return '指令格式：出3箱2件 / 出3箱 / 出2件（一定要帶「箱」或「件」）';
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
  return `${y}-${m}-${d}`;
}

/* =========================
 * webhook 去重（避免重送/併發）
 * ========================= */
const SEEN_EVENT = new Map();
function seenEvent_(id) {
  if (!id) return false;
  const now = Date.now();
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
const STATE = new Map();

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
    p_warehouse_name: warehouseCode, // 傳 warehouse_code（main/withdraw/swap）
    p_out_box: outBox,
    p_out_piece: outPiece,
    p_at: atIso,
    p_created_by: createdBy,
  });
  if (error) throw error;
  return data;
}

/* =========================
 * LINE Quick Reply
 * ========================= */
function buildWarehouseQuickReply_(items) {
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
 * Handlers
 * ========================= */
async function handleTextMessage(ev) {
  const actorKey = getActorKey_(ev);
  const msg = normalizeText_(ev.message?.text ?? '');

  if (msg === '取消' || msg === 'cancel' || msg === 'c') {
    clearState_(actorKey);
    await safeReplyText_(ev, '已取消。');
    return;
  }

  const st = getState_(actorKey);

  // 0) 以「出/出庫」開頭但解析失敗 → 回提示（你現在的「出1」就會回）
  if (isOutPrefix_(msg)) {
    const parsed0 = parseOutCommand(msg);
    if (!parsed0.ok) {
      await safeReplyText_(ev, formatOutHelp_());
      return;
    }
  }

  // 1) 解析出庫（成功就進 await_sku）
  const parsed = parseOutCommand(msg);
  if (parsed.ok) {
    setState_(actorKey, {
      step: 'await_sku',
      out: { boxQty: parsed.boxQty, pieceQty: parsed.pieceQty },
    });
    await safeReplyText_(ev, formatOutParseReply(parsed));
    return;
  }

  // 2) 等 SKU
  if (st?.step === 'await_sku') {
    const sku = msg.toLowerCase();
    if (!/^[a-z0-9_]+$/.test(sku)) {
      await safeReplyText_(ev, 'SKU 格式不對（例：a564）。或輸入「取消」。');
      return;
    }

    const bizDate = getTaipeiTodayDateString_();
    const rows = await rpcGetBusinessDayStock_(GROUP_CODE, bizDate);

    const skuRows = rows.filter(r => String(r.product_sku || '').toLowerCase() === sku);
    if (!skuRows.length) {
      await safeReplyText_(ev, `找不到此 SKU 的當日庫存：${sku}\n請確認 SKU，或輸入「取消」。`);
      return;
    }

    // 每個倉庫的庫存（只保留有庫存的倉）
    const available = skuRows
      .map(r => ({
        warehouse_code: String(r.warehouse_code || '').toLowerCase(),
        warehouse_name: r.warehouse_name,
        box: Number(r.box ?? 0),
        piece: Number(r.piece ?? 0),
      }))
      .filter(r => r.box > 0 || r.piece > 0);

    if (!available.length) {
      await safeReplyText_(ev, `此 SKU 當日庫存為 0：${sku}\n或輸入「取消」。`);
      return;
    }

    // 建一個 map，之後點倉庫要做「不超扣」檢查
    const whStockMap = {};
    for (const r of available) {
      whStockMap[r.warehouse_code] = { box: r.box, piece: r.piece, name: r.warehouse_name ?? '' };
    }

    setState_(actorKey, { step: 'await_wh', out: st.out, sku, whStockMap });

    const lines = available.map(r => `- ${r.warehouse_code}（${r.warehouse_name ?? ''}）：${r.box}箱 ${r.piece}件`);
    const qr = buildWarehouseQuickReply_(
      available.map(r => ({
        label: r.warehouse_code,
        data: `act=pick_wh&wh=${encodeURIComponent(r.warehouse_code)}`,
      }))
    );

    await safeReplyText_(ev, `✅ SKU：${sku}\n請選擇倉庫：\n${lines.join('\n')}\n（或輸入「取消」）`);
    // quickReply 需要用 replyMessage 才能帶出來，所以補一個純 reply（不成功就算了）
    try {
      await lineClient.replyMessage(ev.replyToken, {
        type: 'text',
        text: `點下面選倉庫（main/withdraw/swap）`,
        quickReply: qr,
      });
    } catch (e) {
      console.error('[quickReply reply failed]', e?.message || e);
    }
    return;
  }
}

async function handlePostback(ev) {
  const actorKey = getActorKey_(ev);
  const createdBy = getCreatedBy_(ev);
  const st = getState_(actorKey);
  if (!st || st.step !== 'await_wh') return;

  const data = ev.postback?.data || '';
  const params = new URLSearchParams(data);
  if (params.get('act') !== 'pick_wh') return;

  const wh = String(params.get('wh') || '').toLowerCase();
  if (!wh) return;

  const outBox = Number(st.out?.boxQty ?? 0);
  const outPiece = Number(st.out?.pieceQty ?? 0);

  // ✅ 防呆：不允許扣超過庫存（箱對箱、件對件）
  const stock = st.whStockMap?.[wh];
  const stockBox = Number(stock?.box ?? 0);
  const stockPiece = Number(stock?.piece ?? 0);

  if (outBox > stockBox || outPiece > stockPiece) {
    await safeReplyText_(
      ev,
      `❌ 庫存不足，拒絕出庫\nSKU：${st.sku}\n倉庫：${wh}\n欲出：${outBox}箱 ${outPiece}件\n現有：${stockBox}箱 ${stockPiece}件`
    );
    // 保留狀態，讓你可直接再選一次/或取消重來
    return;
  }

  const atIso = new Date().toISOString();

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

    // 扣完 requery 回覆最新庫存
    const bizDate = getTaipeiTodayDateString_();
    const rows = await rpcGetBusinessDayStock_(GROUP_CODE, bizDate);
    const picked = rows.find(
      r => String(r.product_sku || '').toLowerCase() === st.sku && String(r.warehouse_code || '').toLowerCase() === wh
    );

    clearState_(actorKey);

    const afterBox = Number(picked?.box ?? 0);
    const afterPiece = Number(picked?.piece ?? 0);

    await safeReplyText_(
      ev,
      `✅ 出庫成功\nSKU：${st.sku}\n倉庫：${wh}\n出庫：${outBox}箱 ${outPiece}件\n最新庫存：${afterBox}箱 ${afterPiece}件`
    );
  } catch (err) {
    console.error('[fifo_out_and_log error]', err);
    clearState_(actorKey);
    await safeReplyText_(ev, `❌ 出庫失敗：${err?.message ?? 'unknown error'}`);
  }
}

async function handleEvent(ev) {
  try {
    const wid = ev.webhookEventId || ev?.deliveryContext?.eventId;
    if (seenEvent_(wid)) return;

    if (ev.type === 'message' && ev.message?.type === 'text') {
      await handleTextMessage(ev);
      return;
    }
    if (ev.type === 'postback') {
      await handlePostback(ev);
    }
  } catch (err) {
    console.error('[handleEvent error]', err);
  }
}

/* =========================
 * Routes
 * ========================= */
app.get('/health', (req, res) => res.status(200).send('ok'));

app.post('/webhook', middleware(lineConfig), (req, res) => {
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
