// index.js
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

if (!LINE_CHANNEL_SECRET || !LINE_CHANNEL_ACCESS_TOKEN) throw new Error('Missing LINE env');
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) throw new Error('Missing Supabase env');

const BOT_VER = 'V2026-01-17_CMD_BARCODE_STUB';

const lineConfig = {
  channelSecret: LINE_CHANNEL_SECRET,
  channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN,
};

const lineClient = new Client(lineConfig);

const supabase = createClient(String(SUPABASE_URL).replace(/\/+$/, ''), SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

/* =========================
 * helpers (non-command)
 * ========================= */
function getSupabaseHost_() {
  try {
    return new URL(SUPABASE_URL).host;
  } catch {
    return String(SUPABASE_URL || '');
  }
}
const SUPA_HOST = getSupabaseHost_();

async function safeReplyText_(ev, text, quickReply = undefined) {
  const to = ev?.source?.userId;
  try {
    await lineClient.replyMessage(ev.replyToken, { type: 'text', text, ...(quickReply ? { quickReply } : {}) });
  } catch (e) {
    console.error('[LINE replyMessage failed]', e?.message || e);
    if (to) {
      try {
        await lineClient.pushMessage(to, { type: 'text', text });
      } catch (e2) {
        console.error('[LINE pushMessage failed]', e2?.message || e2);
      }
    }
  }
}

/* =========================
 * time: 05:00 biz_date (TPE)
 * ========================= */
function getBizDate0500TPE_() {
  const d = new Date(Date.now() - 5 * 60 * 60 * 1000);
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d);
}

/* =========================
 * stock rpc
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
    p_warehouse_name: warehouseCode,
    p_out_box: outBox,
    p_out_piece: outPiece,
    p_at: atIso,
    p_created_by: createdBy,
  });
  if (error) throw error;
  return data;
}

/* =========================
 * warehouse label/code
 * ========================= */
const FIX_CODE_TO_NAME = new Map([
  ['main', '總倉'],
  ['main_warehouse', '總倉'],
  ['swap', '夾換品'],
  ['withdraw', '撤台'],
  ['unspecified', '未指定'],
]);

function skuKey_(s) {
  return String(s || '').trim().toLowerCase();
}
function pickNum_(v, fb = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fb;
}
function resolveWarehouseLabel_(codeOrName) {
  const k = String(codeOrName || '').trim();
  if (!k) return '未指定';
  if (FIX_CODE_TO_NAME.has(k)) return FIX_CODE_TO_NAME.get(k);
  return k;
}
function getWarehouseCodeForLabel_(labelOrCode) {
  const s = String(labelOrCode || '').trim();
  if (!s) return 'unspecified';
  const low = s.toLowerCase();
  if (/^[a-z0-9_]+$/i.test(low)) {
    if (low === 'main_warehouse') return 'main';
    return low;
  }
  for (const [code, name] of FIX_CODE_TO_NAME.entries()) {
    if (name === s) {
      if (code === 'main_warehouse') return 'main';
      return code;
    }
  }
  return 'unspecified';
}

/* =========================
 * caches
 * ========================= */
const LAST_SKU_BY_ACTOR = new Map();
const LAST_WH_BY_ACTOR = new Map();

function getActorKey_(ev) {
  const s = ev.source || {};
  return s.groupId || s.roomId || s.userId || 'unknown';
}
function getCreatedBy_(ev) {
  const s = ev.source || {};
  return s.userId || s.groupId || s.roomId || 'line';
}
function setLastSku_(actorKey, sku) {
  if (!actorKey) return;
  LAST_SKU_BY_ACTOR.set(actorKey, skuKey_(sku));
}
function getLastSku_(actorKey) {
  return skuKey_(LAST_SKU_BY_ACTOR.get(actorKey) || '');
}
function setLastWh_(actorKey, whCode) {
  if (!actorKey) return;
  LAST_WH_BY_ACTOR.set(actorKey, String(whCode || '').trim().toLowerCase() || 'unspecified');
}
function getLastWh_(actorKey) {
  return String(LAST_WH_BY_ACTOR.get(actorKey) || '').trim().toLowerCase() || '';
}

/* =========================
 * command parser (加上「條碼」)
 * ========================= */
function parseCommand(text) {
  const t = String(text || '').trim();
  if (!t) return null;

  if (/^(db|DB|版本)$/.test(t)) return { type: 'db' };

  if (!/^(查|查詢|編號|#|條碼|出庫|出|倉)/.test(t)) return null;

  const mWhSel = t.match(/^倉(?:庫)?\s*(.+)$/);
  if (mWhSel) return { type: 'wh_select', warehouse: mWhSel[1].trim() };

  const mBarcode = t.match(/^條碼[:：]?\s*(.+)$/);
  if (mBarcode) return { type: 'barcode', barcode: mBarcode[1].trim() };

  const mSkuHash = t.match(/^#\s*(.+)$/);
  if (mSkuHash) return { type: 'sku', sku: mSkuHash[1].trim() };

  const mSku = t.match(/^編號[:：]?\s*(.+)$/);
  if (mSku) return { type: 'sku', sku: mSku[1].trim() };

  const mQuery = t.match(/^查(?:詢)?\s*(.+)$/);
  if (mQuery) return { type: 'query', keyword: mQuery[1].trim() };

  const mChange = t.match(
    /^(出庫|出)\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:個|散|件))?\s*(?:(\d+))?(?:\s*(?:@|（?\(?倉庫[:：=]\s*)([^)）]+)\)?)?\s*$/,
  );
  if (mChange) {
    const box = mChange[2] ? parseInt(mChange[2], 10) : 0;
    const pieceLabeled = mChange[3] ? parseInt(mChange[3], 10) : 0;
    const pieceTail = mChange[4] ? parseInt(mChange[4], 10) : 0;

    const rawHasDigit = /\d+/.test(t);
    const hasBoxOrPieceUnit = /箱|個|散|件/.test(t);
    const piece =
      pieceLabeled ||
      pieceTail ||
      (!hasBoxOrPieceUnit && rawHasDigit && box === 0 ? parseInt(t.replace(/[^\d]/g, ''), 10) || 0 : 0);

    const warehouse = (mChange[5] || '').trim();

    return {
      type: 'change',
      action: 'out',
      box,
      piece,
      warehouse: warehouse || null,
    };
  }

  return null;
}

/* =========================
 * postback parser
 * ========================= */
function parsePostback(data) {
  const s = String(data || '').trim();
  if (!s) return null;
  const params = new URLSearchParams(s);
  const a = params.get('a');

  if (a === 'wh_select') {
    return { type: 'wh_select_postback', sku: skuKey_(params.get('sku')), wh: String(params.get('wh') || '') };
  }
  if (a === 'out') {
    return {
      type: 'out_postback',
      sku: skuKey_(params.get('sku')),
      wh: String(params.get('wh') || ''),
      box: parseInt(params.get('box') || '0', 10) || 0,
      piece: parseInt(params.get('piece') || '0', 10) || 0,
    };
  }
  return null;
}

/* =========================
 * quick reply builders
 * ========================= */
function buildQuickReplyWarehousesForQuery_(sku, whList) {
  return {
    items: whList.slice(0, 12).map((w) => ({
      type: 'action',
      action: {
        type: 'postback',
        label: `${w.label}（${w.box}箱/${w.piece}件）`.slice(0, 20),
        data: `a=wh_select&sku=${encodeURIComponent(sku)}&wh=${encodeURIComponent(w.code)}`,
        displayText: `倉 ${w.label}`,
      },
    })),
  };
}
function buildQuickReplyWarehousesForOut_(sku, outBox, outPiece, whList) {
  return {
    items: whList.slice(0, 12).map((w) => ({
      type: 'action',
      action: {
        type: 'postback',
        label: `${w.label}（${w.box}箱/${w.piece}件）`.slice(0, 20),
        data: `a=out&sku=${encodeURIComponent(sku)}&wh=${encodeURIComponent(w.code)}&box=${outBox}&piece=${outPiece}`,
        displayText: `出 ${outBox > 0 ? `${outBox}箱 ` : ''}${outPiece > 0 ? `${outPiece}件 ` : ''}@${w.label}`.trim(),
      },
    })),
  };
}

/* =========================
 * stock helpers
 * ========================= */
async function getWarehousesStockBySku_(sku) {
  const bizDate = getBizDate0500TPE_();
  const rows = await rpcGetBusinessDayStock_(GROUP_CODE, bizDate);

  const s = skuKey_(sku);
  const kept = rows
    .filter((r) => skuKey_(r.product_sku) === s)
    .map((r) => {
      const code = String(r.warehouse_code || 'unspecified').trim().toLowerCase() || 'unspecified';
      const box = pickNum_(r.box ?? 0, 0);
      const piece = pickNum_(r.piece ?? 0, 0);
      return { code, label: resolveWarehouseLabel_(code), box, piece };
    })
    .filter((w) => w.box > 0 || w.piece > 0);

  return kept;
}

async function getWarehouseSnapshot_(sku, whCode) {
  const list = await getWarehousesStockBySku_(sku);
  const code = String(whCode || '').trim().toLowerCase() || 'unspecified';
  const found = list.find((x) => x.code === code);
  return found || { code, label: resolveWarehouseLabel_(code), box: 0, piece: 0 };
}

/* =========================
 * command handlers (新增 barcode 指令：目前 DB 無條碼資料 → 回提示)
 * ========================= */
async function handleCommandMessage_(ev, parsed) {
  const actorKey = getActorKey_(ev);
  const createdBy = getCreatedBy_(ev);

  if (parsed.type === 'db') {
    const bizDate = getBizDate0500TPE_();
    await safeReplyText_(ev, `BOT=${BOT_VER}\nDB_HOST=${SUPA_HOST}\nBIZ_DATE_0500=${bizDate}`);
    return;
  }

  // ✅ 新增：條碼
  if (parsed.type === 'barcode') {
    await safeReplyText_(
      ev,
      `目前資料庫沒有「條碼→SKU」對照資料（products 也沒有條碼欄位）。\n請先建立一張 product_barcodes 表或在 products 加 barcode 欄位，之後我再把條碼查詢接上。\n你輸入的條碼：${parsed.barcode}`,
    );
    return;
  }

  if (parsed.type === 'query') {
    await safeReplyText_(ev, `目前未開放「查詢」；請用「編號 a564」或「#a564」指定 SKU`);
    return;
  }

  if (parsed.type === 'sku') {
    const sku = skuKey_(parsed.sku);
    if (!sku) return;

    setLastSku_(actorKey, sku);

    const whList = await getWarehousesStockBySku_(sku);
    if (!whList.length) {
      await safeReplyText_(ev, `無此商品庫存：${sku}`);
      return;
    }

    if (whList.length >= 2) {
      await safeReplyText_(ev, `編號：${sku}\n👉請選擇倉庫`, buildQuickReplyWarehousesForQuery_(sku, whList));
      return;
    }

    const chosen = whList[0];
    setLastWh_(actorKey, chosen.code);
    await safeReplyText_(ev, `編號：${sku}\n倉庫類別：${chosen.label}\n庫存：${chosen.box}箱${chosen.piece}件`);
    return;
  }

  if (parsed.type === 'wh_select') {
    const sku = getLastSku_(actorKey);
    if (!sku) {
      await safeReplyText_(ev, '請先用「編號 a564」或「#a564」選定商品，再選倉庫');
      return;
    }

    const whCode = getWarehouseCodeForLabel_(parsed.warehouse);
    setLastWh_(actorKey, whCode);

    const snap = await getWarehouseSnapshot_(sku, whCode);
    await safeReplyText_(ev, `編號：${sku}\n倉庫類別：${snap.label}\n庫存：${snap.box}箱${snap.piece}件`);
    return;
  }

  if (parsed.type === 'change' && parsed.action === 'out') {
    const outBox = Number(parsed.box || 0);
    const outPiece = Number(parsed.piece || 0);
    if (outBox === 0 && outPiece === 0) {
      await safeReplyText_(ev, '指令格式：出3箱2件 / 出3箱 / 出2件（出1 會視為 1件）');
      return;
    }

    const sku = getLastSku_(actorKey);
    if (!sku) {
      await safeReplyText_(ev, '請先用「編號 a564」或「#a564」選定「有庫存」商品後再出庫');
      return;
    }

    const whList = await getWarehousesStockBySku_(sku);
    if (!whList.length) {
      await safeReplyText_(ev, '所有倉庫皆無庫存，無法出庫');
      return;
    }

    let chosenWhCode = null;

    if (parsed.warehouse) {
      chosenWhCode = getWarehouseCodeForLabel_(parsed.warehouse);
    } else {
      const lastWh = getLastWh_(actorKey);
      if (lastWh && whList.some((w) => w.code === lastWh)) chosenWhCode = lastWh;
    }

    if (!chosenWhCode) {
      if (whList.length >= 2) {
        await safeReplyText_(ev, '請選擇要出庫的倉庫', buildQuickReplyWarehousesForOut_(sku, outBox, outPiece, whList));
        return;
      }
      chosenWhCode = whList[0].code;
    }

    const snapBefore = await getWarehouseSnapshot_(sku, chosenWhCode);
    if (outBox > 0 && snapBefore.box < outBox) {
      await safeReplyText_(ev, `庫存不足，無法出庫（倉別：${snapBefore.label}）\n目前庫存：${snapBefore.box}箱${snapBefore.piece}件`);
      return;
    }
    if (outPiece > 0 && snapBefore.piece < outPiece) {
      await safeReplyText_(ev, `庫存不足，無法出庫（倉別：${snapBefore.label}）\n目前庫存：${snapBefore.box}箱${snapBefore.piece}件`);
      return;
    }

    try {
      await rpcFifoOutAndLog_({
        groupCode: GROUP_CODE,
        sku,
        warehouseCode: chosenWhCode,
        outBox,
        outPiece,
        atIso: new Date().toISOString(),
        createdBy,
      });
    } catch (e) {
      console.error('[fifo_out_and_log error]', e);
      await safeReplyText_(ev, `操作失敗：${e?.message || '未知錯誤'}`);
      return;
    }

    setLastWh_(actorKey, chosenWhCode);

    const snapAfter = await getWarehouseSnapshot_(sku, chosenWhCode);
    await safeReplyText_(
      ev,
      `✅ 出庫成功\n編號：${sku}\n倉別：${snapAfter.label}\n出庫：${outBox}箱 ${outPiece}件\n👉目前庫存：${snapAfter.box}箱${snapAfter.piece}件`,
    );
  }
}

/* =========================
 * event handling
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

async function handlePostback_(ev) {
  const actorKey = getActorKey_(ev);

  const pb = parsePostback(ev?.postback?.data);
  if (!pb) return;

  if (pb.type === 'wh_select_postback') {
    const sku = pb.sku || getLastSku_(actorKey);
    if (!sku) {
      await safeReplyText_(ev, '請先用「編號 a564」或「#a564」選定商品，再選倉庫');
      return;
    }
    const whCode = getWarehouseCodeForLabel_(pb.wh);
    setLastSku_(actorKey, sku);
    setLastWh_(actorKey, whCode);

    const snap = await getWarehouseSnapshot_(sku, whCode);
    await safeReplyText_(ev, `編號：${sku}\n倉庫類別：${snap.label}\n庫存：${snap.box}箱${snap.piece}件`);
    return;
  }

  if (pb.type === 'out_postback') {
    const createdBy = getCreatedBy_(ev);
    const sku = pb.sku || getLastSku_(actorKey);
    if (!sku) {
      await safeReplyText_(ev, '請先用「編號 a564」或「#a564」選定商品後再出庫');
      return;
    }
    const whCode = getWarehouseCodeForLabel_(pb.wh);
    const outBox = Number(pb.box || 0);
    const outPiece = Number(pb.piece || 0);

    const snapBefore = await getWarehouseSnapshot_(sku, whCode);
    if (outBox > 0 && snapBefore.box < outBox) {
      await safeReplyText_(ev, `庫存不足，無法出庫（倉別：${snapBefore.label}）\n目前庫存：${snapBefore.box}箱${snapBefore.piece}件`);
      return;
    }
    if (outPiece > 0 && snapBefore.piece < outPiece) {
      await safeReplyText_(ev, `庫存不足，無法出庫（倉別：${snapBefore.label}）\n目前庫存：${snapBefore.box}箱${snapBefore.piece}件`);
      return;
    }

    try {
      await rpcFifoOutAndLog_({
        groupCode: GROUP_CODE,
        sku,
        warehouseCode: whCode,
        outBox,
        outPiece,
        atIso: new Date().toISOString(),
        createdBy,
      });
    } catch (e) {
      console.error('[fifo_out_and_log error]', e);
      await safeReplyText_(ev, `操作失敗：${e?.message || '未知錯誤'}`);
      return;
    }

    setLastSku_(actorKey, sku);
    setLastWh_(actorKey, whCode);

    const snapAfter = await getWarehouseSnapshot_(sku, whCode);
    await safeReplyText_(
      ev,
      `✅ 出庫成功\n編號：${sku}\n倉別：${snapAfter.label}\n出庫：${outBox}箱 ${outPiece}件\n👉目前庫存：${snapAfter.box}箱${snapAfter.piece}件`,
    );
  }
}

async function handleEvent_(ev) {
  const wid = ev.webhookEventId || ev?.deliveryContext?.eventId;
  if (seenEvent_(wid)) return;

  if (ev.type === 'postback') {
    await handlePostback_(ev);
    return;
  }

  if (ev.type !== 'message' || ev.message?.type !== 'text') return;

  const text = ev.message.text || '';
  const parsed = parseCommand(text);
  if (!parsed) return;

  await handleCommandMessage_(ev, parsed);
}

/* =========================
 * routes
 * ========================= */
app.get('/health', (_req, res) => res.status(200).send('ok'));

app.post('/webhook', middleware(lineConfig), (req, res) => {
  res.sendStatus(200);

  const events = req.body?.events ?? [];
  for (const ev of events) {
    console.log('[LINE EVENT]', JSON.stringify(ev));
    void handleEvent_(ev);
  }
});

app.listen(PORT, () => {
  console.log(`LINE Bot server running on port ${PORT} ver=${BOT_VER} db_host=${SUPA_HOST}`);
});
