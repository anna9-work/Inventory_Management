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
  GAS_WEBHOOK_URL = '',
  GAS_WEBHOOK_SECRET = '',
} = process.env;

if (!LINE_CHANNEL_SECRET || !LINE_CHANNEL_ACCESS_TOKEN) throw new Error('Missing LINE env');
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) throw new Error('Missing Supabase env');

const BOT_VER = 'V2026-01-17_DB_DEDUP_PUSH_GAS_PIECE_UNITS';

const lineConfig = {
  channelSecret: LINE_CHANNEL_SECRET,
  channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN,
};

const lineClient = new Client(lineConfig);

const supabase = createClient(String(SUPABASE_URL).replace(/\/+$/, ''), SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

/* =========================
 * helpers
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

function skuKey_(s) {
  return String(s || '').trim().toLowerCase();
}
function pickNum_(v, fb = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fb;
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

function tpeNowISO_() {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(new Date());
  return s.replace(' ', 'T') + '+08:00';
}

/* =========================
 * GAS push (fire-and-forget)
 * ========================= */
function getGasCallUrl_() {
  const base = String(GAS_WEBHOOK_URL || '').trim();
  const secret = String(GAS_WEBHOOK_SECRET || '').trim();
  if (!base || !secret) return null;
  const clean = base.replace(/\?.*$/, '');
  return `${clean}?secret=${encodeURIComponent(secret)}`;
}

async function postToGAS_(payload) {
  const url = getGasCallUrl_();
  if (!url) return;

  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      console.warn('[GAS WARN]', res.status, txt.slice(0, 300));
    }
  } catch (e) {
    console.warn('[GAS ERROR]', e?.message || e);
  }
}

function fireAndForgetGas_(payload) {
  postToGAS_(payload).catch(() => {});
}

/* =========================
 * RPC
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
 * DB dedup (跨重啟/多 instance)
 * ========================= */
async function acquireEventDedup_(eventId, ev) {
  const id = String(eventId || '').trim();
  if (!id) return true; // 沒有 id 就不擋（極少）

  const payload = {
    event_id: id,
    group_code: String(GROUP_CODE || '').trim().toLowerCase(),
    line_user_id: ev?.source?.userId || null,
    event_type: ev?.type || null,
  };

  const { error } = await supabase.from('line_event_dedup').insert(payload);

  if (!error) return true;

  // 23505 = unique_violation (primary key)
  if (String(error.code) === '23505') {
    console.log('[DEDUP] duplicated event_id => skip', id);
    return false;
  }

  // 其他錯誤：不要擋主流程（避免 dedup 表出問題導致 bot 全死）
  console.warn('[DEDUP WARN] insert failed, allow continue:', error?.message || error);
  return true;
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
 * barcode lookup (products.barcode)
 * ========================= */
function normalizeBarcode_(s) {
  return String(s || '').trim();
}

async function lookupProductsByBarcode_(barcodeRaw) {
  const barcode = normalizeBarcode_(barcodeRaw);
  if (!barcode) return [];

  const { data, error } = await supabase
    .from('products')
    .select('product_sku, product_name')
    .eq('barcode', barcode)
    .limit(10);

  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  return rows
    .map((r) => ({
      sku: skuKey_(r.product_sku),
      name: String(r.product_name || '').trim(),
    }))
    .filter((x) => x.sku);
}

/* =========================
 * TODAY STOCK LIST cache (for 查/查詢)
 * ========================= */
const STOCK_LIST_CACHE = new Map(); // key = `${group}::${bizDate}` -> { ts, rows }
const STOCK_LIST_TTL_MS = 3000;

function getStockCacheKey_(group, bizDate) {
  return `${String(group || '').trim().toLowerCase()}::${bizDate}`;
}

async function getTodayStockRows_(group) {
  const bizDate = getBizDate0500TPE_();
  const key = getStockCacheKey_(group, bizDate);
  const cached = STOCK_LIST_CACHE.get(key);
  if (cached && Date.now() - cached.ts < STOCK_LIST_TTL_MS) return cached.rows;

  const rows = await rpcGetBusinessDayStock_(group, bizDate);

  const kept = rows
    .map((r) => {
      const sku = skuKey_(r.product_sku || r['貨品編號']);
      const name = String(r.product_name || r['貨品名稱'] || '').trim();
      const box = pickNum_(r.box ?? r['庫存箱數'] ?? 0, 0);
      const piece = pickNum_(r.piece ?? r['庫存散數'] ?? 0, 0);
      return { sku, name, box, piece };
    })
    .filter((x) => x.sku && (x.box > 0 || x.piece > 0));

  STOCK_LIST_CACHE.set(key, { ts: Date.now(), rows: kept });
  return kept;
}

function buildQuickReplyForProducts_(items) {
  return {
    items: items.slice(0, 12).map((p) => ({
      type: 'action',
      action: {
        type: 'message',
        label: `${p.name || p.sku}`.slice(0, 20),
        text: `編號 ${p.sku}`,
      },
    })),
  };
}

async function searchInTodayStock_(group, keywordRaw) {
  const kw = String(keywordRaw || '').trim();
  if (!kw) return [];

  const kwLower = kw.toLowerCase();
  const rows = await getTodayStockRows_(group);

  const seen = new Set();
  const out = [];

  for (const r of rows) {
    if (!r.sku || seen.has(r.sku)) continue;
    const nameLower = String(r.name || '').toLowerCase();
    const skuLower = String(r.sku || '').toLowerCase();
    if (nameLower.includes(kwLower) || skuLower.includes(kwLower)) {
      seen.add(r.sku);
      out.push({ sku: r.sku, name: r.name || r.sku });
      if (out.length >= 10) break;
    }
  }
  return out;
}

/* =========================
 * command parser
 * 出1 / 出1件 / 出1個 / 出1散 -> 都是件數
 * 出1（無單位） -> 視為 1件
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
 * stock helpers by sku
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
 * handlers
 * ========================= */
async function handleSkuFlow_(ev, sku) {
  const actorKey = getActorKey_(ev);
  const s = skuKey_(sku);
  if (!s) return;

  setLastSku_(actorKey, s);

  const whList = await getWarehousesStockBySku_(s);
  if (!whList.length) {
    await safeReplyText_(ev, `無此商品庫存：${s}`);
    return;
  }

  if (whList.length >= 2) {
    await safeReplyText_(ev, `編號：${s}\n👉請選擇倉庫`, buildQuickReplyWarehousesForQuery_(s, whList));
    return;
  }

  const chosen = whList[0];
  setLastWh_(actorKey, chosen.code);
  await safeReplyText_(ev, `編號：${s}\n倉庫類別：${chosen.label}\n庫存：${chosen.box}箱${chosen.piece}件`);
}

async function handleCommandMessage_(ev, parsed) {
  const actorKey = getActorKey_(ev);
  const createdBy = getCreatedBy_(ev);

  if (parsed.type === 'db') {
    const bizDate = getBizDate0500TPE_();
    await safeReplyText_(ev, `BOT=${BOT_VER}\nDB_HOST=${SUPA_HOST}\nBIZ_DATE_0500=${bizDate}`);
    return;
  }

  if (parsed.type === 'query') {
    const list = await searchInTodayStock_(GROUP_CODE, parsed.keyword);
    if (!list.length) {
      await safeReplyText_(ev, `無此商品庫存（只查當日有庫存清單）\n關鍵字：${parsed.keyword}`);
      return;
    }
    if (list.length === 1) {
      await handleSkuFlow_(ev, list[0].sku);
      return;
    }
    await safeReplyText_(ev, `找到以下品項（只含當日有庫存）`, buildQuickReplyForProducts_(list));
    return;
  }

  if (parsed.type === 'barcode') {
    const list = await lookupProductsByBarcode_(parsed.barcode);
    if (!list.length) {
      await safeReplyText_(ev, `無此條碼：${normalizeBarcode_(parsed.barcode)}`);
      return;
    }
    if (list.length === 1) {
      await handleSkuFlow_(ev, list[0].sku);
      return;
    }
    await safeReplyText_(
      ev,
      `條碼找到多筆，請選擇商品`,
      buildQuickReplyForProducts_(list.map((x) => ({ sku: x.sku, name: x.name || x.sku }))),
    );
    return;
  }

  if (parsed.type === 'sku') {
    await handleSkuFlow_(ev, parsed.sku);
    return;
  }

  if (parsed.type === 'wh_select') {
    const sku = getLastSku_(actorKey);
    if (!sku) {
      await safeReplyText_(ev, '請先用「查 xxx」或「編號 a564」選定商品，再選倉庫');
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
      await safeReplyText_(ev, '指令格式：出3箱2件 / 出3箱 / 出2件（出1/出1個/出1散 都視為件）');
      return;
    }

    const sku = getLastSku_(actorKey);
    if (!sku) {
      await safeReplyText_(ev, '請先用「查 xxx」或「編號 a564」選定「有庫存」商品後再出庫');
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

    const atIso = new Date().toISOString();

    try {
      await rpcFifoOutAndLog_({
        groupCode: GROUP_CODE,
        sku,
        warehouseCode: chosenWhCode,
        outBox,
        outPiece,
        atIso,
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

    const gasPayload = {
      type: 'line_outbound',
      group_code: String(GROUP_CODE || '').trim().toLowerCase(),
      product_sku: sku,
      warehouse_code: chosenWhCode,
      warehouse_name: snapAfter.label,
      out_box: outBox,
      out_piece: outPiece,
      stock_box: Number(snapAfter.box || 0),
      stock_piece: Number(snapAfter.piece || 0),
      at: atIso,
      tpe_time: tpeNowISO_(),
      biz_date_0500: getBizDate0500TPE_(),
      bot_ver: BOT_VER,
      db_host: SUPA_HOST,
      source: 'LINE_OUTBOUND',
    };
    fireAndForgetGas_(gasPayload);
  }
}

/* =========================
 * postback handlers
 * ========================= */
async function handlePostback_(ev) {
  const actorKey = getActorKey_(ev);
  const createdBy = getCreatedBy_(ev);

  const pb = parsePostback(ev?.postback?.data);
  if (!pb) return;

  if (pb.type === 'wh_select_postback') {
    const sku = pb.sku || getLastSku_(actorKey);
    if (!sku) {
      await safeReplyText_(ev, '請先用「查 xxx」或「編號 a564」選定商品，再選倉庫');
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
    const sku = pb.sku || getLastSku_(actorKey);
    if (!sku) {
      await safeReplyText_(ev, '請先用「查 xxx」或「編號 a564」選定商品後再出庫');
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

    const atIso = new Date().toISOString();

    try {
      await rpcFifoOutAndLog_({
        groupCode: GROUP_CODE,
        sku,
        warehouseCode: whCode,
        outBox,
        outPiece,
        atIso,
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

    const gasPayload = {
      type: 'line_outbound',
      group_code: String(GROUP_CODE || '').trim().toLowerCase(),
      product_sku: sku,
      warehouse_code: whCode,
      warehouse_name: snapAfter.label,
      out_box: outBox,
      out_piece: outPiece,
      stock_box: Number(snapAfter.box || 0),
      stock_piece: Number(snapAfter.piece || 0),
      at: atIso,
      tpe_time: tpeNowISO_(),
      biz_date_0500: getBizDate0500TPE_(),
      bot_ver: BOT_VER,
      db_host: SUPA_HOST,
      source: 'LINE_OUTBOUND',
    };
    fireAndForgetGas_(gasPayload);
  }
}

/* =========================
 * event handling
 * ========================= */
async function handleEvent_(ev) {
  // ✅ 先做 DB 去重（跨重啟/多 instance）
  const eventId = ev.webhookEventId || ev?.deliveryContext?.eventId || '';
  const ok = await acquireEventDedup_(eventId, ev);
  if (!ok) return;

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
  // 鐵律：立刻回 200，避免 LINE 重送
  res.sendStatus(200);

  const events = req.body?.events ?? [];
  for (const ev of events) {
    console.log('[LINE EVENT]', JSON.stringify(ev));
    void handleEvent_(ev);
  }
});

app.listen(PORT, () => {
  console.log(
    `LINE Bot server running on port ${PORT} ver=${BOT_VER} db_host=${SUPA_HOST} gas=${getGasCallUrl_() ? 'on' : 'off'}`,
  );
});
