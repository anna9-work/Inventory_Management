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

/**
 * ✅ 這版改動重點
 * 1) 查庫存 / 選倉 / 出庫前不足檢查：全部改成「直接從 inventory_lots.qty_left」計算（箱對箱、散對散）
 *    - 不再依賴 get_business_day_stock 的 box/piece（避免 ledger/affect_biz_date 對齊問題造成 13箱）
 * 2) 單價：優先取 piece lot 最新 unit_cost_piece，沒有就取 box lot 最新 unit_cost_piece
 * 3) 保留：DB 去重、立刻回 200、reply 失敗改 push、GAS fire-and-forget
 * 4) 增加：只有「出庫」鎖 5 秒（同一個 actorKey）
 */
const BOT_VER = 'V2026-01-25_LIVE_STOCK_FROM_LOTS_ONLY_DEDUP_OUTLOCK5S';

const lineConfig = {
  channelSecret: LINE_CHANNEL_SECRET,
  channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN,
};
const lineClient = new Client(lineConfig);

/* =========================
 * timeouts (避免卡死)
 * ========================= */
const SUPA_TIMEOUT_MS = 8000;
const LINE_TIMEOUT_MS = 8000;
const GAS_TIMEOUT_MS = 6000;

async function fetchWithTimeout_(url, options = {}, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } finally {
    clearTimeout(t);
  }
}

const supabase = createClient(String(SUPABASE_URL).replace(/\/+$/, ''), SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
  global: {
    fetch: (url, options) => fetchWithTimeout_(url, options, SUPA_TIMEOUT_MS),
  },
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

function skuKey_(s) {
  return String(s || '').trim().toLowerCase();
}
function pickNum_(v, fb = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fb;
}
function pickInt_(v, fb = 0) {
  const n = parseInt(String(v ?? ''), 10);
  return Number.isFinite(n) ? n : fb;
}

function getToId_(ev) {
  const s = ev?.source || {};
  return s.groupId || s.roomId || s.userId || '';
}

async function lineReplyWithTimeout_(replyToken, message) {
  const p = lineClient.replyMessage(replyToken, message);
  const timeout = new Promise((_, rej) => setTimeout(() => rej(new Error('LINE reply timeout')), LINE_TIMEOUT_MS));
  return Promise.race([p, timeout]);
}

async function linePushWithTimeout_(to, message) {
  const p = lineClient.pushMessage(to, message);
  const timeout = new Promise((_, rej) => setTimeout(() => rej(new Error('LINE push timeout')), LINE_TIMEOUT_MS));
  return Promise.race([p, timeout]);
}

async function safeReplyText_(ev, text, quickReply = undefined) {
  const to = getToId_(ev);
  try {
    if (ev.replyToken) {
      await lineReplyWithTimeout_(ev.replyToken, { type: 'text', text, ...(quickReply ? { quickReply } : {}) });
      return;
    }
  } catch (e) {
    console.error('[LINE replyMessage failed]', e?.message || e);
  }

  if (!to) return;
  try {
    await linePushWithTimeout_(to, { type: 'text', text, ...(quickReply ? { quickReply } : {}) });
  } catch (e2) {
    console.error('[LINE pushMessage failed]', e2?.message || e2);
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
    const res = await fetchWithTimeout_(
      url,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      },
      GAS_TIMEOUT_MS,
    );
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
 * RPC (出庫)
 * ========================= */
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
 * ✅ 出庫鎖：只鎖出庫 5 秒
 * ========================= */
const OUT_LOCK = new Map(); // actorKey -> untilMs
const OUT_LOCK_MS = 5000;
function isOutLocked_(actorKey) {
  const now = Date.now();
  const until = OUT_LOCK.get(actorKey) || 0;
  return now < until;
}
function setOutLock_(actorKey) {
  OUT_LOCK.set(actorKey, Date.now() + OUT_LOCK_MS);
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

  const { data, error } = await supabase.from('products').select('product_sku, product_name').eq('barcode', barcode).limit(10);
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
 * ✅ products cache（避免大量 IN 查詢造成 URL 太長）
 * ========================= */
const PRODUCT_CACHE = new Map(); // sku -> { name, unitsPerBox, ts }
const PRODUCT_TTL_MS = 10 * 60 * 1000;

async function getProductInfo_(sku) {
  const s = skuKey_(sku);
  if (!s) return { sku: '', name: '', unitsPerBox: 0 };

  const cached = PRODUCT_CACHE.get(s);
  if (cached && Date.now() - cached.ts < PRODUCT_TTL_MS) return { sku: s, name: cached.name, unitsPerBox: cached.unitsPerBox };

  const { data, error } = await supabase
    .from('products')
    .select('product_sku, product_name, units_per_box')
    .eq('product_sku', s)
    .limit(1)
    .maybeSingle();

  if (error) {
    console.warn('[getProductInfo_ WARN]', error?.message || error);
    return { sku: s, name: s, unitsPerBox: 0 };
  }

  const name = String(data?.product_name || s).trim();
  const unitsPerBox = pickInt_(data?.units_per_box ?? 0, 0);

  PRODUCT_CACHE.set(s, { name, unitsPerBox, ts: Date.now() });
  return { sku: s, name, unitsPerBox };
}

/* =========================
 * ✅ LIVE STOCK FROM LOTS (核心)
 * - 箱數：sum(uom='box' qty_left)
 * - 散數：sum(uom='piece' qty_left)
 * - 金額：sum(piece qty_left*cost) + sum(box qty_left*units_per_box*cost)
 * ========================= */
async function getLatestUnitCostPieceFromLots_(sku, warehouseCode) {
  const s = skuKey_(sku);
  const wh = String(warehouseCode || '').trim().toLowerCase() || 'unspecified';
  if (!s) return null;

  // 先 piece lot
  {
    const { data, error } = await supabase
      .from('inventory_lots')
      .select('unit_cost_piece,inbound_at,qty_left,uom')
      .eq('product_sku', s)
      .eq('warehouse_code', wh)
      .eq('uom', 'piece')
      .gt('qty_left', 0)
      .order('inbound_at', { ascending: false })
      .limit(1);

    if (!error) {
      const row = Array.isArray(data) && data.length ? data[0] : null;
      const n = Number(row?.unit_cost_piece);
      if (Number.isFinite(n)) return n;
    }
  }

  // 再 box lot（box 的 unit_cost_piece 仍是「每件成本」）
  {
    const { data, error } = await supabase
      .from('inventory_lots')
      .select('unit_cost_piece,inbound_at,qty_left,uom')
      .eq('product_sku', s)
      .eq('warehouse_code', wh)
      .eq('uom', 'box')
      .gt('qty_left', 0)
      .order('inbound_at', { ascending: false })
      .limit(1);

    if (!error) {
      const row = Array.isArray(data) && data.length ? data[0] : null;
      const n = Number(row?.unit_cost_piece);
      if (Number.isFinite(n)) return n;
    }
  }

  return null;
}

async function getWarehousesStockBySkuFromLots_(sku) {
  const s = skuKey_(sku);
  if (!s) return [];

  const p = await getProductInfo_(s);
  const unitsPerBox = pickInt_(p.unitsPerBox ?? 0, 0) || 1;

  const { data, error } = await supabase
    .from('inventory_lots')
    .select('warehouse_code,uom,qty_left,unit_cost_piece')
    .eq('product_sku', s)
    .in('uom', ['box', 'piece'])
    .gt('qty_left', 0)
    .limit(5000);

  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  const map = new Map(); // wh -> { box, piece, amount }
  for (const r of rows) {
    const wh = String(r.warehouse_code || 'unspecified').trim().toLowerCase() || 'unspecified';
    const uom = String(r.uom || '').trim().toLowerCase();
    const qtyLeft = pickNum_(r.qty_left ?? 0, 0);
    const cost = pickNum_(r.unit_cost_piece ?? 0, 0);

    if (!map.has(wh)) map.set(wh, { box: 0, piece: 0, amount: 0 });
    const acc = map.get(wh);

    if (uom === 'box') {
      acc.box += qtyLeft;
      acc.amount += qtyLeft * unitsPerBox * cost;
    } else if (uom === 'piece') {
      acc.piece += qtyLeft;
      acc.amount += qtyLeft * cost;
    }
  }

  const out = [];
  for (const [wh, acc] of map.entries()) {
    if (acc.box > 0 || acc.piece > 0) {
      out.push({
        code: wh,
        label: resolveWarehouseLabel_(wh),
        box: Number(acc.box),
        piece: Number(acc.piece),
        amount: Number(acc.amount),
      });
    }
  }
  return out;
}

async function getWarehouseSnapshotDetailsFromLots_(sku, whCode) {
  const s = skuKey_(sku);
  const code = String(whCode || '').trim().toLowerCase() || 'unspecified';

  const p = await getProductInfo_(s);
  const unitsPerBox = pickInt_(p.unitsPerBox ?? 0, 0);

  const { data, error } = await supabase
    .from('inventory_lots')
    .select('uom,qty_left,unit_cost_piece')
    .eq('product_sku', s)
    .eq('warehouse_code', code)
    .in('uom', ['box', 'piece'])
    .gt('qty_left', 0)
    .limit(5000);

  if (error) throw error;

  let box = 0;
  let piece = 0;
  let amount = 0;

  const upb = unitsPerBox > 0 ? unitsPerBox : 1;

  const rows = Array.isArray(data) ? data : [];
  for (const r of rows) {
    const uom = String(r.uom || '').trim().toLowerCase();
    const qtyLeft = pickNum_(r.qty_left ?? 0, 0);
    const cost = pickNum_(r.unit_cost_piece ?? 0, 0);
    if (uom === 'box') {
      box += qtyLeft;
      amount += qtyLeft * upb * cost;
    } else if (uom === 'piece') {
      piece += qtyLeft;
      amount += qtyLeft * cost;
    }
  }

  const unitCostPiece = await getLatestUnitCostPieceFromLots_(s, code);

  return {
    sku: s,
    name: String(p.name || s).trim(),
    unitsPerBox: unitsPerBox || 0,
    unitCostPiece,
    code,
    label: resolveWarehouseLabel_(code),
    box: Number(box),
    piece: Number(piece),
    amount: Number(amount),
  };
}

function formatSkuInfoText_(d) {
  const priceText = d.unitCostPiece === null ? '-' : String(d.unitCostPiece);
  return (
    `名稱：${d.name}\n` +
    `編號：${d.sku}\n` +
    `箱入數：${d.unitsPerBox || '-'}\n` +
    `單價：${priceText}\n` +
    `倉庫類別：${d.label}\n` +
    `庫存：${d.box}箱${d.piece}件`
  );
}

/* =========================
 * TODAY STOCK LIST cache (for 查/查詢)
 * ✅ 改為：從 lots 建「有庫存 sku 清單」
 * ========================= */
const STOCK_LIST_CACHE = new Map(); // key = `${group}::${bizDate}` -> { ts, rows }
const STOCK_LIST_TTL_MS = 3000;

function getStockCacheKey_(group, bizDate) {
  return `${String(group || '').trim().toLowerCase()}::${bizDate}`;
}

async function getTodayStockRowsFromLots_(group) {
  // group 只用作 cache key（lots 本身沒有 group_code）
  const bizDate = getBizDate0500TPE_();
  const key = getStockCacheKey_(group, bizDate);
  const cached = STOCK_LIST_CACHE.get(key);
  if (cached && Date.now() - cached.ts < STOCK_LIST_TTL_MS) return cached.rows;

  const { data, error } = await supabase
    .from('inventory_lots')
    .select('product_sku,uom,qty_left,warehouse_code')
    .in('uom', ['box', 'piece'])
    .gt('qty_left', 0)
    .limit(5000);

  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  const skuSet = new Set();

  for (const r of rows) {
    const sku = skuKey_(r.product_sku);
    if (!sku) continue;
    const qtyLeft = pickNum_(r.qty_left ?? 0, 0);
    if (qtyLeft <= 0) continue;
    skuSet.add(sku);
  }

  const skus = Array.from(skuSet);
  // 不在這裡批量查 products（避免 URL 太長）；搜尋命中後再逐筆拿名稱
  const kept = skus.map((s) => ({ sku: s, name: '' }));

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
  const rows = await getTodayStockRowsFromLots_(group);

  const out = [];
  for (const r of rows) {
    const sku = String(r.sku || '').toLowerCase();
    if (!sku) continue;

    if (sku.includes(kwLower)) {
      const p = await getProductInfo_(sku);
      out.push({ sku, name: p.name || sku });
      if (out.length >= 10) break;
      continue;
    }

    // 名稱比對：命中時才查 products（最多 10 筆）
    const p = await getProductInfo_(sku);
    const nameLower = String(p.name || '').toLowerCase();
    if (nameLower.includes(kwLower)) {
      out.push({ sku, name: p.name || sku });
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
 * handlers
 * ========================= */
async function handleSkuFlow_(ev, sku) {
  const actorKey = getActorKey_(ev);
  const s = skuKey_(sku);
  if (!s) return;

  setLastSku_(actorKey, s);

  const whList = await getWarehousesStockBySkuFromLots_(s);
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

  const detail = await getWarehouseSnapshotDetailsFromLots_(s, chosen.code);
  await safeReplyText_(ev, formatSkuInfoText_(detail));
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
      await safeReplyText_(ev, `無此商品庫存（以 lots 即時庫存為準）\n關鍵字：${parsed.keyword}`);
      return;
    }
    if (list.length === 1) {
      await handleSkuFlow_(ev, list[0].sku);
      return;
    }
    await safeReplyText_(ev, `找到以下品項（只含目前有庫存）`, buildQuickReplyForProducts_(list));
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

    const detail = await getWarehouseSnapshotDetailsFromLots_(sku, whCode);
    await safeReplyText_(ev, formatSkuInfoText_(detail));
    return;
  }

  if (parsed.type === 'change' && parsed.action === 'out') {
    const outBox = Number(parsed.box || 0);
    const outPiece = Number(parsed.piece || 0);

    if (outBox === 0 && outPiece === 0) {
      await safeReplyText_(ev, '指令格式：出3箱2件 / 出3箱 / 出2件（出1/出1個/出1散 都視為件）');
      return;
    }

    // ✅ 只有出庫鎖 5 秒
    if (isOutLocked_(actorKey)) {
      await safeReplyText_(ev, '⚠️ 出庫處理中，請稍後再試一次（5 秒內）');
      return;
    }
    setOutLock_(actorKey);

    const sku = getLastSku_(actorKey);
    if (!sku) {
      await safeReplyText_(ev, '請先用「查 xxx」或「編號 a564」選定「有庫存」商品後再出庫');
      return;
    }

    const whList = await getWarehousesStockBySkuFromLots_(sku);
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

    // ✅ 出庫前不足檢查：用 lots（箱對箱、散對散）
    const beforeDetail = await getWarehouseSnapshotDetailsFromLots_(sku, chosenWhCode);
    if (outBox > 0 && beforeDetail.box < outBox) {
      await safeReplyText_(ev, `庫存不足，無法出庫（倉別：${beforeDetail.label}）\n目前庫存：${beforeDetail.box}箱${beforeDetail.piece}件`);
      return;
    }
    if (outPiece > 0 && beforeDetail.piece < outPiece) {
      await safeReplyText_(ev, `庫存不足，無法出庫（倉別：${beforeDetail.label}）\n目前庫存：${beforeDetail.box}箱${beforeDetail.piece}件`);
      return;
    }

    const atIso = new Date().toISOString();

    try {
      await rpcFifoOutAndLog_({
        groupCode: String(GROUP_CODE || '').trim().toLowerCase(),
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

    const afterDetail = await getWarehouseSnapshotDetailsFromLots_(sku, chosenWhCode);
    await safeReplyText_(
      ev,
      `✅ 出庫成功\n編號：${sku}\n倉庫類別：${afterDetail.label}\n出庫：${outBox}箱 ${outPiece}件\n👉目前庫存：${afterDetail.box}箱${afterDetail.piece}件`,
    );

    const gasPayload = {
      type: 'line_outbound',
      group_code: String(GROUP_CODE || '').trim().toLowerCase(),
      product_sku: sku,
      warehouse_code: chosenWhCode,
      warehouse_name: afterDetail.label,
      out_box: outBox,
      out_piece: outPiece,
      stock_box: Number(afterDetail.box || 0),
      stock_piece: Number(afterDetail.piece || 0),
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

    const detail = await getWarehouseSnapshotDetailsFromLots_(sku, whCode);
    await safeReplyText_(ev, formatSkuInfoText_(detail));
    return;
  }

  if (pb.type === 'out_postback') {
    const sku = pb.sku || getLastSku_(actorKey);
    if (!sku) {
      await safeReplyText_(ev, '請先用「查 xxx」或「編號 a564」選定商品後再出庫');
      return;
    }

    // ✅ 只有出庫鎖 5 秒
    if (isOutLocked_(actorKey)) {
      await safeReplyText_(ev, '⚠️ 出庫處理中，請稍後再試一次（5 秒內）');
      return;
    }
    setOutLock_(actorKey);

    const whCode = getWarehouseCodeForLabel_(pb.wh);
    const outBox = Number(pb.box || 0);
    const outPiece = Number(pb.piece || 0);

    const beforeDetail = await getWarehouseSnapshotDetailsFromLots_(sku, whCode);
    if (outBox > 0 && beforeDetail.box < outBox) {
      await safeReplyText_(ev, `庫存不足，無法出庫（倉別：${beforeDetail.label}）\n目前庫存：${beforeDetail.box}箱${beforeDetail.piece}件`);
      return;
    }
    if (outPiece > 0 && beforeDetail.piece < outPiece) {
      await safeReplyText_(ev, `庫存不足，無法出庫（倉別：${beforeDetail.label}）\n目前庫存：${beforeDetail.box}箱${beforeDetail.piece}件`);
      return;
    }

    const atIso = new Date().toISOString();

    try {
      await rpcFifoOutAndLog_({
        groupCode: String(GROUP_CODE || '').trim().toLowerCase(),
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

    const afterDetail = await getWarehouseSnapshotDetailsFromLots_(sku, whCode);
    await safeReplyText_(
      ev,
      `✅ 出庫成功\n編號：${sku}\n倉庫類別：${afterDetail.label}\n出庫：${outBox}箱 ${outPiece}件\n👉目前庫存：${afterDetail.box}箱${afterDetail.piece}件`,
    );

    const gasPayload = {
      type: 'line_outbound',
      group_code: String(GROUP_CODE || '').trim().toLowerCase(),
      product_sku: sku,
      warehouse_code: whCode,
      warehouse_name: afterDetail.label,
      out_box: outBox,
      out_piece: outPiece,
      stock_box: Number(afterDetail.box || 0),
      stock_piece: Number(afterDetail.piece || 0),
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
 * DB dedup (跨重啟/多 instance)
 * ========================= */
async function acquireEventDedup_(eventId, ev) {
  const id = String(eventId || '').trim();
  if (!id) return true;

  const payload = {
    event_id: id,
    group_code: String(GROUP_CODE || '').trim().toLowerCase(),
    line_user_id: ev?.source?.userId || null,
    event_type: ev?.type || null,
  };

  const { error } = await supabase.from('line_event_dedup').insert(payload);

  if (!error) return true;

  if (String(error.code) === '23505') {
    console.log('[DEDUP] duplicated event_id => skip', id);
    return false;
  }

  console.warn('[DEDUP WARN] insert failed, allow continue:', error?.message || error);
  return true;
}

/* =========================
 * event handling
 * ========================= */
async function handleEvent_(ev) {
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
