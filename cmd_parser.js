// cmd_parser.js
// 出庫指令解析（只解析，不做任何扣庫）
// 支援：出3箱2件 / 出3箱 / 出2件 / 出庫3箱2件
// 箱/件不互轉，僅回傳原始數量

function normalizeText_(s) {
  if (typeof s !== 'string') return '';
  return s
    .replace(/\u3000/g, ' ')          // 全形空白
    .replace(/[，,]/g, ' ')           // 逗號
    .replace(/\s+/g, ' ')            // 多空白收斂
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

/**
 * 解析出庫指令
 * @param {string} input 使用者訊息
 * @returns {{
 *   ok: boolean,
 *   action?: 'out',
 *   boxQty?: number,
 *   pieceQty?: number,
 *   normalized?: string,
 *   error?: string
 * }}
 */
export function parseOutCommand(input) {
  const text = normalizeText_(input);
  if (!text) return { ok: false, error: 'empty' };

  // 允許前綴：出 / 出庫
  // 例：出3箱2件、出 3箱 2件、出庫3箱、出庫2件
  // 注意：只解析「整句就是出庫指令」；避免把一般聊天誤判成出庫
  const mPrefix = text.match(/^(出庫|出)\s*(.+)$/i);
  if (!mPrefix) return { ok: false, error: 'no_prefix' };

  const rest = mPrefix[2].trim();
  if (!rest) return { ok: false, error: 'no_amount' };

  // 解析 token：<數字><單位>
  // 單位：
  // - 箱：箱
  // - 件：件 / 散 / 個 / pcs / pc
  // 允許：3箱2件、3箱 2件、2件3箱（順序可顛倒，但會合併同單位）
  const tokenRe = /(\d+)\s*(箱|件|散|個|pcs|pc)\b/gi;

  let boxQty = 0;
  let pieceQty = 0;

  // 用 replace 走訪所有 token（避免 matchAll 在部分 runtime 出問題）
  let any = false;
  rest.replace(tokenRe, (_, numRaw, unitRaw) => {
    any = true;
    const n = parsePositiveInt_(numRaw);
    if (n == null) return _;
    const unit = String(unitRaw).toLowerCase();
    if (unit === '箱') boxQty += n;
    else pieceQty += n; // 件/散/個/pcs/pc 都算 piece
    return _;
  });

  // rest 中若含有非 token 的雜字（例如「出3箱2件給我」），視為不合法，避免誤判
  const restStripped = rest.replace(tokenRe, '').replace(/\s+/g, '').trim();
  if (!any) return { ok: false, error: 'no_tokens', normalized: text };
  if (restStripped.length > 0) {
    return { ok: false, error: 'has_extra_text', normalized: text };
  }

  if (boxQty <= 0 && pieceQty <= 0) {
    return { ok: false, error: 'non_positive', normalized: text };
  }

  return {
    ok: true,
    action: 'out',
    boxQty,
    pieceQty,
    normalized: `${mPrefix[1]} ${boxQty ? `${boxQty}箱` : ''}${boxQty && pieceQty ? ' ' : ''}${pieceQty ? `${pieceQty}件` : ''}`.trim(),
  };
}

/**
 * 產生給使用者看的解析回覆（先回覆解析結果，再接扣庫 RPC）
 */
export function formatOutParseReply(parseResult) {
  if (!parseResult?.ok) {
    // 你可依 error 做更精準的提示
    return '指令格式：出3箱2件 / 出3箱 / 出2件（不要加多餘文字）';
  }
  const b = parseResult.boxQty || 0;
  const p = parseResult.pieceQty || 0;

  if (b > 0 && p > 0) return `✅ 解析：出庫 ${b} 箱 + ${p} 件`;
  if (b > 0) return `✅ 解析：出庫 ${b} 箱`;
  return `✅ 解析：出庫 ${p} 件`;
}
