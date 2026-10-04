// @ts-nocheck
/* ================================================================
   محاكيات بيئة Node لسيرفر Termux — تُقيَّم قبل المحرك (ترتيب الواردات)
   · localStorage على ملف JSON في ~/.trq — الحالة تنجو من إعادة التشغيل
   · TRQ_SERVER=1 يحوّل المحرك للاتصال المباشر بـ KuCoin (لا بروكسي)
   ================================================================ */
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

process.env.TRQ_SERVER = '1';

const dir = join(homedir(), '.trq');
try { mkdirSync(dir, { recursive: true }); } catch (e) {}
const file = join(dir, 'trq-server-store.json');

let data = {};
try { if (existsSync(file)) data = JSON.parse(readFileSync(file, 'utf8') || '{}'); } catch (e) { data = {}; }

let timer = null;
function persist() {
  if (timer) return;
  timer = setTimeout(() => {
    timer = null;
    try { writeFileSync(file, JSON.stringify(data)); } catch (e) {}
  }, 300);
}

globalThis.localStorage = {
  getItem: k => (k in data ? data[k] : null),
  setItem: (k, v) => { data[k] = String(v); persist(); },
  removeItem: k => { delete data[k]; persist(); },
  clear: () => { data = {}; persist(); },
};

console.log('[TRQ] مخزن الحالة: ' + file);
