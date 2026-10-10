// @ts-nocheck
/* ================================================================
   TRQ Trading — سيرفر الجوال (Termux)
   العقل الدائم للبوت: نفس محرك التطبيق يعمل هنا في Node بلا نوم
   ولا خنق خلفية ولا قتل بطارية. التطبيق يكتشفه تلقائيًا ويتحول واجهة.
   التشغيل:  node trq-server.mjs
   ================================================================ */
import './server-shims'; // أولًا دائمًا: يثبّت البيئة قبل تقييم المحرك
import {
  S, initEngine, startBot, pauseBot, stopBot, newCycle,
  saveCfg, saveKeys, clearKeys, scanRadar,
} from './engine';
import { createServer } from 'node:http';

const PORT = 8787;

function send(res, code, obj) {
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control': 'no-store',
  });
  res.end(JSON.stringify(obj));
}

const srv = createServer(async (req, res) => {
  try {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      });
      return res.end();
    }
    const u = new URL(req.url || '/', 'http://127.0.0.1');

    if (req.method === 'GET' && (u.pathname === '/' || u.pathname === '/health'))
      return send(res, 200, { ok: true, name: 'TRQ Termux Server', at: Date.now() });

    if (req.method === 'GET' && u.pathname === '/state')
      return send(res, 200, { ok: true, at: Date.now(), state: S });

    if (req.method === 'POST' && u.pathname === '/cmd') {
      let body = '';
      for await (const chunk of req) body += chunk;
      let j = {};
      try { j = JSON.parse(body || '{}'); } catch (e) {}
      const a = String(j.action || '');
      switch (a) {
        case 'start': startBot(); return send(res, 200, { ok: true });
        case 'pause': pauseBot(); return send(res, 200, { ok: true });
        case 'stop': await stopBot(); return send(res, 200, { ok: true });
        case 'newCycle': newCycle(); return send(res, 200, { ok: true });
        case 'saveCfg': saveCfg(j.cfg || {}); return send(res, 200, { ok: true });
        case 'saveKeys': {
          const ok = await saveKeys(j.keys || {});
          return send(res, 200, { ok: !!ok, error: ok ? null : 'فشل الاتصال بالمنصة — تحقق من المفاتيح' });
        }
        case 'clearKeys': clearKeys(); return send(res, 200, { ok: true });
        // إيقاف نهائي من الواجهة: إيقاف مؤكد للمراكز ثم خروج نظيف (رمز 0) —
        // سكربت التشغيل يكسر حلقة إعادة التشغيل عند الخروج النظيف فيبقى متوقفًا
        case 'shutdown': {
          send(res, 200, { ok: true });
          try { await stopBot(); } catch (e) {}
          setTimeout(() => process.exit(0), 600);
          return;
        }
        case 'scanRadar': await scanRadar(); return send(res, 200, { ok: true, radar: S.radar || null });
        default: return send(res, 400, { ok: false, error: 'إجراء غير معروف: ' + a });
      }
    }

    send(res, 404, { ok: false, error: 'not found' });
  } catch (e) {
    send(res, 500, { ok: false, error: String((e && e.message) || e) });
  }
});

srv.listen(PORT, '127.0.0.1', () => {
  console.log('[TRQ] سيرفر الجوال يعمل على http://127.0.0.1:' + PORT);
  console.log('[TRQ] افتح تطبيق TRQ — سيكتشف السيرفر تلقائيًا ويتحول لوضع الواجهة');
});

initEngine();
console.log('[TRQ] المحرك بدأ — أبقِ Termux حيًا (termux-wake-lock مفعّل)');
