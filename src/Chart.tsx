// @ts-nocheck
/* ================================================================
   شارت احترافي — شموع يابانية + إطارات زمنية + مناطق:
   منطقة الدخول (عنبر) · منطقة جني الربح (خضراء) ·
   خط المتوسط · خط التصفية · السعر الحي
   ================================================================ */
import { useEffect, useRef, useState } from 'react';
import { S, fetchKlines, fmtPx } from './engine';

const TFS = [
  { label: '1د', gran: 1 },
  { label: '5د', gran: 5 },
  { label: '15د', gran: 15 },
  { label: '1س', gran: 60 },
  { label: '4س', gran: 240 },
];
const MAX_CANDLES = 90;

export default function Chart() {
  const ref = useRef(null);
  const dataRef = useRef({ key: '', candles: [], loading: false });
  const [gran, setGran] = useState(1);

  // جلب الشموع عند تغيير الزوج أو الإطار + تحديث دوري
  useEffect(() => {
    let alive = true;
    const load = () => {
      const key = S.config.symbol + ':' + gran;
      dataRef.current.loading = true;
      fetchKlines(S.config.symbol, gran).then(k => {
        if (!alive) return;
        dataRef.current = { key, candles: k.slice(-220), loading: false };
      }).catch(() => { if (alive) dataRef.current.loading = false; });
    };
    load();
    const iv = setInterval(load, 15000);
    return () => { alive = false; clearInterval(iv); };
  }, [S.config.symbol, gran]);

  useEffect(() => {
    const cv = ref.current; if (!cv) return;
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const W = cv.clientWidth, H = cv.clientHeight; if (!W) return;
    cv.width = W * dpr; cv.height = H * dpr;
    const x = cv.getContext('2d'); x.setTransform(dpr, 0, 0, dpr, 0, 0);
    x.clearRect(0, 0, W, H);
    const AXIS = 58, TOP = 6, BOT = 16;
    const plotW = W - AXIS - 8, plotH = H - TOP - BOT;

    // دمج السعر الحي في آخر شمعة
    let candles = dataRef.current.candles.slice(-MAX_CANDLES);
    const granMs = gran * 60000;
    const p = S.lastPrice;
    if (candles.length && p > 0) {
      const nowStart = Math.floor(Date.now() / granMs) * granMs;
      const last = candles[candles.length - 1];
      if (last.t === nowStart) {
        candles = candles.slice(0, -1).concat([{ ...last, c: p, h: Math.max(last.h, p), l: Math.min(last.l, p) }]);
      } else if (nowStart > last.t) {
        candles.push({ t: nowStart, o: last.c, h: Math.max(last.c, p), l: Math.min(last.c, p), c: p, v: 0 });
      }
    }
    if (candles.length < 2) {
      x.fillStyle = '#5b6a7d'; x.font = '11px Tahoma'; x.textAlign = 'center';
      x.fillText('يُحمَّل الشارت…', W / 2, H / 2); return;
    }

    // نطاق السعر — أساسه الشموع؛ تُضاف المناطق فقط ضمن نطاق منطقي حول السعر
    let lo = Infinity, hi = -Infinity;
    for (const k of candles) { if (k.l < lo) lo = k.l; if (k.h > hi) hi = k.h; }
    const refPx = p || (candles[candles.length - 1].c) || 1;
    const sane = v => v > 0 && v > refPx * 0.55 && v < refPx * 1.8; // استبعاد أوامر شاذة تسحق المحور
    const entries = S.grid.filter(g => !g.reduceOnly && (g.status === 'armed' || g.status === 'open') && sane(g.price));
    const tps = S.grid.filter(g => g.reduceOnly && (g.status === 'armed' || g.status === 'open') && sane(g.price));
    const avg = S.position && sane(S.position.entry) ? S.position.entry : null;
    let liq = S.liqPrice || (S.position && S.position.liquidation) || null;
    if (liq && !sane(liq)) liq = null;
    for (const g of entries.concat(tps)) { lo = Math.min(lo, g.price); hi = Math.max(hi, g.price); }
    if (avg) { lo = Math.min(lo, avg); hi = Math.max(hi, avg); }
    if (liq) { lo = Math.min(lo, liq); hi = Math.max(hi, liq); }
    if (p) { lo = Math.min(lo, p); hi = Math.max(hi, p); }
    const pad = (hi - lo) * 0.06 || hi * 0.002 || 1;
    lo -= pad; hi += pad;
    const py = v => TOP + (hi - v) / (hi - lo) * plotH;
    const px = i => 4 + i * (plotW - 8) / (candles.length - 1);

    // شبكة أفقية + محور الأسعار
    x.font = '9px ui-monospace,monospace'; x.textAlign = 'left';
    const ticks = 5;
    for (let i = 0; i < ticks; i++) {
      const v = hi - (hi - lo) * i / (ticks - 1);
      const y = py(v);
      x.strokeStyle = 'rgba(35,44,58,.7)'; x.lineWidth = 1;
      x.beginPath(); x.moveTo(4, y); x.lineTo(W - AXIS, y); x.stroke();
      x.fillStyle = '#5b6a7d';
      x.fillText(fmtPx(v), W - AXIS + 4, y + 3);
    }

    // منطقة الدخول (أوامر الشبكة المسلّحة/المفتوحة)
    const band = (list, color, label) => {
      if (!list.length) return;
      let bLo = Infinity, bHi = -Infinity;
      for (const g of list) { bLo = Math.min(bLo, g.price); bHi = Math.max(bHi, g.price); }
      const y1 = py(bHi), y2 = py(bLo);
      x.fillStyle = color.fill;
      x.fillRect(4, y1, plotW - 4, Math.max(2, y2 - y1));
      x.strokeStyle = color.edge; x.setLineDash([2, 3]); x.lineWidth = 1;
      x.strokeRect(4, y1, plotW - 4, Math.max(2, y2 - y1)); x.setLineDash([]);
      x.fillStyle = color.text; x.font = '9px Tahoma'; x.textAlign = 'left';
      x.fillText(label + ' ×' + list.length, 8, Math.min(y2 - 4, y1 + 11));
    };
    band(entries, { fill: 'rgba(245,158,11,.10)', edge: 'rgba(245,158,11,.45)', text: '#f59e0b' }, 'منطقة الدخول');
    band(tps, { fill: 'rgba(34,197,94,.10)', edge: 'rgba(34,197,94,.45)', text: '#22c55e' }, 'منطقة جني الربح');

    // الشموع
    const cw = Math.max(1, (plotW - 8) / candles.length * 0.62);
    for (let i = 0; i < candles.length; i++) {
      const k = candles[i], up = k.c >= k.o, col = up ? '#22c55e' : '#f43f5e';
      const cx = px(i);
      x.strokeStyle = col; x.lineWidth = 1;
      x.beginPath(); x.moveTo(cx, py(k.h)); x.lineTo(cx, py(k.l)); x.stroke();
      x.fillStyle = col;
      const yO = py(k.o), yC = py(k.c);
      x.fillRect(cx - cw / 2, Math.min(yO, yC), cw, Math.max(1, Math.abs(yC - yO)));
    }

    // خط أفقي بشارة سعر
    const hline = (v, col, dash, label) => {
      if (!v || v <= 0) return;
      const y = py(v);
      x.strokeStyle = col; x.lineWidth = 1.1; x.setLineDash(dash);
      x.beginPath(); x.moveTo(4, y); x.lineTo(W - AXIS, y); x.stroke(); x.setLineDash([]);
      x.font = '8.5px Tahoma';
      const txt = (label ? label + ' ' : '') + fmtPx(v);
      const tw = x.measureText(txt).width + 8;
      x.fillStyle = col; x.fillRect(W - AXIS - tw - 4, y - 8, tw, 15);
      x.fillStyle = '#0a0e14'; x.textAlign = 'center';
      x.fillText(txt, W - AXIS - tw / 2 - 4, y + 3);
    };
    if (liq) hline(liq, '#f43f5e', [6, 3], 'التصفية');
    if (avg) hline(avg, '#f59e0b', [1, 0], 'المتوسط');
    if (p) hline(p, p >= candles[0].o ? '#22c55e' : '#f43f5e', [4, 3], '');

    // محور الزمن — أول وآخر شمعة
    x.fillStyle = '#5b6a7d'; x.font = '9px ui-monospace,monospace';
    const ft = t => { const d = new Date(t); return String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0'); };
    x.textAlign = 'left'; x.fillText(ft(candles[0].t), 6, H - 4);
    x.textAlign = 'right'; x.fillText(ft(candles[candles.length - 1].t), W - AXIS - 4, H - 4);
  });

  return (
    <div>
      <div style={{ display: 'flex', gap: 6, marginBottom: 6, direction: 'ltr', justifyContent: 'flex-end' }}>
        {TFS.map(tf => (
          <button key={tf.gran} onClick={() => setGran(tf.gran)}
            style={{
              background: gran === tf.gran ? 'var(--accent-dim)' : 'var(--surface2)',
              border: '1px solid ' + (gran === tf.gran ? 'var(--accent)' : 'var(--border)'),
              color: gran === tf.gran ? 'var(--accent)' : 'var(--muted)',
              borderRadius: 8, padding: '3px 10px', fontSize: 10, fontWeight: 700, cursor: 'pointer',
            }}>{tf.label}</button>
        ))}
      </div>
      <canvas ref={ref} style={{ width: '100%', height: 240, display: 'block' }} />
      <div className="subtle" style={{ display: 'flex', gap: 12, justifyContent: 'center', marginTop: 4, fontSize: 9.5 }}>
        <span style={{ color: '#f59e0b' }}>▩ منطقة الدخول</span>
        <span style={{ color: '#22c55e' }}>▩ جني الربح</span>
        <span style={{ color: '#f59e0b' }}>— المتوسط</span>
        <span style={{ color: '#f43f5e' }}>┄ التصفية</span>
      </div>
    </div>
  );
}
