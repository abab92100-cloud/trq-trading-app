// @ts-nocheck
import { useEffect, useRef, useState } from 'react';
import {
  S, subscribe, initEngine, startBot, pauseBot, stopBot, newCycle,
  saveKeys, clearKeys, saveCfg, equity, desk, pulseAge, fmtPx, fmtUsd, fmtTime,
  listContracts, effLevels, scanRadar, detectRemoteServer,
} from './engine';
import { streamState } from './ws';
import Chart from './Chart';

function useEngine() {
  const [, setN] = useState(0);
  useEffect(() => {
    // جسّ سيرفر Termux أولًا: إن وُجد يعمل هو بالمحرك ونصبح واجهة — بلا جسّ لا يتغير شيء
    (async () => { await detectRemoteServer().catch(() => false); initEngine(); })();
    return subscribe(() => setN(n => n + 1));
  }, []);
}

/* ---------- أيقونات ---------- */
const I = {
  home: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 10.5 12 3l9 7.5V21h-6v-6h-6v6H3z"/></svg>,
  pos: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M4 17V7m5 10V4m5 13v-7m5 7V9"/></svg>,
  jrnl: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M4 6h16M4 12h16M4 18h10"/></svg>,
  set: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.9l.1.1a2 2 0 1 1-2.9 2.9l-.1-.1a1.7 1.7 0 0 0-1.9-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.2a1.7 1.7 0 0 0-1-1.5 1.7 1.7 0 0 0-1.9.3l-.1.1a2 2 0 1 1-2.9-2.9l.1-.1a1.7 1.7 0 0 0 .3-1.9 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.2a1.7 1.7 0 0 0 1.5-1 1.7 1.7 0 0 0-.3-1.9l-.1-.1a2 2 0 1 1 2.9-2.9l.1.1a1.7 1.7 0 0 0 1.9.3h.1a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.2a1.7 1.7 0 0 0 1 1.5h.1a1.7 1.7 0 0 0 1.9-.3l.1-.1a2 2 0 1 1 2.9 2.9l-.1.1a1.7 1.7 0 0 0 .3-1.9 1.7 1.7 0 0 0-1.5 1h.2a2 2 0 1 1 0 4h-.2a1.7 1.7 0 0 0-1.5-1z"/></svg>,
  play: <svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>,
  pause: <svg viewBox="0 0 24 24" fill="currentColor"><path d="M6 5h4v14H6zM14 5h4v14h-4z"/></svg>,
  stop: <svg viewBox="0 0 24 24" fill="currentColor"><path d="M6 6h12v12H6z"/></svg>,
  cycle: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.4"><path d="M3 12a9 9 0 1 0 3-6.7M3 4v5h5"/></svg>,
  radar: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="4.5"/><circle cx="12" cy="12" r="1" fill="currentColor"/><path d="M12 12 18 6"/></svg>,
};

/* عرض الرمز بدون لاحقة M الخاصة بعقود KuCoin — SHIBUSDTM ← SHIB/USDT */
const dispSym = s => String(s || '').replace(/USDTM$/i, '/USDT').replace(/^XBT/i, 'BTC');

/* ---------- بحث أزواج المنصة ---------- */
function SymbolPicker({ value, onPick }) {
  const [query, setQuery] = useState('');
  const [all, setAll] = useState([]);
  const [open, setOpen] = useState(false);
  useEffect(() => { listContracts().then(setAll).catch(() => {}); }, []);
  const q = query.trim().toUpperCase();
  const matches = q
    ? all.filter(c => c.symbol.includes(q) || c.base.includes(q)).slice(0, 30)
    : all.slice(0, 12);
  return (
    <div style={{ position: 'relative' }}>
      <input className="field" value={open ? query : dispSym(value)}
        placeholder="ابحث باسم العملة… مثل BTC أو PEPE"
        onFocus={() => { setOpen(true); setQuery(''); }}
        onChange={e => { setQuery(e.target.value); setOpen(true); }} />
      {open && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 40, marginTop: 4,
          background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 10,
          maxHeight: 220, overflowY: 'auto', direction: 'ltr',
        }}>
          {matches.length ? matches.map(c => (
            <button key={c.symbol} onClick={() => { onPick(c.symbol); setOpen(false); setQuery(''); }}
              style={{
                display: 'flex', justifyContent: 'space-between', width: '100%', padding: '9px 12px',
                background: 'none', border: 'none', borderBottom: '1px solid var(--border)',
                color: c.symbol === value ? 'var(--accent)' : 'var(--fg)',
                fontSize: 12, fontWeight: 700, cursor: 'pointer', fontFamily: 'ui-monospace,monospace',
              }}>
              <span>{dispSym(c.symbol)}</span>
              <span style={{ color: 'var(--muted)', fontSize: 10 }}>{c.base}/USDT</span>
            </button>
          )) : <div className="subtle" style={{ textAlign: 'center', padding: 12 }}>لا نتائج — جرّب اسمًا آخر</div>}
          <button onClick={() => setOpen(false)}
            style={{ width: '100%', padding: 8, background: 'none', border: 'none', color: 'var(--muted)', fontSize: 11, cursor: 'pointer' }}>
            إغلاق
          </button>
        </div>
      )}
    </div>
  );
}

const QUICK = ['XBTUSDTM','ETHUSDTM','SOLUSDTM','XRPUSDTM','DOGEUSDTM','PEPEUSDTM'];

/* ---------- حالة القناة ---------- */
function WsBar() {
  const st = streamState;
  const silentMs = st.lastMsgAt ? Date.now() - st.lastMsgAt : null;
  const alive = st.connected && silentMs != null && silentMs < 5000;
  return (
    <div className="wsbar">
      <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <i className={'wsdot ' + (alive ? 'on' : st.connected ? 'on' : 'off')} />
        {st.connected ? 'قناة KuCoin متصلة — تدفق مستمر' : st.connecting ? 'جارٍ الاتصال بالقناة…' : 'القناة مقطوعة — إعادة اتصال تلقائية'}
      </span>
      <span className="mono">
        {silentMs != null && silentMs < 1000 ? silentMs + ' مللي ث' : pulseAge()}
        {st.reconnects > 0 ? ' · إعادات ' + st.reconnects : ''}
      </span>
    </div>
  );
}

/* ---------- الرئيسية ---------- */
function HomeScreen() {
  const d = desk();
  const first = S.priceTrail[0];
  const chg = first > 0 && S.lastPrice ? ((S.lastPrice - first) / first) * 100 : 0;
  const net = S.position ? S.position.unrealized : 0; // صافي الصفقة الحالية فقط — المجني له مربعه الخاص
  const live = S.status === 'running' && S.lastTickAt && Date.now() - S.lastTickAt < 15000;
  const liqDangerNow = S.liqPrice && S.lastPrice &&
    (S.position && S.position.side === 'long' ? S.lastPrice <= S.liqPrice * 1.05 : S.lastPrice >= S.liqPrice * 0.95);
  return (
    <section>
      <div className="card">
        <div className="row" style={{ alignItems: 'flex-start' }}>
          <div style={{ textAlign: 'left' }}>
            <div className="subtle">{S.config.displaySymbol}/USDT · ×{S.config.leverage}</div>
            <div className="mono muted" style={{ fontSize: 11 }}>Mark <span>{fmtPx(S.markPrice)}</span></div>
          </div>
          <div style={{ textAlign: 'right' }}>
            <div className={'big ' + (S.priceTrail.length > 1 ? (S.lastPrice >= S.priceTrail[0] ? 'up' : 'dn') : '')}>
              {S.lastPrice ? fmtPx(S.lastPrice) : '—'}
            </div>
            <div className={'mono ' + (chg >= 0 ? 'up' : 'dn')} style={{ fontSize: 12 }}>
              {(chg >= 0 ? '+' : '') + chg.toFixed(2) + '%'}
            </div>
          </div>
        </div>
        <Chart />
        <WsBar />
        <div className="subtle" style={{ textAlign: 'center', marginTop: 4 }}>
          {'آخر نبض: ' + pulseAge() + ' · نبضة #' + S.heartbeat}
        </div>
      </div>

      <div className="card">
        <div className="row">
          <span className={'mono ' + (S.biasScore >= 0 ? 'up' : 'dn')} style={{ fontSize: 12 }}>{fmtUsd(S.biasScore, 1)}</span>
          <b>{(S.regime ? S.regime.label : 'يُقرأ السوق…') + ' → ' + (S.config.direction === 'short' ? 'شورت' : 'لونغ')}</b>
        </div>
        <div className="subtle" style={{ marginTop: 2 }}>{(S.biasReasons || []).join(' · ')}</div>
      </div>

      <div className="grid4">
        <button className="btn acc" onClick={startBot}>{I.play}تشغيل</button>
        <button className="btn" onClick={pauseBot}>{I.pause}مؤقت</button>
        <button className="btn dgr" onClick={() => stopBot()}>{I.stop}إيقاف</button>
        <button className="btn" onClick={() => { if (window.confirm('دورة جديدة؟ يُغلق أي مركز مفتوح وتُمسح سجلات الصفقات والسيرفر — تبقى فقط ذاكرة التعلم.')) newCycle(); }}>{I.cycle}دورة</button>
      </div>
      <div className="subtle" style={{ textAlign: 'center', margin: '8px 0 12px' }}>
        {S.status === 'paused' ? 'مؤقت: لا صفقات جديدة — الجني مستمر'
          : live ? 'البوت يتداول الآن على بيانات لحظية'
          : S.status === 'running' ? 'البوت شغّال — بانتظار النبض'
          : 'اضغط تشغيل لبدء البوت'}
      </div>

      <div className="grid3">
        <div className="chip"><small>الرافعة</small><b>{'×' + S.config.leverage}</b></div>
        <div className="chip"><small>المستويات</small><b>{effLevels()}{effLevels() !== S.config.levels ? <span style={{ opacity: .55, fontSize: 10 }}> /{S.config.levels}</span> : null}</b></div>
        <div className="chip"><small>رصيد الدورة</small><b>{'$' + S.config.cycleBalance}</b></div>
        <div className="chip"><small>خطوة %</small><b>{S.config.gridStepPct}</b></div>
        <div className="chip"><small>صيد %</small><b>{S.config.huntPct}</b></div>
        <div className="chip"><small>الاتجاه</small><b>{S.config.directionMode === 'auto' ? 'تلقائي' : (S.config.direction === 'short' ? 'شورت' : 'لونغ')}</b></div>
      </div>

      <div className="card" style={{ marginTop: 12 }}>
        <div className="grid3" style={{ textAlign: 'center' }}>
          <div><small className="subtle">حقوق الدورة</small><div className="mono up">{'$' + equity().toFixed(2)}</div></div>
          <div><small className="subtle">صافي الصفقة الحالية</small><div className={'mono ' + (net > 0 ? 'up' : net < 0 ? 'dn' : '')}>{fmtUsd(net) + ' $'}</div></div>
          <div><small className="subtle">الربح المجني</small><div className={'mono ' + ((S.cycleHarvested || 0) > 0 ? 'up' : (S.cycleHarvested || 0) < 0 ? 'dn' : '')}>{fmtUsd(S.cycleHarvested || 0) + ' $'}</div></div>
          <div><small className="subtle">التصفية</small><div className={'mono ' + (liqDangerNow ? 'dn' : '')}>{S.liqPrice ? fmtPx(S.liqPrice) : '—'}</div></div>
        </div>
        <div className="sep"></div>
        <div className="grid3" style={{ textAlign: 'center' }}>
          <div><small className="subtle">ناجح</small><div className="mono up">{d.win.length}</div></div>
          <div><small className="subtle">خاسر</small><div className="mono dn">{d.lose.length}</div></div>
          <div><small className="subtle">معلّق</small><div className="mono" style={{ color: 'var(--warn)' }}>{d.open.length}</div></div>
        </div>
        <div className="subtle" style={{ textAlign: 'center', marginTop: 8 }}>
          {'رسوم: $' + S.feesPaid.toFixed(2) + ' · مفتوح: ' + d.open.length}
        </div>
      </div>
    </section>
  );
}

/* ---------- رادار الفرص ---------- */
function RadarScreen() {
  const r = S.radar, list = r ? r.list : [];
  const cur = list.find(l => l.symbol === S.config.symbol);
  const [busy, setBusy] = useState(false);
  const scan = async () => { setBusy(true); try { await scanRadar(); } finally { setBusy(false); } };
  return (
    <section>
      <div className="card">
        <div className="row">
          <b>📡 رادار الفرص</b>
          <button className="btn" style={{ padding: '4px 12px', fontSize: 12 }} onClick={scan} disabled={busy}>
            {busy ? 'يمسح…' : 'مسح الآن'}
          </button>
        </div>
        <div className="subtle" style={{ marginTop: 4 }}>
          {r ? 'آخر مسح: ' + fmtTime(r.at) + ' — يتحدث تلقائيًا كل 3 دقائق' : 'لم يُمسح بعد — اضغط «مسح الآن»'}
        </div>
        {cur ? (
          <div className="row" style={{ marginTop: 8 }}>
            <span>عملتك الحالية <b>{cur.disp}</b></span>
            <span className="badge g">{cur.grade} {cur.score}</span>
          </div>
        ) : null}
      </div>
      {list.slice(0, 10).map((l, i) => (
        <div className="card" key={l.symbol} style={{ padding: '10px 12px' }}>
          <div className="row">
            <b>{(i + 1) + '. ' + l.disp}{l.symbol === S.config.symbol ? ' ✓' : ''}</b>
            <span className={'badge ' + (l.grade === '🟢' ? 'g' : l.grade === '🟡' ? 'b' : '')}>{l.grade + ' ' + l.score}</span>
          </div>
          <div className="subtle" style={{ marginTop: 4 }}>
            {l.state + ' · تذبذب ' + l.vol + '% · 24س ' + (l.chg >= 0 ? '+' : '') + l.chg + '%' + (l.why.length ? ' · ' + l.why.join('، ') : '')}
          </div>
          {l.symbol !== S.config.symbol && l.score >= 65 ? (
            <button className="btn acc" style={{ marginTop: 8, width: '100%', padding: '6px' }}
              onClick={() => {
                if (S.position) { window.alert('أغلق المركز الحالي أولًا ثم بدّل'); return; }
                if (window.confirm('التبديل إلى ' + l.disp + '؟ تُلغى حالة الزوج الحالي وتُدرس العملة الجديدة قبل الدخول.'))
                  saveCfg({ symbol: l.symbol, leverage: S.config.leverage, levels: S.config.levels, gridStepPct: S.config.gridStepPct, huntPct: S.config.huntPct, cycleBalance: S.config.cycleBalance, directionMode: S.config.directionMode, mode: S.config.mode, sound: S.sound, soundTone: S.soundTone });
              }}>
              تبديل إلى {l.disp}
            </button>
          ) : null}
        </div>
      ))}
      {!list.length ? <div className="card subtle" style={{ textAlign: 'center' }}>الرادار يقرأ كل عقود المنصة ويرتّبها: الأعلى درجة = أنسب بيئة للشبكة الآن</div> : null}
    </section>
  );
}

/* ---------- المراكز ---------- */
function OrderRow({ l, tp }) {
  return (
    <div className="orow">
      <span className={'badge ' + (tp ? 'b' : 'g')}>{tp ? 'جني' : l.status === 'open' ? 'مفتوح' : 'مسلّح'}</span>
      <span className={l.side === 'sell' ? 'dn' : 'up'}>
        {(l.side === 'sell' ? 'SELL' : 'BUY') + ' ' + fmtPx(l.price) + ' ×' + l.qty}
      </span>
    </div>
  );
}
function PosScreen() {
  const pos = S.position;
  const armed = S.grid.filter(g => !g.reduceOnly && g.status === 'armed');
  const open = S.grid.filter(g => !g.reduceOnly && g.status === 'open');
  const tps = S.grid.filter(g => g.reduceOnly && (g.status === 'armed' || g.status === 'open'));
  const empty = <div className="subtle" style={{ textAlign: 'center', padding: 14 }}>لا أوامر</div>;
  return (
    <section>
      <div className="card">
        {pos ? (
          <div>
            <div className="row">
              <span className={'badge ' + (pos.side === 'short' ? 's' : 'b')}>
                {(pos.side === 'short' ? 'SHORT' : 'LONG') + ' ×' + pos.leverage}
              </span>
              <b>المركز الصافي</b>
            </div>
            <div className="grid3" style={{ textAlign: 'center', marginTop: 10 }}>
              <div><small className="subtle">الحجم</small><div className="mono">{pos.size.toFixed(0)}</div></div>
              <div><small className="subtle">المتوسط</small><div className="mono">{fmtPx(pos.entry)}</div></div>
              <div><small className="subtle">العائم</small><div className={'mono ' + (pos.unrealized >= 0 ? 'up' : 'dn')}>{fmtUsd(pos.unrealized)}</div></div>
            </div>
            <div className="subtle" style={{ textAlign: 'center', marginTop: 8 }}>
              {'التصفية: ' + (pos.liquidation ? fmtPx(pos.liquidation) : '—')}
            </div>
          </div>
        ) : (
          <div className="subtle" style={{ textAlign: 'center', padding: 10 }}>لا مركز صافٍ — البوت ينتظر فرصة الدخول</div>
        )}
      </div>
      <div className="card">
        <h3>الشبكة المسلّحة <span className="badge g">{armed.length}</span></h3>
        <div className="olist">{armed.length ? armed.map(l => <OrderRow key={l.id} l={l} />) : empty}</div>
      </div>
      <div className="card">
        <h3>أوامر مفتوحة <span className="badge g">{open.length}</span></h3>
        <div className="olist">{open.length ? open.map(l => <OrderRow key={l.id} l={l} />) : empty}</div>
      </div>
      <div className="card">
        <h3>أوامر جني <span className="badge b">{tps.length}</span></h3>
        <div className="olist">{tps.length ? tps.map(l => <OrderRow key={l.id} l={l} tp />) : empty}</div>
      </div>
    </section>
  );
}

/* ---------- السجل ---------- */
function exportCsv() {
  const rows = [...S.journal, ...(S.history || [])];
  const head = 'time,symbol,side,source,entry,exit,qty,pnl,fees,status\n';
  const body = rows.map(j => [
    new Date(j.closedAt || j.openedAt).toISOString(), S.config.symbol, j.side, j.source,
    j.entry, j.exit || '', j.qty, j.pnl != null ? j.pnl : '', j.fees, j.status,
  ].join(',')).join('\n');
  const blob = new Blob(['﻿' + head + body], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'trq-trades.csv';
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 3000);
}

function JournalScreen() {
  const rows = [...S.journal, ...(S.history || [])];
  return (
    <section>
      <div className="card">
        <h3>سجل الصفقات
          <button onClick={exportCsv}
            style={{ float: 'left', background: 'var(--surface2)', border: '1px solid var(--border)', color: 'var(--accent)', borderRadius: 8, padding: '2px 10px', fontSize: 10, cursor: 'pointer' }}>
            تصدير CSV
          </button>
        </h3>
        <div style={{ maxHeight: 320, overflowY: 'auto', WebkitOverflowScrolling: 'touch' }}>
          {rows.length ? rows.map(j => {
            const pnl = j.status === 'closed' ? j.pnl : (S.lastPrice ?
              ((j.side === 'short' ? 1 : -1) * (j.entry - S.lastPrice) * (S.multiplier || 1) * j.qty - j.fees) : 0);
            return (
              <div className="log" key={j.id}>
                <span className={(pnl > 0 ? 'up' : pnl < 0 ? 'dn' : 'muted') + ' mono'}>{fmtUsd(pnl)}</span>
                <span>
                  {j.source + ' · ' + (j.side === 'short' ? 'شورت' : 'لونغ') + ' · ' +
                    (j.status === 'open' ? 'مفتوحة' : 'مغلقة') + (j.mergedOrders > 1 ? ' · متوسط ×' + j.mergedOrders : '') + ' '}
                  <span className="subtle">{fmtTime(j.closedAt || j.openedAt)}</span>
                </span>
              </div>
            );
          }) : <div className="subtle" style={{ textAlign: 'center', padding: 14 }}>لا صفقات بعد</div>}
        </div>
      </div>
      <div className="card">
        <h3>سجل السيرفر</h3>
        <div style={{ maxHeight: 320, overflowY: 'auto', WebkitOverflowScrolling: 'touch' }}>
          {S.logs.length ? S.logs.map(l => (
            <div className="log" key={l.id}>
              <span className={l.kind === 'fill' ? 'lfill' : l.kind === 'error' ? 'lerr' : l.kind === 'recover' ? 'lrec' : 'muted'}>{l.text}</span>
              <span className="tm">{fmtTime(l.at)}</span>
            </div>
          )) : <div className="subtle" style={{ textAlign: 'center', padding: 10 }}>السجل فارغ</div>}
        </div>
      </div>
    </section>
  );
}

/* ---------- الإعدادات ---------- */
function SettingsScreen() {
  const c = S.config;
  const [apiKey, setApiKey] = useState(S.keys ? S.keys.apiKey : '');
  const [apiSecret, setApiSecret] = useState(S.keys ? S.keys.apiSecret : '');
  const [apiPass, setApiPass] = useState(S.keys ? S.keys.passphrase : '');
  const [symbol, setSymbol] = useState(c.symbol);
  const [lev, setLev] = useState(c.leverage);
  const [levels, setLevels] = useState(c.levels);
  const [step, setStep] = useState(c.gridStepPct);
  const [hunt, setHunt] = useState(c.huntPct);
  const [bal, setBal] = useState(c.cycleBalance);
  const [dirMode, setDirMode] = useState(c.directionMode);
  const [mode, setMode] = useState(c.mode);
  const [tone, setTone] = useState(S.sound ? (S.soundTone || 'soft') : 'mute');
  return (
    <section>
      <div className="card">
        <h3>مفاتيح KuCoin</h3>
        <p className="note">⚠️ أنشئ مفتاحًا بصلاحية «تداول فقط» بدون سحب. المفاتيح تُحفظ على جهازك فقط (localStorage) ولا تُرسل لأي طرف ثالث.</p>
        <label className="lb">API Key</label>
        <input className="field" value={apiKey} onChange={e => setApiKey(e.target.value)} placeholder="أدخل المفتاح" autoComplete="off" />
        <label className="lb">API Secret</label>
        <input className="field" type="password" value={apiSecret} onChange={e => setApiSecret(e.target.value)} placeholder="••••••" />
        <label className="lb">Passphrase</label>
        <input className="field" type="password" value={apiPass} onChange={e => setApiPass(e.target.value)} placeholder="••••••" />
        <div className="grid2" style={{ marginTop: 12 }}>
          <button className="btn acc" onClick={() => saveKeys({ apiKey: apiKey.trim(), apiSecret: apiSecret.trim(), passphrase: apiPass.trim() })}>حفظ المفاتيح</button>
          <button className="btn dgr" onClick={() => { clearKeys(); setApiKey(''); setApiSecret(''); setApiPass(''); }}>حذف المفاتيح</button>
        </div>
        <div className="subtle" style={{ textAlign: 'center', marginTop: 8 }}>
          {S.keys
            ? (S.linkOk ? '✅ مربوط بالمنصة فعليًا ••' + S.keys.apiKey.slice(-4) : '⏳ محفوظ — جارٍ التحقق من المنصة…')
            : 'غير مربوط'}
        </div>
        {S.keys && S.exEquity != null && (
          <div style={{ textAlign: 'center', marginTop: 6, fontWeight: 800, color: 'var(--accent)', fontSize: 15 }}>
            الرصيد الكلي في المنصة: ${S.exEquity.toFixed(2)}
          </div>
        )}
        {S.keys && !S.linkOk && S.exEquity == null && (
          <div className="subtle" style={{ textAlign: 'center', marginTop: 4, color: '#f43f5e', fontSize: 10 }}>
            تعذّر الوصول للمنصة — تحقق من المفاتيح والصلاحيات، يُعاد المحاولة تلقائيًا كل 20 ثانية
          </div>
        )}
        {S.permDenied && (
          <div className="note" style={{ border: '1px solid rgba(244,63,94,.5)', borderRadius: 10, padding: 10, marginTop: 8, color: '#f43f5e', background: 'rgba(244,63,94,.08)' }}>
            ⛔ مفتاحك يقرأ الرصيد لكنه <b>بلا صلاحية تداول</b> — لهذا رفضت المنصة كل الأوامر. الحل: في KuCoin ← إدارة API ← تعديل المفتاح ← فعّل صلاحية «التداول» (Trade) للعقود الآجلة ← ثم اضغط «حفظ المفاتيح» هنا مجددًا.
          </div>
        )}
        <div className="sep"></div>
        <p className="note">✅ الاتصال يمر الآن عبر بروكسي محلي مدمج — لا حاجة لبروكسي CORS خارجي. البيانات اللحظية تصل عبر قناة WebSocket مستمرة.</p>
      </div>

      <div className="card">
        <h3>معاملات البوت</h3>
        <label className="lb">الزوج — ابحث باسم أي عملة في المنصة</label>
        <SymbolPicker value={symbol} onPick={setSymbol} />
        <div className="symrow">
          {QUICK.map(s => (
            <button key={s} className={symbol === s ? 'on' : ''} onClick={() => setSymbol(s)}>{s.replace('USDTM', '').replace(/^XBT$/, 'BTC')}</button>
          ))}
        </div>
        <div className="grid2">
          <div><label className="lb">الرافعة</label><input className="field" inputMode="numeric" value={lev} onChange={e => setLev(e.target.value)} /></div>
          <div><label className="lb">المستويات</label><input className="field" inputMode="numeric" value={levels} onChange={e => setLevels(e.target.value)} /></div>
          <div><label className="lb">خطوة الشبكة %</label><input className="field" inputMode="decimal" value={step} onChange={e => setStep(e.target.value)} /></div>
          <div><label className="lb">صيد %</label><input className="field" inputMode="decimal" value={hunt} onChange={e => setHunt(e.target.value)} /></div>
          <div><label className="lb">رصيد الدورة $</label><input className="field" inputMode="decimal" value={bal} onChange={e => setBal(e.target.value)} /></div>
          <div>
            <label className="lb">الاتجاه</label>
            <select className="field" value={dirMode} onChange={e => setDirMode(e.target.value)}>
              <option value="auto">تلقائي — دراسة السوق</option>
              <option value="short">شورت ثابت</option>
              <option value="long">لونغ ثابت</option>
            </select>
          </div>
        </div>
        <label className="lb">الوضع</label>
        <select className="field" value={mode} onChange={e => setMode(e.target.value)}>
          <option value="paper">ورقي — محاكاة بدون أوامر حقيقية</option>
          <option value="live">حقيقي — أوامر على KuCoin</option>
        </select>
        <label className="lb">صوت التنبيه عند التعبئة</label>
        <div className="symrow">
          {[{ k: 'soft', l: 'هادئ' }, { k: 'bell', l: 'جرس' }, { k: 'alarm', l: 'تنبيه قوي' }, { k: 'mute', l: 'صامت' }].map(t => (
            <button key={t.k} className={tone === t.k ? 'on' : ''} onClick={() => setTone(t.k)}>{t.l}</button>
          ))}
        </div>
        <button className="btn acc" style={{ marginTop: 12, width: '100%' }}
          onClick={() => saveCfg({ symbol, leverage: lev, levels, gridStepPct: step, huntPct: hunt, cycleBalance: bal, directionMode: dirMode, mode, sound: tone !== 'mute', soundTone: tone === 'mute' ? 'soft' : tone })}>
          حفظ المعاملات
        </button>
      </div>
    </section>
  );
}

/* ---------- الجذر ---------- */
export default function App() {
  useEngine();
  const [tab, setTab] = useState('home');
  const toast = S.toastMsg && Date.now() - S.toastMsg.at < 2600 ? S.toastMsg.text : null;
  return (
    <div>
      <header>
        <div className="logo">TRQ</div>
        <div className="t"><b>TRQ TRADING</b><span>FUTURES BOT</span></div>
        <span className={'pill ' + (streamState.connected ? 'on' : 'dn-pill')} title="صحة قناة السعر اللحظية">
          <i></i>{streamState.connected ? 'متصل' : 'مقطوع'} · {pulseAge()}
        </span>
        {window.__TRQ_REMOTE && (
          <span className="pill on" title="المحرك يعمل في سيرفر Termux على هذا الجوال — بلا نوم ولا قتل خلفية">سيرفر</span>
        )}
        <span className={'pill ' + (S.config.mode === 'live' ? 'live' : '')}>{S.config.mode === 'live' ? 'LIVE' : 'ورقي'}</span>
        <span className={'pill' + (S.status === 'running' ? ' on' : S.status === 'paused' ? ' warn' : '')}>
          <i></i><em style={{ fontStyle: 'normal' }}>
            {S.status === 'running' ? 'تشغيل' : S.status === 'paused' ? 'مؤقت' : S.status === 'stopped' ? 'موقوف' : 'متوقف'}
          </em>
        </span>
      </header>
      <main>
        {tab === 'home' && <HomeScreen />}
        {tab === 'radar' && <RadarScreen />}
        {tab === 'pos' && <PosScreen />}
        {tab === 'jrnl' && <JournalScreen />}
        {tab === 'set' && <SettingsScreen />}
      </main>
      <nav className="tabbar">
        <button data-tab="home" className={tab === 'home' ? 'on' : ''} onClick={() => setTab('home')}>{I.home}الرئيسية</button>
        <button data-tab="radar" className={tab === 'radar' ? 'on' : ''} onClick={() => setTab('radar')}>{I.radar}الرادار</button>
        <button data-tab="pos" className={tab === 'pos' ? 'on' : ''} onClick={() => setTab('pos')}>{I.pos}المراكز</button>
        <button data-tab="jrnl" className={tab === 'jrnl' ? 'on' : ''} onClick={() => setTab('jrnl')}>{I.jrnl}السجل</button>
        <button data-tab="set" className={tab === 'set' ? 'on' : ''} onClick={() => setTab('set')}>{I.set}إعدادات</button>
      </nav>
      <div className={'toast' + (toast ? ' show' : '')}>{toast || ''}</div>
    </div>
  );
}
