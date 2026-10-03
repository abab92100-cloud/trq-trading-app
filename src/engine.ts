// @ts-nocheck
/* ================================================================
   TRQ Trading — بوت تداول عقود KuCoin الآجلة
   النسخة المتصلة: بيانات لحظية عبر WebSocket (دفع مستمر) +
   REST عبر بروكسي محلي (/kucoin) لتجاوز CORS.
   منطق المحرك مطابق للنسخة الأصلية: شبكة + صيد + جني +
   انعكاس بشبكتين واسعتين + هروب من التصفية.
   ================================================================ */
import { startStream, setStreamSymbol, streamState } from './ws';

const TICK_MS = 1000; // القرارات كل ثانية على أسعار لحظية من القناة
const MIN_NET_USD = 0.05, MAX_HUNT_OPEN = 3, HUNT_COOLDOWN = 15000,
      ADD_COOLDOWN = 20000, SCOUT_MS = 240000, MIN_HUNT_GAP = 0.12;

/* ---------- أدوات ---------- */
const clamp=(v,a,b)=>Math.min(b,Math.max(a,v));
const uid=p=>p+'_'+Math.random().toString(36).slice(2,10)+Date.now().toString(36).slice(-4);
const fmtPx=n=>{ if(!n) return '—';
  if(n>=1000) return n.toLocaleString('en-US',{maximumFractionDigits:2});
  if(n>=1) return n.toFixed(2);
  const s=n.toFixed(12).replace(/0+$/,'').replace(/\.$/,'');
  return s.length>11 ? n.toPrecision(4) : s; };
const fmtUsd=(n,d=2)=>{ const s=n<0?'-':n>0?'+':''; return s+Math.abs(n).toFixed(d); };
const fmtTime=t=>new Date(t).toLocaleTimeString('en-GB',{hour12:false});
const nextEma=(p,x,k)=>p==null?x:x*k+p*(1-k);
const volSum=l=>(l||[]).reduce((a,b)=>a+(b.size||0),0);
function roundTick(p,t){ if(!t||t<=0) return p;
  const n=Math.round(p/t)*t, d=Math.max(0,Math.ceil(-Math.log10(t)));
  return Number(n.toFixed(d)); }
function beep(tone){ try{ const c=new (window.AudioContext||window.webkitAudioContext)();
  const o=c.createOscillator(),g=c.createGain(); o.connect(g); g.connect(c.destination);
  const t=c.currentTime;
  if(tone==='bell'){ o.type='sine'; o.frequency.setValueAtTime(1319,t);
    o.frequency.setValueAtTime(1760,t+.1);
    g.gain.setValueAtTime(.16,t); g.gain.exponentialRampToValueAtTime(.001,t+.35); o.start(t); o.stop(t+.36); }
  else if(tone==='alarm'){ o.type='square'; o.frequency.value=540;
    g.gain.setValueAtTime(.1,t); g.gain.setValueAtTime(0,t+.12);
    g.gain.setValueAtTime(.1,t+.18); g.gain.setValueAtTime(0,t+.32); o.start(t); o.stop(t+.36); }
  else { o.type='sine'; o.frequency.setValueAtTime(880,t);
    o.frequency.exponentialRampToValueAtTime(660,t+.14);
    g.gain.setValueAtTime(.1,t); g.gain.exponentialRampToValueAtTime(.001,t+.22); o.start(t); o.stop(t+.24); }
  setTimeout(()=>{ try{c.close();}catch(e){} },800); }catch(e){} }
/* إشعارات الجوال الأصلية — تعمل والتطبيق في الخلفية */
function lnPlugin(){ try{ return window.Capacitor&&window.Capacitor.Plugins&&window.Capacitor.Plugins.LocalNotifications; }catch(e){ return null; } }
let _channelsReady=false;
function ensureChannels(LN){ if(_channelsReady||!LN.createChannel) return; _channelsReady=true;
  // قناة تنبيهات عالية الأهمية: تظهر منبثقة مع صوت حتى والشاشة مقفلة
  LN.createChannel({id:'trq_alerts',name:'تنبيهات TRQ',description:'صفقات وجني أرباح وتحذيرات',
    importance:5,visibility:1,vibration:true,lights:true}).catch(()=>{});
  // قناة حالة دائمة صامتة للإشعار المستمر
  LN.createChannel({id:'trq_status',name:'حالة TRQ',description:'إشعار البقاء الدائم',
    importance:2,visibility:-1,vibration:false}).catch(()=>{}); }
function nativeNotify(title,body,ongoing){ const LN=lnPlugin(); if(!LN) return;
  ensureChannels(LN);
  LN.requestPermissions().then(r=>{ if(r.display!=='granted') return;
    LN.schedule({notifications:[{id:ongoing?7:((Date.now()%2000000000)+10),
      title,body,ongoing:!!ongoing,autoCancel:!ongoing,
      channelId:ongoing?'trq_status':'trq_alerts'}]}).catch(()=>{}); }).catch(()=>{}); }
/* بقاء صوتي صامت — تشغيل تيار صوتي غير مسموع يمنع نظام الويب من خنق
   مؤقتات المحرك وقناة الأسعار عند إطفاء الشاشة (السبب الأكبر لتوقف البوت) */
let _kaCtx=null;
export function startSilentKeepAlive(){ if(_kaCtx||!(typeof window!=='undefined'&&window.Capacitor)) return;
  try{ const c=new (window.AudioContext||window.webkitAudioContext)(); _kaCtx=c;
    const o=c.createOscillator(),g=c.createGain(); g.gain.value=0.0001;
    o.frequency.value=18; o.connect(g); g.connect(c.destination); o.start();
    setInterval(()=>{ if(_kaCtx&&_kaCtx.state!=='running') _kaCtx.resume().catch(()=>{}); },15000);
  }catch(e){} }
/* خدمة أمامية أندرويد: إشعار دائم يُبقي العملية حية ويمنع النظام من قتلها في الخلفية،
   + طلب استثناء «تحسين البطارية» مرة واحدة — السبب الثاني الأكبر لتوقف البوت أثناء النوم */
function startForegroundSvc(){ if(!(typeof window!=='undefined'&&window.Capacitor)) return;
  try{ const P=(window.Capacitor&&window.Capacitor.Plugins)||{};
    const FS=P.ForegroundService;
    if(FS&&FS.startForegroundService)
      FS.startForegroundService({id:7,title:'TRQ يعمل',
        body:'متصل بالمنصة ويراقب الصفقات باستمرار',smallIcon:'ic_launcher',silent:true}).catch(()=>{});
    const BO=P.BatteryOptimization;
    if(BO&&BO.isBatteryOptimizationEnabled&&!(localStorage.getItem('trq_bo_asked')))
      BO.isBatteryOptimizationEnabled().then(r=>{
        if(r&&r.enabled&&BO.requestIgnoreBatteryOptimization){
          localStorage.setItem('trq_bo_asked','1');
          BO.requestIgnoreBatteryOptimization().catch(()=>{}); } }).catch(()=>{});
  }catch(e){} }
/* قفل استيقاظ المعالج + منعّاش أصلي: الإضافة المحلية TrqNative تمنع نوم المعالج
   بإطفاء الشاشة، وتراقب نبض المحرك من خارج الويب فيو — إن جُمّد 90 ثانية
   أُعيد تحميل التطبيق تلقائيًا فيُستأنف الاتصال والعمل دون تدخل */
function startNativeKeepAlive(){ if(!(typeof window!=='undefined'&&window.Capacitor)) return;
  const N=(window.Capacitor.Plugins||{}).TrqNative; if(!N) return;
  try{ N.acquireWakeLock&&N.acquireWakeLock().catch(()=>{}); }catch(e){}
  try{ N.startWatchdog&&N.startWatchdog().catch(()=>{}); }catch(e){}
  if(!window.__trqHb) window.__trqHb=setInterval(()=>{
    try{ N.heartbeat&&N.heartbeat().catch(()=>{}); }catch(e){} },15000);
  pushLog('server','قفل المعالج والمنعّاش الذاتي فعّالان — المحرك لا ينام بإطفاء الشاشة'); }
// علامة الإنعاش: المنعّاش الأصلي أعاد تحميل التطبيق بعد تجميد — يُسجَّل عند الإقلاع
function logRevival(){ try{ const at=localStorage.getItem('trq_revived');
  if(at){ localStorage.removeItem('trq_revived');
    const s=Math.max(1,Math.round((Date.now()-+at)/1000));
    pushLog('server','أُنعش التطبيق تلقائيًا بعد تجميد النظام له — استؤنف العمل خلال '+s+' ث'); } }catch(e){} }
function clearNativeOngoing(){ const LN=lnPlugin(); if(LN) LN.cancel({notifications:[{id:7}]}).catch(()=>{}); }
function notify(txt,kind){ if(S.sound){
    // نغمة مميزة لكل حدث: ربح = نغمتان صاعدتان · تحذير = تنبيه قوي · عادي = النغمة المختارة
    if(kind==='win'){ beep(S.soundTone); setTimeout(()=>{ try{beep('bell');}catch(e){} },200); }
    else if(kind==='warn'){ beep('alarm'); }
    else beep(S.soundTone);
    try{navigator.vibrate&&navigator.vibrate(kind==='win'?[80,60,140]:120);}catch(e){} }
  pushLog(kind==='warn'?'error':'fill',txt); nativeNotify('TRQ Trading',txt); }

/* ---------- مخزن الحالة + إشعارات الواجهة ---------- */
const listeners=new Set();
let emitTimer=null;
export function subscribe(fn){ listeners.add(fn); return ()=>listeners.delete(fn); }
export function emit(){ for(const fn of [...listeners]) fn(); }
function emitThrottled(){
  if(emitTimer) return;
  // توفير البطارية: التطبيق في الخلفية = تحديثات واجهة أبطأ، والمحرك والتنفيذ بلا تغيير
  const d=(typeof document!=='undefined'&&document.hidden)?1500:300;
  emitTimer=setTimeout(()=>{ emitTimer=null; emit(); },d);
}
function toast(m){ S.toastMsg={text:m,at:Date.now()}; emit(); }

/* ---------- الحالة ---------- */
export const S = {
  keys:null,
  config:{symbol:'XBTUSDTM',displaySymbol:'BTC',leverage:3,gridStepPct:0.3,huntPct:0.15,
    levels:12,direction:'short',directionMode:'auto',cycleBalance:100,mode:'paper'},
  status:'idle', heartbeat:0, lastPrice:null, markPrice:null,
  multiplier:1, tickSize:0.1, makerFee:0.0002, takerFee:0.0006, funding:0,
  grid:[], position:null, journal:[], logs:[],
  history:[], linkOk:false, exEquity:null,
  realizedPnl:0, feesPaid:0,
  priceTrail:[], emaFast:null, emaSlow:null, cvd:0, tape:[],
  biasScore:0, biasReasons:[], regime:null, confluence:null,
  huntAnchor:null, gridAnchor:null, lastHuntAt:0, lastAddAt:0, lastWorkAt:0, huntCount:0,
  liqPrice:null, exLiqPrice:null, mmr:0.004,
  orderBook:{bids:[],asks:[]}, startedAt:null, activeCycle:null,
  studyUntil:0, studyAcc:null, // دراسة العملة الجديدة قبل أي دخول
  ignoreExchangeUntil:0, lastTickAt:null, sound:true, soundTone:'soft', permDenied:false,
  toastMsg:null, _metaAt:0, _lastTrailAt:0,
  memory:{pairs:{},cycles:0,totalPnl:0}, // الذاكرة القوية — تبقى عبر الدورات ولا تُمسح أبدًا
};

/* ---------- التخزين ---------- */
function saveAll(){ try{
  localStorage.setItem('trq:cfg',JSON.stringify(S.config));
  localStorage.setItem('trq:keys',S.keys?JSON.stringify(S.keys):'');
  localStorage.setItem('trq:snd',S.sound?'1':'0');
  localStorage.setItem('trq:tone',S.soundTone||'soft');
  const rt={grid:S.grid,position:S.position,journal:S.journal,realizedPnl:S.realizedPnl,
    feesPaid:S.feesPaid,priceTrail:S.priceTrail,cvd:S.cvd,huntCount:S.huntCount,
    lastPrice:S.lastPrice,markPrice:S.markPrice,activeCycle:S.activeCycle,status:S.status,
    history:S.history,logs:S.logs};
  localStorage.setItem('trq:rt',JSON.stringify(rt));
  localStorage.setItem('trq:mem',JSON.stringify(S.memory||{pairs:{},cycles:0,totalPnl:0}));
}catch(e){} }
function loadAll(){ try{
  const cfg=JSON.parse(localStorage.getItem('trq:cfg')||'null'); if(cfg) S.config={...S.config,...cfg};
  const ks=localStorage.getItem('trq:keys'); if(ks){ try{S.keys=JSON.parse(ks);}catch(e){S.keys=null;} }
  S.sound=localStorage.getItem('trq:snd')!=='0';
  S.soundTone=localStorage.getItem('trq:tone')||'soft';
  const rt=JSON.parse(localStorage.getItem('trq:rt')||'null');
  if(rt){ Object.assign(S,{grid:rt.grid||[],position:rt.position||null,journal:rt.journal||[],
    realizedPnl:rt.realizedPnl||0,feesPaid:rt.feesPaid||0,priceTrail:rt.priceTrail||[],
    cvd:rt.cvd||0,huntCount:rt.huntCount||0,lastPrice:rt.lastPrice??null,
    markPrice:rt.markPrice??null,activeCycle:rt.activeCycle||null,status:rt.status||'idle',
    history:rt.history||[],logs:rt.logs||[]}); }
  // لا استئناف تلقائي أبدًا: البوت لا يعمل بعد فتح التطبيق إلا بضغطة «تشغيل» من المالك.
  // جلسة كانت تعمل تُستعاد «متوقفة مؤقتًا» — يبقى الجني والحارس يحميان أي مركز مفتوح،
  // لكن لا صفقة جديدة ولا صيد حتى يقرر المالك
  if(S.status==='running'){ S.status='paused';
    pushLog('info','استُعيدت الجلسة متوقفة مؤقتًا — اضغط «تشغيل» للبدء'); }
  // بصمات العملات (دراسة 60 ثانية) تُحفظ — أما سجل التعلم فأُلغي نهائيًا بطلب المالك
  const mem=JSON.parse(localStorage.getItem('trq:mem')||'null');
  S.memory={v:3,pairs:{},cycles:0,totalPnl:0,profiles:(mem&&mem.profiles)||{}};
}catch(e){} }

/* ---------- السجل ---------- */
function pushLog(kind,text){ S.logs=[{id:uid('l'),at:Date.now(),kind,text},...S.logs].slice(0,80); }
function pushJr(row){ S.journal=[row,...S.journal].slice(0,120); }

/* ================================================================
   عميل KuCoin — REST عبر البروكسي المحلي /kucoin
   مع تحويل تلقائي للاتصال المباشر داخل WebView/APK (بلا CORS)
   ================================================================ */
const DIRECT_BASE = 'https://api-futures.kucoin.com';
// داخل تطبيق الجوال (Capacitor) لا يوجد بروكسي إطلاقًا — اتصال مباشر من أول طلب
const IS_NATIVE = (typeof window !== 'undefined' && (window.Capacitor || location.protocol === 'capacitor:'));
let API_BASE = IS_NATIVE ? DIRECT_BASE : '/kucoin';
// AbortSignal.timeout غير مدعوم في WebViews القديمة — بديل متوافق
function sig(ms){ const c=new AbortController(); setTimeout(()=>c.abort(),ms); return c.signal; }
async function kcFetch(path,opts){
  if(API_BASE===DIRECT_BASE){
    if(IS_NATIVE) return await fetch(API_BASE+path,opts);
    // تحوّل سابق بسبب عثرة بروكسي — إن فشل المباشر أيضًا (CORS) نرجع للبروكسي ولا نقفل عليه
    try{ return await fetch(API_BASE+path,opts); }
    catch(e){ if(e&&e.name==='AbortError') throw e;
      API_BASE='/kucoin'; return await fetch(API_BASE+path,freshOpts(opts)); } }
  let res;
  try{ res=await fetch(API_BASE+path,opts); }
  catch(e){
    // مهلة/إجهاض = مشكلة الطلب نفسه وليست غياب البروكسي — لا تحويل نهائي للمباشر بسببها
    if(e&&e.name==='AbortError') throw e;
    API_BASE=DIRECT_BASE; return await fetch(API_BASE+path,freshOpts(opts)); }
  // 404 أو استجابة HTML (خادم الجوال المحلي يرجع index.html برمز 200) = لا بروكسي — تحوّل مباشر نهائي
  if(res.status===404 || !(res.headers.get('content-type')||'').toLowerCase().includes('json')){
    API_BASE=DIRECT_BASE; return await fetch(API_BASE+path,freshOpts(opts)); }
  return res;
}
// إشارة المهلة الأولى قد تكون استُنفدت — المحاولة البديلة تحتاج إشارة جديدة وإلا فشلت فورًا
function freshOpts(opts){ if(!opts||!opts.signal) return opts;
  const o={...opts}; o.signal=sig(8000); return o; }
function normFee(raw,fb){ const n=Number(raw);
  if(!Number.isFinite(n)||n<=0) return fb;
  return n>=0.05 ? n/100 : n; }

async function hmacB64(secret,payload){
  const key=await crypto.subtle.importKey('raw',
    new TextEncoder().encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);
  const sig=await crypto.subtle.sign('HMAC',key,new TextEncoder().encode(payload));
  return btoa(String.fromCharCode(...new Uint8Array(sig)));
}
async function kcPublic(path,timeout){
  const res=await kcFetch(path,{cache:'no-store',
    signal:sig(timeout||3500)});
  if(!res.ok) throw new Error('KuCoin '+res.status);
  const j=await res.json();
  if(j.code!=='200000') throw new Error(j.msg||j.code);
  return j.data;
}
async function kcPrivate(method,path,body){
  if(!S.keys) throw new Error('لا مفاتيح — اربط KuCoin من الإعدادات');
  const ts=String(Date.now());
  const raw=body?JSON.stringify(body):'';
  const signStr=ts+method+path+raw;
  const res=await kcFetch(path,{method,cache:'no-store',
    signal:sig(6000),
    headers:{'Content-Type':'application/json',
      'KC-API-KEY':S.keys.apiKey,
      'KC-API-SIGN':await hmacB64(S.keys.apiSecret,signStr),
      'KC-API-TIMESTAMP':ts,
      'KC-API-PASSPHRASE':await hmacB64(S.keys.apiSecret,S.keys.passphrase),
      'KC-API-KEY-VERSION':'2'},
    body:raw||undefined});
  const j=await res.json().catch(()=>({code:'-1',msg:'bad json'}));
  if(j.code!=='200000') throw new Error(j.msg||('KuCoin '+res.status));
  return j.data;
}
export async function getApiBase(){ return API_BASE; }
const fetchTicker  = s => kcPublic('/api/v1/ticker?symbol='+encodeURIComponent(s),2500);
const fetchContract= s => kcPublic('/api/v1/contracts/'+encodeURIComponent(s),3500);
const fetchBook    = s => kcPublic('/api/v1/level2/depth20?symbol='+encodeURIComponent(s),2500)
  .then(d=>({bids:(d.bids||[]).map(r=>({price:+r[0],size:+r[1]})).filter(r=>r.price>0),
             asks:(d.asks||[]).map(r=>({price:+r[0],size:+r[1]})).filter(r=>r.price>0)}))
  .catch(()=>({bids:[],asks:[]}));
const fetchTape    = s => kcPublic('/api/v1/trade/history?symbol='+encodeURIComponent(s),2500)
  .then(a=>(a||[]).slice(0,32).map(t=>({side:String(t.side||''),size:+t.size||0,price:+t.price||0})))
  .catch(()=>[]);

/* ---------- شموع الأسعار + قائمة كل عقود المنصة ---------- */
export function fetchKlines(symbol,gran){
  return kcPublic('/api/v1/kline/query?symbol='+encodeURIComponent(symbol)+'&granularity='+gran,6000)
    .then(a=>(a||[]).map(r=>{
      let t=+r[0]; if(t<1e12) t*=1000; // توحيد الطابع الزمني إلى مللي ثانية
      return {t,o:+r[1],h:+r[2],l:+r[3],c:+r[4],v:+r[5]||0};
    }).filter(k=>k.t>0&&k.o>0).sort((x,y)=>x.t-y.t));
}
let contractsCache=null;
export function listContracts(){
  if(contractsCache&&Date.now()-contractsCache.at<300000) return Promise.resolve(contractsCache.items);
  return kcPublic('/api/v1/contracts/active',6000).then(d=>{
    const items=(d||[]).map(c=>({
      symbol:c.symbol,
      base:String(c.baseCurrency||c.symbol.replace(/USDTM?$/i,'')),
      name:String(c.baseCurrency||'')}));
    contractsCache={at:Date.now(),items};
    return items;
  });
}

/* ================================================================
   رادار الفرص — مسح كل عقود المنصة وترتيبها بدرجة ملاءمة للشبكة
   مرحلتان: (1) كل العقود بحجم 24 ساعة من طلب واحد
   (2) أعلى 18 سيولةً: شموع 15د + أفضل عرض/طلب لقياس التقلب والسبريد والاتجاه
   ================================================================ */
function radarScore(x){ // x: {vol,spread,chg,trendPct,turnover,price}
  let sc=50; const why=[];
  // السعر المنخفض (العملات الصفرية) أولوية قصوى: برأس مال صغير تعطي عقودًا كثيرة
  // بخطوات شبكة دقيقة وأرباحًا متكررة، والتصفية أبعد — الغالية تعطي عقدًا هشًا وتصفية قريبة
  if(x.price>0){
    if(x.price<0.001){ sc+=16; why.push('صفرية مثالية'); }
    else if(x.price<0.01){ sc+=12; why.push('صفرية'); }
    else if(x.price<0.1){ sc+=8; }
    else if(x.price<1){ sc+=3; }
    else if(x.price>100){ sc-=18; why.push('غالية — لا تناسب رأس المال'); }
    else if(x.price>10){ sc-=8; why.push('سعر مرتفع'); } }
  // التقلب المتحقق لكل شمعة 15د: الشبكة تريد ذبذبة كافية لا ميتة ولا وحشية
  if(x.vol>=0.18&&x.vol<=1.1){ sc+=18; why.push('تذبذب مثالي'); }
  else if(x.vol>1.1&&x.vol<=2){ sc+=6; why.push('تذبذب حاد'); }
  else if(x.vol<0.18){ sc-=14; why.push('سوق ميت'); }
  else { sc-=10; why.push('تذبذب وحشي'); }
  // السبريد: واسع يأكل ربح المستوى
  if(x.spread>0){ if(x.spread<=0.03) sc+=12; else if(x.spread<=0.08) sc+=6;
    else if(x.spread>0.2){ sc-=16; why.push('سبريد واسع'); } }
  // السيولة: دوران 24 ساعة بالدولار
  if(x.turnover>=5e7) sc+=10; else if(x.turnover>=1e7) sc+=6;
  else if(x.turnover<1e6){ sc-=14; why.push('سيولة ضعيفة'); }
  // نوع السوق: العرضي المتذبذب بيئة الشبكة المثالية؛ الاتجاه القوي يفيد الصيد لا الشبكة
  const a=Math.abs(x.trendPct);
  if(a<1.2){ sc+=8; x.state='عرضي متذبذب'; }
  else if(a<3.5){ sc+=4; x.state=x.trendPct>0?'صاعد':'هابط'; }
  else { sc-=4; x.state=x.trendPct>0?'صاعد بقوة':'هابط بقوة'; why.push('اتجاه حاد'); }
  // حركة 24 ساعة المتطرفة = إرهاق أو مخاطرة
  if(Math.abs(x.chg)>12){ sc-=6; why.push('حركة يوم متطرفة'); }
  return {score:clamp(Math.round(sc),0,100),why}; }

export async function scanRadar(){
  if(S._radarBusy) return S.radar; S._radarBusy=true;
  try{
    const all=await kcPublic('/api/v1/contracts/active',6000);
    const rows0=(all||[]).filter(c=>c&&c.symbol&&/USDTM$/i.test(c.symbol)&&c.status==='Open')
      .map(c=>({symbol:c.symbol,
        turnover:+(c.turnoverOf24h??c.turnover24h??c.volumeOf24h??c.volume24h??0)||0,
        price:+(c.lastTradePrice??c.markPrice??0)||0,
        chg:+(c.priceChgPct??0)*100||0}))
      .filter(r=>r.turnover>0&&r.price>0);
    // قاعدة المرشحين: الصفرية أولًا — رأس مال صغير يحتاج عملات رخيصة بسيولة،
    // مع إبقاء بضع عملات كبيرة للمقارنة فقط
    const cheap=rows0.filter(r=>r.price<1).sort((a,b)=>b.turnover-a.turnover).slice(0,16);
    const big=rows0.filter(r=>r.price>=1).sort((a,b)=>b.turnover-a.turnover).slice(0,4);
    const rows=[...cheap,...big];
    const det=await Promise.all(rows.map(async r=>{
      const out={...r,vol:0,spread:0,trendPct:0,price:0};
      try{ const ks=await fetchKlines(r.symbol,15);
        if(ks.length>=20){ const last=ks.slice(-24);
          let v=0; for(const k of last) v+=(k.h-k.l)/k.c; out.vol=v/last.length*100;
          out.price=ks[ks.length-1].c;
          out.trendPct=(out.price-ks[ks.length-20].o)/ks[ks.length-20].o*100; }
      }catch(e){}
      try{ const t=await fetchTicker(r.symbol);
        const bb=+t.bestBidPrice||0, ba=+t.bestAskPrice||0;
        if(bb>0&&ba>0) out.spread=(ba-bb)/((ba+bb)/2)*100;
        if(!out.price) out.price=+t.price||0; }catch(e){}
      return out; }));
    const list=det.filter(d=>d.price>0).map(d=>{ const s=radarScore(d);
      return {symbol:d.symbol,disp:d.symbol.replace(/USDTM$/i,'').replace(/^XBT/i,'BTC'),
        price:d.price,score:s.score,why:s.why,state:d.state||'—',
        vol:+d.vol.toFixed(2),chg:Math.round(d.chg*10)/10,turnover:d.turnover,
        grade:s.score>=65?'🟢':s.score>=45?'🟡':'🔴'}; })
      // الأفضل أولًا بلا تعادل مربك: الدرجة، ثم العرضي المتذبذب يتقدم، ثم الأعلى سيولة
      .sort((a,b)=>b.score-a.score||
        (b.state==='عرضي متذبذب')-(a.state==='عرضي متذبذب')||b.turnover-a.turnover);
    S.radar={at:Date.now(),list};
    // توصية التبديل: عملتي الحالية ضعيفة وأخرى أقوى منها بفارق واضح
    const cur=list.find(l=>l.symbol===S.config.symbol);
    const top=list[0];
    if(top&&cur&&top.symbol!==cur.symbol&&top.score-cur.score>=18&&cur.score<50){
      if(!S._radarTipAt||Date.now()-S._radarTipAt>1800000){ S._radarTipAt=Date.now();
        pushLog('server','📡 رادار: '+cur.disp+' ضعيفة ('+cur.score+') بينما '+top.disp+
          ' '+top.state+' بدرجة '+top.score+' — يُنصح بالتبديل بعد إغلاق أي مركز');
        toast('📡 الرادار يقترح '+top.disp+' بدل '+cur.disp);
        nativeNotify('📡 فرصة أقوى',top.disp+' '+top.state+' — درجة '+top.score+' مقابل '+cur.score+' لعملتك'); } }
    else if(top&&!cur&&top.score>=70){
      if(!S._radarTipAt||Date.now()-S._radarTipAt>1800000){ S._radarTipAt=Date.now();
        pushLog('server','📡 رادار: '+top.disp+' '+top.state+' بدرجة '+top.score+' — أقوى فرصة الآن'); } }
    emit(); saveAll();
    return S.radar;
  }catch(e){ return S.radar; }
  finally{ S._radarBusy=false; }
}

async function exPosition(symbol){
  const raw=await kcPrivate('GET','/api/v1/position?symbol='+encodeURIComponent(symbol));
  const p=Array.isArray(raw)?raw[0]:raw;
  const qty=p?Number(p.currentQty)||0:0;
  if(qty===0) return null;
  return {side:qty<0?'short':'long', size:Math.abs(qty),
    entry:Number(p.avgEntryPrice)||S.lastPrice||0,
    leverage:Number(p.leverage)||S.config.leverage,
    unrealized:Number(p.unrealisedPnl)||0,
    liquidation:Number(p.liquidationPrice)||null,
    openedAt:Date.now()};
}
const exOrders = symbol => kcPrivate('GET','/api/v1/orders?status=active&symbol='+encodeURIComponent(symbol))
  .then(d=>{const a=Array.isArray(d)?d:(d.items||[]);
    return a.map(o=>({orderId:o.id,clientOid:o.clientOid||null,side:o.side==='sell'?'sell':'buy',
      price:+o.price,size:+o.size,reduceOnly:!!o.reduceOnly}));})
  .catch(()=>[]);
/* رفض «وضع هامش الأمر لا يتطابق» — طابِق وضع الأمر مع وضع المركز الفعلي على المنصة قبل الإرسال */
const mmTried={};
// وضع الهامش يُخزَّن 60 ثانية لكل زوج — قراءته مع كل أمر كانت تضاعف طلبات التنفيذ
const mmCache={};
async function placeOrderSmart(body){
  try{ const cm=mmCache[body.symbol];
    if(cm&&Date.now()-cm.at<60000){ if(cm.mode) body={...body,marginMode:cm.mode}; }
    else { const pos=await kcPrivate('GET','/api/v1/position?symbol='+encodeURIComponent(body.symbol));
      if(pos&&typeof pos.crossMode==='boolean'){
        mmCache[body.symbol]={at:Date.now(),mode:pos.crossMode?'CROSS':'ISOLATED'};
        body={...body,marginMode:pos.crossMode?'CROSS':'ISOLATED'}; }
      else mmCache[body.symbol]={at:Date.now(),mode:null}; }
  }catch(_){}
  try{ return await kcPrivate('POST','/api/v1/orders',body); }
  catch(e){ let m=e.message||String(e);
    // رفض postOnly (السعر قاطع الدفتر): أعد المحاولة كأمر حدّي عادي — مستوى بعيد عابر لا يُفوَّت
    if(body.postOnly){ const b3={...body}; delete b3.postOnly;
      try{ return await kcPrivate('POST','/api/v1/orders',b3); }
      catch(e2){ m=e2.message||String(e2); body=b3; if(!/margin|هامش/i.test(m)) throw e2; } }
    if(!/margin|هامش/i.test(m)) throw e;
    if(!mmTried[body.symbol]){ mmTried[body.symbol]=1;
      try{ await kcPrivate('POST','/api/v1/marginMode/change',{symbol:body.symbol,marginMode:'ISOLATED'}); }catch(_){}
      try{ await kcPrivate('POST','/api/v1/position/margin/change-margin-mode',{symbol:body.symbol,marginMode:'ISOLATED'}); }catch(_){}
    }
    // جرّب الوضع المعاكس صراحةً، ثم بلا حقل نهائيًا (يتبع وضع الحساب)
    try{ const alt=body.marginMode==='ISOLATED'?'CROSS':'ISOLATED';
      return await kcPrivate('POST','/api/v1/orders',{...body,marginMode:alt}); }catch(_){}
    const b2={...body}; delete b2.marginMode;
    return await kcPrivate('POST','/api/v1/orders',b2); } }
function exPlaceLimit(intent){
  return placeOrderSmart({clientOid:intent.clientOid,symbol:intent.symbol,
    side:intent.side,type:'limit',price:String(intent.price),size:intent.qty,
    leverage:String(intent.leverage),timeInForce:'GTC',reduceOnly:!!intent.reduceOnly,
    postOnly:true, // كل الأوامر الحدّية صانعة سوق: رسوم 0.02% بدل 0.06% — برأس مال صغير الفرق صافٍ حقيقي
    marginMode:'ISOLATED'});
}
function exPlaceMarket(symbol,side,qty){
  return placeOrderSmart({clientOid:'hunt_'+Date.now().toString(36),
    symbol,side,type:'market',size:qty,leverage:String(S.config.leverage),marginMode:'ISOLATED'});
}
const exCancelAll = symbol => kcPrivate('DELETE','/api/v1/orders?symbol='+encodeURIComponent(symbol)).catch(()=>{});
const exCancelOne = id => kcPrivate('DELETE','/api/v1/orders/'+id).catch(()=>{});
// أوامر الوقف قناة منفصلة في KuCoin — «إلغاء الكل» العادي لا يشملها أبدًا
const exCancelStops = symbol => kcPrivate('DELETE','/api/v1/stopOrders?symbol='+encodeURIComponent(symbol)).catch(()=>{});
const exCancelStopOne = id => kcPrivate('DELETE','/api/v1/stopOrders/'+id).catch(()=>{});
// إغلاق حقيقي بكمية محددة — جني ربح فعلي على المنصة (سعر السوق، تخفيض فقط)
function exCloseQty(symbol,side,qty){
  return kcPrivate('POST','/api/v1/orders',{clientOid:'cls_'+Date.now().toString(36)+Math.floor(Math.random()*1000),
    symbol,type:'market',side:side==='short'?'buy':'sell',size:qty,reduceOnly:true}); }
function exClose(symbol,side){
  return kcPrivate('POST','/api/v1/orders',{clientOid:'cls_'+Date.now().toString(36),
    symbol,type:'market',side:side==='short'?'buy':'sell',closeOrder:true,reduceOnly:true});
}
async function exPing(){
  const a=await kcPrivate('GET','/api/v1/account-overview?currency=USDT');
  return a?Number(a.accountEquity??a.availableBalance??0):null;
}
/* حارس خادمي: أمر إيقاف طوارئ يعيش على خوادم KuCoin نفسها — يقفل المركز قبل التصفية
   حتى لو انقطع التطبيق أو نام الجوال (آخر خط دفاع عند غياب البوت) */
async function exPlaceStopGuard(){ if(S.config.mode!=='live'||!S.keys||!S.position) return;
  const liq=S.exLiqPrice||S.position.liquidation||S.liqPrice||0; if(!liq) return;
  const sh=S.position.side==='short';
  // يقف قبل التصفية مباشرة وبعد خط هروب التطبيق (0.985/1.015) — خلف التصفية لا يحمي شيئًا أبدًا
  const gp=sh?liq*0.99:liq*1.01;
  // حارس قائم قريب من المطلوب = لا شيء — إعادة الإرسال كل دقيقة كانت تراكم أوامر وقف مكدسة
  if(S._guardId&&S._guardPx&&Math.abs(gp-S._guardPx)/S._guardPx<0.005) return;
  try{ if(S._guardId){ await exCancelStopOne(S._guardId); S._guardId=null; } // ألغِ القديم قبل الجديد — لا تكديس
    const r=await kcPrivate('POST','/api/v1/orders',{clientOid:'grd_'+Date.now().toString(36),
      symbol:S.config.symbol,type:'market',side:sh?'buy':'sell',
      stop:sh?'up':'down',stopPrice:String(gp),stopPriceType:'MP',
      reduceOnly:true,closeOrder:true});
    S._guardId=(r&&(r.orderId||r.id))||null; S._guardPx=gp; S._guardAt=Date.now();
    if(!S._guardLogged){ S._guardLogged=true;
      pushLog('server','حارس خادمي مفعّل — إيقاف طوارئ على المنصة عند '+fmtPx(gp)+' (يعمل حتى لو نام التطبيق)'); }
  }catch(e){ if(!S._guardErrAt||Date.now()-S._guardErrAt>300000){ S._guardErrAt=Date.now();
      pushLog('error','الحارس الخادمي: '+(e.message||e)); } } }

/* ---------- تدفق الشريط والحيتان — من صفقات المنصة المنفذة لحظيًا ---------- */
function tapeFlow(){ const t=S.tape; if(!t||t.length<6) return {bias:0,whale:0,whaleVol:0};
  let b=0,s=0; const sizes=t.map(x=>x.size).sort((x,y)=>x-y);
  const med=sizes[Math.floor(sizes.length/2)]||1;
  let wb=0,ws=0;
  for(const x of t){ if(x.side==='buy')b+=x.size; else s+=x.size;
    if(x.size>med*5){ if(x.side==='buy')wb+=x.size; else ws+=x.size; } } // طبعة حوت: 5× الوسيط
  const tot=b+s||1, wtot=wb+ws||1;
  return {bias:(b-s)/tot, whale:(wb-ws)/wtot, whaleVol:(wb+ws)/tot}; }

/* ================================================================
   محرك القرار — شبكة + صيد + جني + انعكاس (مطابق للأصل)
   ================================================================ */
function wavg(f){ let n=0,q=0; for(const x of f){ if(x.qty>0&&x.price>0){n+=x.price*x.qty;q+=x.qty;} }
  return q>0?n/q:0; }
function filledAdds(){ const o=S.journal.filter(j=>j.status==='open');
  const n=o.reduce((a,j)=>a+Math.max(1,j.mergedOrders||1),0); return n>0?n:(S.position?1:0); }
function addsBlocked(){ if(S.status!=='running') return false;
  if(filledAdds()>=effLevels()) return true;
  return !!(S.position&&S.regime&&S.regime.shock); }
function adversePct(p){ const pos=S.position; if(!pos||!p||!pos.entry) return 0;
  return pos.side==='short'?((p-pos.entry)/pos.entry)*100:((pos.entry-p)/pos.entry)*100; }
function lastJumpPct(){ const t=S.priceTrail; if(t.length<2) return 0;
  const a=t[t.length-2],b=t[t.length-1]; return a>0?Math.abs(b-a)/a*100:0; }
function bookQuality(){ const b=S.orderBook,p=S.lastPrice||0;
  if(!b||!b.bids.length||!b.asks.length||!p) return true;
  const bb=b.bids[0].price,ba=b.asks[0].price;
  if(bb<=0||ba<=0) return true;
  const mid=(bb+ba)/2, sp=(ba-bb)/mid*100;
  if(sp>0.35) return false;
  if(lastJumpPct()>0.45) return false;
  const short=S.config.direction==='short';
  const wall=short?b.bids:b.asks, depth=volSum(wall.slice(0,8));
  const need=contractsForLevel(p)*2.5;
  return !(depth>0&&need>0&&depth<need); }
// وسيط أحجام مستويات الدفتر — مرجع لكشف الجدران الضخمة وتطبيع تدفق الأوامر
function medLevelSz(ob){ const s=[...ob.bids.slice(0,10),...ob.asks.slice(0,10)].map(x=>x.size).sort((a,b)=>a-b);
  return s.length?s[Math.floor(s.length/2)]:0; }
function retStdev(){ const t=S.priceTrail; if(t.length<4) return 0;
  const r=[]; for(let i=1;i<t.length;i++) if(t[i-1]>0) r.push((t[i]-t[i-1])/t[i-1]*100);
  if(r.length<3) return 0; const m=r.reduce((a,b)=>a+b,0)/r.length;
  return Math.sqrt(r.reduce((s,x)=>s+(x-m)**2,0)/r.length); }
function readRegime(){ const mom=S.confluence?S.confluence.momentum:0;
  const vol=retStdev(), jump=lastJumpPct();
  const shock=jump>0.9||Math.abs(mom)>0.9;
  const up=S.emaFast!=null&&S.emaSlow!=null&&S.emaFast>S.emaSlow;
  const dn=S.emaFast!=null&&S.emaSlow!=null&&S.emaFast<S.emaSlow;
  let trend='range';
  if((up&&mom>0.04&&S.biasScore>3)||mom>0.12) trend='up';
  else if((dn&&mom<-0.04&&S.biasScore<-3)||mom<-0.12) trend='down';
  if(Math.abs(mom)<0.04&&vol<0.08) trend='range';
  let vk='normal';
  if(vol>=0.22||shock) vk='wild'; else if(vol<=0.045&&Math.abs(mom)<0.045) vk='calm';
  let label='سوق عرضي';
  if(shock) label='تغير مفاجئ';
  else if(trend==='up') label=vk==='calm'?'صاعد هادئ':'اتجاه صاعد';
  else if(trend==='down') label=vk==='calm'?'هابط هادئ':'اتجاه هابط';
  else if(vk==='wild') label='عرضي متذبذب';
  else if(vk==='calm') label='عرضي هادئ';
  const young=!!(S.startedAt&&Date.now()-S.startedAt<SCOUT_MS);
  return {trend,vol:vk,shock,label,
    sizeMult:shock?0.65:vk==='wild'?0.75:trend==='range'?0.9:1,
    stepMult:vk==='wild'||shock?1.35:vk==='calm'?0.9:1,
    huntMult:shock?1.5:vk==='wild'?1.25:1,
    maxAdds:shock||vk==='wild'?1:3, scoutOnly:shock||vk==='wild'||young};
}
function scoreBias(){ let sc=0; const rs=[];
  const bv=volSum(S.orderBook.bids),av=volSum(S.orderBook.asks),bt=bv+av;
  if(bt>0){ const im=(bv-av)/bt; sc+=im*40;
    if(Math.abs(im)>0.08) rs.push(im>0?'دفتر مشترين':'دفتر بائعين'); }
  let buyT=0,sellT=0; for(const t of S.tape){ if(t.side==='buy')buyT+=t.size; else sellT+=t.size; }
  const tt=buyT+sellT;
  if(tt>0){ const im=(buyT-sellT)/tt; sc+=im*25;
    if(Math.abs(im)>0.1) rs.push(im>0?'شريط شراء':'شريط بيع'); }
  if(S.funding){ sc+=clamp(-S.funding*80000,-20,20);
    rs.push(S.funding>0?'تمويل إيجابي ← شورت':'تمويل سلبي ← لونغ'); }
  if(S.lastPrice&&S.emaFast!=null&&S.emaSlow!=null){
    if(S.lastPrice>S.emaFast&&S.emaFast>S.emaSlow){sc+=15;rs.push('متوسط سريع فوق البطيء');}
    else if(S.lastPrice<S.emaFast&&S.emaFast<S.emaSlow){sc-=15;rs.push('متوسط سريع تحت البطيء');} }
  if(S.cvd){ sc+=Math.sign(S.cvd)*Math.min(20,Math.log10(Math.abs(S.cvd)+1)*5); }
  return {direction:sc>=0?'long':'short',score:Math.round(sc*10)/10,reasons:rs.slice(0,3)};
}
function readConfluence(){ const t=S.priceTrail,w=t.slice(-10);
  const bids=volSum(S.orderBook.bids),asks=volSum(S.orderBook.asks),bd=bids+asks;
  const imb=bd>0?(bids-asks)/bd:0;
  let mom=0; if(w.length>=2&&w[0]>0) mom=(w[w.length-1]-w[0])/w[0]*100;
  let tn=0,tp=0; for(const r of S.tape){tn+=r.size;tp+=r.price*r.size;}
  const vwap=tn>0?tp/tn:null; let sc=0; const rs=[];
  if(Math.abs(imb)>0.08){sc+=imb*40;rs.push(imb>0?'دفتر شراء غالب':'دفتر بيع غالب');}
  if(Math.abs(mom)>=0.04){sc+=mom*8;rs.push(mom>0?'زخم صاعد':'زخم هابط');}
  if(vwap&&S.lastPrice){ if(S.lastPrice>vwap){sc+=8;rs.push('فوق VWAP');} else {sc-=8;rs.push('تحت VWAP');} }
  if(S.emaFast!=null&&S.emaSlow!=null){ if(S.emaFast>S.emaSlow){sc+=10;rs.push('ميل صاعد');}
    else {sc-=10;rs.push('ميل هابط');} }
  const idle=w.length>=8&&S.tape.length>=8&&Math.abs(mom)<0.04&&Math.abs(imb)<0.08;
  if(idle) rs.unshift('السوق خامل');
  return {score:Math.round(sc*10)/10,idle,momentum:Math.round(mom*1000)/1000,
    bookImb:Math.round(imb*1000)/1000,vwap,reasons:rs.slice(0,3)};
}
function resolveDirection(now){ const mode=S.config.directionMode;
  if(mode==='long'||mode==='short'){ S.config.direction=mode; return mode; }
  if(S.status==='paused'||S.status==='stopped') return S.config.direction;
  const bias=scoreBias(); S.biasScore=bias.score; S.biasReasons=bias.reasons;
  const mom=S.confluence?S.confluence.momentum:0;
  if(S.position){ const flipped=S._flipUntil&&now<S._flipUntil&&S.config.direction!==S.position.side;
    if(!flipped) S.config.direction=S.position.side;
    return S.config.direction; }
  const prev=S.config.direction;
  if(S.regime&&S.regime.shock) return prev;
  // لا تبديل اتجاه أكثر من مرة كل 60 ثانية — التقليب السريع كان يقطع الخاسر ويطارد الضجيج
  const flipOk=!S._dirFlipAt||now-S._dirFlipAt>60000;
  if(S.regime&&S.regime.trend==='up'&&bias.score>-6&&mom>-0.04&&!toxicDir('long')){
    if(prev!=='long'){ if(!flipOk) return prev; S._dirFlipAt=now; }
    S.config.direction='long'; return 'long'; }
  if(S.regime&&S.regime.trend==='down'&&bias.score<6&&mom<0.04&&!toxicDir('short')){
    if(prev!=='short'){ if(!flipOk) return prev; S._dirFlipAt=now; }
    S.config.direction='short'; return 'short'; }
  if(Math.abs(bias.score)<4) return prev;
  if(bias.direction!==prev&&flipOk&&Math.abs(bias.score)>=14&&!toxicDir(bias.direction)){
    S.config.direction=bias.direction; S._dirFlipAt=now; }
  else if(bias.direction===prev) S.config.direction=bias.direction;
  return S.config.direction;
}
function contractsForLevel(p){ const c=S.config;
  const cv=Math.max(1e-12,(S.multiplier||1)*p);
  const per=Math.max(0,c.cycleBalance)*Math.max(1,c.leverage)/Math.max(1,effLevels());
  const raw=Math.max(1,Math.floor(per/cv));
  return Math.max(1,Math.floor(raw*(S.regime?S.regime.sizeMult:1)*memSizeMult())); }
// المستويات الفعّالة: رأس المال الصغير يُركَّز لا يُفتَّت —
// عدد يضمن أن يكون صافي جني كل مستوى ≥ $0.15 بخطوة شبكة واحدة بعد الرسوم
export function effLevels(){ const c=S.config;
  const step=Math.max(0.12,addStepPct())/100;
  const fees=(S.makerFee||0.0002)+(S.takerFee||0.0006);
  const minN=0.15/Math.max(0.0005,step-fees); // أقل قيمة صفقة تحقق جنيًا مجديًا
  const budget=Math.max(0,c.cycleBalance)*Math.max(1,c.leverage);
  return clamp(Math.floor(budget/minN),4,Math.max(4,c.levels)); }
// مضاعف خطوة الشبكة المستمد من الذاكرة القوية:
// زوج/اتجاه رابح تاريخيًا → خطوة أضيق (التقاط أكثر) · خاسر → خطوة أوسع (حذر أكبر)
export function memStepMult(){ const lv=sanctionLevel(S.config.direction);
  if(lv>=2) return 1.4; // مقيّد/محظور: خطوة أوسع — حذر أكبر
  const r=memRec(S.config.direction); if(!r||r.n<6) return 1;
  const wr=smWR(r);
  if(wr>=0.6&&r.pnl>0) return 0.8;
  if(lv===1) return 1.25;
  return 1; }
function addStepPct(){ return Math.max(0.12,S.config.gridStepPct)*memStepMult()*(S.regime?S.regime.stepMult:1)*profStepMult(); }
function tooClose(a,b,st){ return a>0&&b>0&&Math.abs(a-b)/Math.max(a,b)*100<st*0.55; }
// أحجام متدرجة: كل منطقة دخول أكبر من سابقتها حتى يكون تعديل المتوسط فعّالًا —
// وإلا صار مجموع العقود السابقة أكبر من عقد التعديل فيصبح التعديل شبه معدوم
function levelQtys(center,n){ const c=S.config;
  const cv=Math.max(1e-12,(S.multiplier||1)*center);
  const total=Math.max(0,c.cycleBalance)*Math.max(1,c.leverage)
    *(S.regime?S.regime.sizeMult:1)*memSizeMult();
  const used=S.journal.filter(j=>j.status==='open')
    .reduce((a,j)=>a+j.qty*(S.multiplier||1)*j.entry,0);
  const budget=Math.max(0,total-used);
  const G=1.4; let sum=0; for(let i=0;i<n;i++) sum+=Math.pow(G,i);
  const base=Math.max(1,Math.floor(budget/cv/Math.max(1,sum)));
  const out=[]; let acc=0;
  for(let i=0;i<n;i++){ const q=Math.max(1,Math.floor(base*Math.pow(G,i)));
    if(acc+q*cv>budget&&out.length) break; // لا تتجاوز ميزانية الرافعة أبدًا
    out.push(q); acc+=q*cv; }
  return out; }
function buildGrid(center,wide){ const c=S.config,tick=S.tickSize||1e-10;
  // خطوة متكيفة مع التقلب اللحظي: الحركات المفاجئة توسّع المناطق تلقائيًا
  const vol=retStdev()||0.05;
  const volF=wide?1:Math.max(1,Math.min(3,vol*10));
  const step=c.gridStepPct/100*(wide?4:memStepMult()*(S.regime?S.regime.stepMult:1)*volF);
  const stepPct=wide?Math.max(0.12,c.gridStepPct)*4:addStepPct();
  const n=effLevels(); // تركيز يناسب رأس المال — لا تفتيت على 25 مستوى تافهًا
  const qtys=levelQtys(center,n);
  const filledN=filledAdds(); // عدد الأوامر الفعلية المملوءة (وليس صفوف الدفتر المدمجة) — حجم المستوى التالي يتبع ترتيبه الحقيقي
  const entry=S.position?S.position.entry:center;
  // السلم يمتد للخارج فقط: لا تسليح أبدًا داخل منطقة سبق الدخول فيها —
  // آخر منطقة دخول + خطوة كاملة هو الحد الأدنى للمستوى الجديد (يمنع تراكم الصفقات)
  // آخر منطقة دخول فعلية + خطوة كاملة هو الحد الأدنى للمستوى الجديد —
  // أسعار التنفيذ الحقيقية من الشبكة، لا متوسط الدفتر المدمج الذي يتأخر خلف السعر
  const fills=S.grid.filter(g=>!g.reduceOnly&&g.status==='filled')
    .map(g=>g.filledPrice||g.price).filter(v=>v>0);
  let from=c.direction==='short'?Math.max(center,entry):Math.min(center,entry);
  if(fills.length&&!wide){
    if(c.direction==='short') from=Math.max(from,Math.max(...fills)*(1+step*0.8));
    else from=Math.min(from,Math.min(...fills)*(1-step*0.8)); }
  const grid=[];
  const G=1.5, maxDist=Math.max(step*6,0.025); // سقف امتداد السلم الكلي
  const liq=S.liqPrice||(S.position&&S.position.liquidation)||0;
  for(let i=1;i<=n;i++){
    // مسافات متسارعة (هندسية): كل منطقة أبعد من سابقتها — لا أوامر متراصة عديمة الجدوى
    // الشبكة الواسعة أيضًا لها سقف (5%) — الامتداد المفتوح كان يسلّح مستويات على بعد 30%
    const dist=wide?Math.min(step*i,0.05):Math.min(maxDist,step*(Math.pow(G,i)-1)/(G-1));
    const raw=c.direction==='short'?from*(1+dist):from*(1-dist);
    const price=roundTick(raw,tick); if(!(price>0)) continue;
    // لا تسليح في آخر 1.5% قبل التصفية (متوافق مع حد الخطر) —
    // مستوى لن يعيش المركز ليراه عبء أعمى، لكن بقية المسافة صالحة للتسليح
    if(liq>0){ if(c.direction==='short'&&price>=liq*0.985) continue;
      if(c.direction==='long'&&price<=liq*1.015) continue; }
    if(grid.some(g=>tooClose(g.price,price,stepPct))) continue;
    if(c.direction==='short'&&price<=center*(1+step*0.4)) continue;
    if(c.direction==='long'&&price>=center*(1-step*0.4)) continue;
    // حجم المستوى يتبع ترتيبه الحقيقي في السلم (بعد المناطق المملوءة)
    const qi=Math.min(filledN+i-1,qtys.length-1);
    const qty=qtys.length?qtys[Math.max(0,qi)]:contractsForLevel(center);
    grid.push({id:uid('lvl'),clientOid:uid('oid'),
      side:c.direction==='short'?'sell':'buy',price,qty,status:'armed',
      reduceOnly:false,exchangeOrderId:null,filledAt:null,origin:'grid',createdAt:Date.now()});
  }
  return grid.sort((a,b)=>b.price-a.price); }
function sanitizeAdds(){ const cap=effLevels();
  if(filledAdds()>=cap){ const side=S.position&&S.position.side;
    for(const g of S.grid){ if(g.reduceOnly||g.filledAt) continue;
      if(g.status!=='armed'&&g.status!=='open') continue;
      if(g.lane==='trend') continue;
      const gs=g.side==='sell'?'short':'long';
      if(!side||gs===side){ g.status='cancelled'; g.exchangeOrderId=null; } }
    return; }
  const st=addStepPct(),px=S.lastPrice||0;
  for(const side of ['buy','sell']){
    const live=S.grid.filter(g=>!g.reduceOnly&&g.side===side&&(g.status==='armed'||g.status==='open'))
      .sort((a,b)=>Math.abs((b.price||0)-px)-Math.abs((a.price||0)-px));
    const kept=[];
    for(const g of live){ const gap=g.lane?Math.max(st,S.config.gridStepPct*4):st;
      if(kept.length>=cap||kept.some(k=>tooClose(k.price,g.price,gap))){
        g.status='cancelled'; g.exchangeOrderId=null; continue; }
      kept.push(g); } } }
function shouldFill(l,p){ if(l.status!=='open'&&l.status!=='armed') return false;
  return l.side==='sell'?p>=l.price:p<=l.price; }
function feeFor(notional,taker){ return notional*(taker?S.takerFee:S.makerFee); }
function lotNet(side,entry,exit,qty,fees){
  return (side==='short'?1:-1)*(entry-exit)*(S.multiplier||1)*qty-fees; }
function applyDelta(side,qty,price){ const dir=side==='sell'?'short':'long';
  if(!S.position){ S.position={side:dir,size:qty,entry:price,leverage:S.config.leverage,
    unrealized:0,openedAt:Date.now(),liquidation:null};
    S._exc={mae:0,mfe:0}; // انحرافات تُقاس من ولادة المركز
    return {realized:0,closedQty:0,addedQty:qty}; }
  const pos=S.position;
  const same=(pos.side==='short'&&side==='sell')||(pos.side==='long'&&side==='buy');
  if(same){ const t=pos.size+qty; pos.entry=(pos.entry*pos.size+price*qty)/t; pos.size=t;
    return {realized:0,closedQty:0,addedQty:qty}; }
  const closeQty=Math.min(qty,pos.size);
  const sgn=pos.side==='short'?1:-1;
  const pnl=sgn*(pos.entry-price)*(S.multiplier||1)*closeQty;
  S.realizedPnl+=pnl; pos.size-=closeQty;
  if(pos.size<=1e-9) S.position=null;
  const leftover=qty-closeQty;
  if(leftover>1e-9){ S.position={side:dir,size:leftover,entry:price,
    leverage:S.config.leverage,unrealized:0,openedAt:Date.now(),liquidation:null};
    S._exc={mae:0,mfe:0};
    return {realized:pnl,closedQty:closeQty,addedQty:leftover}; }
  return {realized:pnl,closedQty:closeQty,addedQty:0}; }
function noteEntry(qty,price,fee,source){ const side=S.position?S.position.side:(source==='hunt'?S.config.direction:'short');
  // لقطة ظروف الدخول لحظتها — تُختم على الصفقة عند إغلاقها في الذاكرة
  S._entryCtx={rg:S.regime?(S.regime.shock?'صدمة':S.regime.trend==='up'?'صاعد':S.regime.trend==='down'?'هابط':'عرضي'):'عرضي',
    ses:sessionOf(Date.now()),origin:source==='hunt'?'صيد':'شبكة',at:Date.now()};
  const label=source==='hunt'?'صفقة':'شبكة';
  const ex=S.journal.find(j=>j.status==='open'&&j.side===side);
  if(ex){ const t=ex.qty+qty; ex.entry=(ex.entry*ex.qty+price*qty)/t; ex.qty=t;
    ex.mergedOrders=(ex.mergedOrders||1)+1; ex.fees+=fee;
    if(S.position) ex.entry=S.position.entry;
    if(ex.source!==label) ex.source='مركز';
    if(S.activeCycle){S.activeCycle.fills++;S.activeCycle.entry=S.position?S.position.entry:ex.entry;}
    return ex.id; }
  const id=uid('jr');
  pushJr({id,source:label,side,qty,entry:S.position?S.position.entry:price,exit:0,pnl:0,
    fees:fee,openedAt:Date.now(),closedAt:0,mergedOrders:1,status:'open'});
  if(!S.activeCycle){ S.activeCycle={side,entry:price,openedAt:Date.now(),fills:1,levels:S.config.levels,pnl:0}; }
  else { S.activeCycle.fills++; if(S.position)S.activeCycle.entry=S.position.entry; }
  return id; }
function noteClose(qty,exit,fee,source,pnl,lotId){ const lotId_=lotId;
  const row=(lotId_?S.journal.find(j=>j.id===lotId_&&j.status==='open'):null)
    ||S.journal.find(j=>j.status==='open'&&(source==='إيقاف'||j.source===source))
    ||S.journal.find(j=>j.status==='open');
  if(row){ const share=row.qty>0?row.fees*(qty/row.qty):row.fees;
    const net=lotNet(row.side,row.entry,exit,qty,share+fee);
    row.status='closed'; row.exit=exit; row.pnl=net; row.fees+=fee; row.closedAt=Date.now();
    if(source==='إيقاف') row.source='إيقاف'; pnl=net; }
  else pushJr({id:uid('jr'),source,side:S.position?S.position.side:S.config.direction,
    qty,entry:exit,exit,pnl,fees:fee,openedAt:Date.now(),closedAt:Date.now(),
    mergedOrders:1,status:'closed'});
  if(S.activeCycle){ S.activeCycle.pnl+=pnl;
    if(!S.position){ S.activeCycle=null; } }
  // تعلّم من نتيجة الصفقة — يُخزَّن في الذاكرة القوية التي لا تُمسح مع الدورات
  learnTrade(row?row.side:(S.position?S.position.side:S.config.direction), pnl);
  return pnl; }
// فترة اليوم (UTC): آسيا 0-8 · أوروبا 8-16 · أمريكا 16-24
function sessionOf(ts){ const h=new Date(ts).getUTCHours(); return h<8?'آسيا':h<16?'أوروبا':'أمريكا'; }
// التعلّم من نتائج الصفقات معطّل نهائيًا بطلب المالك — لا كتابة ولا قيود
function learnTrade(side,pnl){ return; }
// قياس الانزلاق معطّل مع التعلم — لا كتابة في الذاكرة
function learnSlippage(side,slipPct){ return; }
// إحصاءات مجمعة من الذاكرة القوية — للوحة الأداء والتقارير
export function memStats(){ const mem=S.memory; const o={n:0,w:0,l:0,pnl:0,grossWin:0,grossLoss:0,maxLoseStreak:0,pf:0,winRate:0,avgWin:0,avgLoss:0};
  if(!mem||!mem.pairs) return o;
  for(const k of Object.keys(mem.pairs)){ const r=mem.pairs[k];
    o.n+=r.n||0; o.w+=r.w||0; o.l+=r.l||0; o.pnl+=r.pnl||0;
    o.maxLoseStreak=Math.max(o.maxLoseStreak,r.maxLoseStreak||0);
    if(r.w&&r.pnl>0){ /* التقريب على مستوى الزوج */ } }
  // الإجماليات الدقيقة تحتاج تفصيل كل صفقة — نقدّرها من متوسطات الأزواج
  o.winRate=o.n?o.w/o.n:0; o.pnl=Math.round(o.pnl*100)/100;
  let gw=0,gl=0,aw=0,al=0;
  for(const k of Object.keys(mem.pairs)){ const r=mem.pairs[k];
    if(r.pnl>0){ gw+=r.pnl; aw+=r.w?r.pnl/r.w:0; } else { gl+=-r.pnl; al+=r.l?-r.pnl/r.l:0; } }
  o.grossWin=Math.round(gw*100)/100; o.grossLoss=Math.round(gl*100)/100;
  o.pf=gl>0?Math.round(gw/gl*100)/100:(gw>0?99:0);
  o.avgWin=Math.round(aw*100)/100; o.avgLoss=Math.round(al*100)/100;
  return o; }
// حجم الصفقة التكيفي (Kelly مبسّط) — سلسلة نجاح تكبّر الحجم وسلسلة خسارة تصغّره
function memSizeMult(){ let m=1;
  const r=memRec(S.config.direction);
  if(r){ const st=r.streak||0;
    if(st>=3) m=1.25; else if(st<=-3) m=0.5; else if(st<=-2) m=0.7; }
  if(sanctionLevel(S.config.direction)>=2) m=Math.min(m,0.5); // مقيّد = نصف حجم
  if(S._probation) m=Math.min(m,0.5); // صفقة إعادة التأهيل = نصف حجم
  return m; }
// انحراف الحيتان: سعر يتحرك عكس تدفق الصفقات الكبيرة = انعكاس محتمل
// يرجع 1 (انحراف صعودي) / -1 (هبوطي) / 0 (لا انحراف)
function cvdDivergence(){ const t=S.cvdTrail; if(!t||t.length<15) return 0;
  let mn=Infinity,mx=-Infinity; for(const p of t){ if(p.cvd<mn)mn=p.cvd; if(p.cvd>mx)mx=p.cvd; }
  const range=mx-mn; if(!(range>0)) return 0;
  const a=t[Math.max(0,t.length-25)], b=t[t.length-1];
  const flow=(b.cvd-a.cvd)/range;
  const mom=S.confluence?S.confluence.momentum:0;
  if(mom>0.05&&flow<-0.4) return -1;
  if(mom<-0.05&&flow>0.4) return 1;
  return 0; }
// تقرير أداء دوري بإشعار — كل 6 ساعات أثناء العمل
function maybeReport(){ const now=Date.now();
  if(S.status!=='running'&&S.status!=='paused') return;
  if(S._lastReportAt&&now-S._lastReportAt<6*3600*1000) return;
  const st=memStats(); if(!st.n) return;
  S._lastReportAt=now;
  const msg='صفقات متعلَّمة: '+st.n+' · نجاح '+(st.winRate*100).toFixed(0)+'% · عامل الربح '+st.pf+' · صافي الذاكرة '+fmtUsd(st.pnl);
  pushLog('info','تقرير دوري — '+msg); nativeNotify('TRQ — تقرير الأداء',msg); }
function tpTargetPrice(side,price,qty,origin){
  const notional=Math.max(1e-9,qty*(S.multiplier||1)*price);
  // الغطاء برسوم المسار الفعلي: صفقات الصيد دخلت آخذًا (taker) لا صانعًا — حسابها بصانع كان يضيّق الهدف ويأكل الصافي
  const entryFee=origin==='hunt'?S.takerFee:S.makerFee;
  const cover=(entryFee+S.takerFee+MIN_NET_USD/notional)*100;
  const px=S.lastPrice||price;
  const uw=!!(S.position&&adversePct(px)>=0.35);
  const st=(uw?Math.max(0.04,cover*0.5):Math.max(cover,origin==='hunt'?0.22:0.24))/100;
  return side==='sell'?roundTick(price*(1-st),S.tickSize):roundTick(price*(1+st),S.tickSize); }
function placeTpOpposite(f){ if(S.status!=='running') return;
  const tp=tpTargetPrice(f.side,f.price,f.qty,f.origin);
  if(S.grid.some(g=>(g.status==='armed'||g.status==='open')&&g.reduceOnly&&
    Math.abs(g.price-tp)/tp<1e-6)) return;
  S.grid.push({id:uid('tp'),clientOid:uid('oid'),side:f.side==='sell'?'buy':'sell',
    price:tp,qty:f.qty,status:'armed',reduceOnly:true,exchangeOrderId:null,
    filledAt:null,origin:f.origin||'grid',createdAt:Date.now(),lotId:f.lotId});
  S.grid.sort((a,b)=>b.price-a.price); }
function syncPositionTp(){ if(S.status!=='running'||!S.position) return;
  const pos=S.position;
  const fSide=pos.side==='short'?'sell':'buy';
  const want=tpTargetPrice(fSide,pos.entry,pos.size,'grid');
  const tickTol=S.tickSize>0?S.tickSize*0.5:Math.abs(want)*1e-9;
  const liveTp=S.grid.filter(g=>g.reduceOnly&&(g.status==='armed'||g.status==='open'));
  const match=liveTp.find(g=>Math.abs(g.price-want)<=tickTol&&g.qty===pos.size);
  // لا هدم وإعادة بناء كل نبضة: الأمر المطابق يبقى، ويُلغى غيره فقط —
  // الهدم الدائم كان يراكم آلاف الأوامر الملغاة ويرسل إلغاءات للمنصة بلا توقف
  if(match){ for(const g of liveTp){ if(g!==match){ g.status='cancelled'; g.exchangeOrderId=null; } }
    return; }
  for(const g of liveTp){ g.status='cancelled'; g.exchangeOrderId=null; }
  placeTpOpposite({side:fSide,price:pos.entry,qty:pos.size,origin:'grid',
    lotId:S.journal.find(j=>j.status==='open')?.id}); }
function coveringLoser(l,p){ const pos=S.position; if(!pos||l.reduceOnly) return false;
  const cover=(pos.side==='short'&&l.side==='buy')||(pos.side==='long'&&l.side==='sell');
  return cover&&adversePct(p)>0.08; }
function fillLevel(id,fp,taker){ const l=S.grid.find(g=>g.id===id);
  if(!l||l.status==='filled') return;
  if(!l.reduceOnly&&coveringLoser(l,fp)) return;
  l.status='filled'; l.filledAt=Date.now(); l.filledPrice=fp;
  const fee=feeFor(l.qty*(S.multiplier||1)*fp,taker); S.feesPaid+=fee;
  const d=applyDelta(l.side,l.qty,fp);
  if(d.addedQty>0){ const lotId=noteEntry(d.addedQty,fp,fee,l.origin==='hunt'?'hunt':'grid');
    l.lotId=l.lotId||lotId;
    // قياس انزلاق التنفيذ: فرق سعر التنفيذ الفعلي عن سعر المستوى المطلوب
    if(l.price>0) learnSlippage(l.side==='sell'?'short':'long',
      Math.round(Math.abs(fp-l.price)/l.price*100*10000)/10000); }
  if(d.closedQty>0){ const src=l.origin==='hunt'?'صفقة':'شبكة';
    const row=l.lotId?S.journal.find(j=>j.id===l.lotId):null;
    const net=row?lotNet(row.side,row.entry,fp,row.qty,row.fees+fee)
               :d.realized-fee;
    noteClose(d.closedQty,fp,fee,src,net,l.lotId);
    if(l.origin==='hunt'&&l.reduceOnly) S.huntOpen=Math.max(0,(S.huntOpen||0)-1);
    const parent=S.grid.find(g=>!g.reduceOnly&&g.status==='filled'&&
      (l.lotId?g.lotId===l.lotId:g.origin===l.origin));
    if(parent) parent.status='cancelled';
    S.huntAnchor=fp; S.lastHuntAt=Date.now()-400;
    // ——— الجني الحقيقي على المنصة ———
    // الإغلاق المحلي وحده لا يحرّك المركز الفعلي — أغلق الكمية حقيقةً وألغِ أمر الجني المحدد
    if(l.reduceOnly&&S.config.mode==='live'&&S.keys){
      const row2=l.lotId?S.journal.find(j=>j.id===l.lotId):null;
      const cSide=row2?row2.side:(S.position?S.position.side:S.config.direction);
      if(l.exchangeOrderId) exCancelOne(l.exchangeOrderId);
      exCloseQty(S.config.symbol,cSide,d.closedQty)
        .catch(e=>pushLog('error','إغلاق الجني الحقيقي فشل: '+(e.message||e)));
      // اكتمل المركز بالكامل؟ — ألغِ كل الأوامر المعلقة (منصة وبوت) وابدأ دراسة دخول جديدة نظيفة
      if(!S.position){ exCancelAll(S.config.symbol).catch(()=>{});
        exCancelStops(S.config.symbol).catch(()=>{}); S._guardId=null; // وقف الحارس أيضًا — لا أوامر يتيمة
        for(const g of S.grid){ if(g.status==='armed'||g.status==='open'){
          g.status='cancelled'; g.exchangeOrderId=null; } }
        S.gridAnchor=null; S.huntAnchor=fp;
        pushLog('server','جني مكتمل ✓ — أُغلقت الصفقة حقيقيًا وأُلغيت أوامرها، تُدرس صفقة جديدة'); } } }
  if(S.position&&S.status==='running') syncPositionTp();
  S.lastWorkAt=Date.now();
  notify((l.side==='sell'?'بيع ':'شراء ')+(l.origin==='hunt'?'صفقة':l.reduceOnly?'جني':'شبكة')+' @ '+fmtPx(fp),
    (l.reduceOnly&&d.closedQty>0)?'win':null); }
function selectDueAdds(due,p){ if(addsBlocked()) return [];
  if(studying()) return [];
  if(toxicBlocked(S.config.direction)) return [];
  if(S.position&&!inAddZone(p)) return [];
  const adds=due.filter(l=>!l.reduceOnly&&!coveringLoser(l,p))
    .sort((a,b)=>Math.abs(a.price-p)-Math.abs(b.price-p));
  const now=Date.now();
  return adds.filter(()=>!(S.startedAt&&now-S.startedAt<SCOUT_MS&&filledAdds()>=1))
    .filter(()=>!(S.lastAddAt&&now-S.lastAddAt<ADD_COOLDOWN)).slice(0,1); }
function inAddZone(p){ if(S.status!=='running'||!p) return false;
  if(addsBlocked()) return false;
  if(lastJumpPct()>0.4) return false;
  if(!bookQuality()) return false;
  const pos=S.position; if(!pos||!pos.entry) return true;
  const step=Math.max(0.12,S.config.gridStepPct*memStepMult());
  const improve=pos.side==='short'?(p-pos.entry)/pos.entry*100:(pos.entry-p)/pos.entry*100;
  if(improve<step*0.7) return false;
  const mom=S.confluence?S.confluence.momentum:0;
  const r=S.regime||{trend:'range'};
  const against=(pos.side==='long'&&r.trend==='down')||(pos.side==='short'&&r.trend==='up');
  // صدمة معاكسة (اندفاع قوي ضد المركز): لا متوسطات أبدًا حتى يتوقف الاندفاع — حماية الرصيد أولًا
  if(r.shock&&against) return false;
  if(against&&((pos.side==='long'&&mom<-0.06)||(pos.side==='short'&&mom>0.06))) return false;
  if(pos.side==='long'&&mom<-0.12) return false;
  if(pos.side==='short'&&mom>0.12) return false;
  // فلتر الحيتان/التدفق: لا تزيد المركز عكس تدفق السيولة الكبيرة
  const f=tapeFlow();
  if(pos.side==='short'&&(f.whale>0.6||f.bias>0.45)) return false;
  if(pos.side==='long'&&(f.whale<-0.6||f.bias<-0.45)) return false;
  return true; }
function harvestRipe(p){ if(!p||S.status==='idle') return;
  for(const tp of S.grid.filter(g=>g.reduceOnly&&(g.status==='open'||g.status==='armed'))){
    const lot=tp.lotId?S.journal.find(j=>j.id===tp.lotId):null;
    const entry=lot?lot.entry:(S.position?S.position.entry:tp.price);
    const side=lot?lot.side:(S.position?S.position.side:S.config.direction);
    const sgn=side==='short'?1:-1;
    const pnl=sgn*(entry-p)*(S.multiplier||1)*tp.qty;
    const exitFee=feeFor(tp.qty*(S.multiplier||1)*p,true);
    const entryFee=lot?lot.fees:feeFor(tp.qty*(S.multiplier||1)*entry,false);
    const net=pnl-entryFee-exitFee;
    const stuck=!!(S.position&&adversePct(p)>=0.45);
    // حد الجني بطلب المالك: $0.15 صافيًا بعد الرسوم لكل صفقة — لا جني تافهًا ولا احتفاظ حتى الخسارة
    const minNet=0.15;

    // ——— تتبع الربح بالدولار (وليس بالسعر) ———
    // يتفعّل التتبع فور بلوغ الصافي $0.20، ويتبع القمة بفجوة $0.05:
    // أي تراجع يعيد الصافي إلى (القمة − $0.05) — وبحد أدنى $0.15 — يُجنى الربح فورًا.
    if(tp.peakNet!=null){
      if(net>tp.peakNet) tp.peakNet=net;
      const stop=Math.max(0.15,tp.peakNet-0.05);
      // هروب طارئ فقط: مركز عالق عكسيًا عميقًا يُقبل فيه خروج أصغر بدل كارثة
      const escape=stuck&&net>=0.10;
      if(net<=stop||escape) fillLevel(tp.id,p,true);
      continue; }

    if(net<minNet) continue;
    // تسامح بمقدار نصف تكة — «السعر عند المستوى» يُفعّل التتبع فورًا
    const tol=Math.min(S.tickSize>0?S.tickSize*0.5:1e-12, Math.abs(tp.price)*0.0005);
    const crossed=tp.side==='sell'?p>=tp.price-tol:p<=tp.price+tol;
    // يتفعّل التتبع ببلوغ $0.20 صافيًا، أو ببلوغ مستوى الجني مع صافٍ مجدٍ
    if(net>=0.20||crossed) tp.peakNet=net; }

  // ——— حارس صافي المركز كاملًا ———
  // فتيلة دقيقة واحدة قد ترفع صافي المركز فوق دولار بينما كل مستوى منفردًا تحت $0.20
  // فلا يتسلح أحدها — هنا يُراقب المجموع: تسليح عند $0.30 وجني الكل عند تراجع $0.08 من القمة
  if(S.position){ const pos=S.position, qty=pos.size||0;
    if(qty>0){ const sgn=pos.side==='short'?1:-1;
      const netAll=sgn*(pos.entry-p)*(S.multiplier||1)*qty
        -qty*(S.multiplier||1)*(pos.entry*S.makerFee+p*S.takerFee);
      if(S._posPeakNet==null){ if(netAll>=0.30){ S._posPeakNet=netAll;
          pushLog('server','⚡ صافي المركز $'+netAll.toFixed(2)+' — تتبع الانزلاق مُسلّح'); } }
      else{ if(netAll>S._posPeakNet) S._posPeakNet=netAll;
        const stopAll=Math.max(0.15,S._posPeakNet-0.08);
        if(netAll<=stopAll){ S._posPeakNet=null;
          pushLog('server','⚡ صيد انزلاق ✓ — تراجع الصافي من القمة، جني المركز كاملًا');
          flattenAt(p,'صيد انزلاق'); return; } } } }
  else S._posPeakNet=null;

  // ——— درع الانزلاق العكسي ———
  // حركة حادة ضد المركز خلال ثوانٍ: ألغِ تسليح المستويات فورًا — إضافة في شلال = متوسط كارثي
  const pv=S._harvPrev;
  if(S.position&&pv&&pv.px>0){ const dt=Date.now()-pv.at;
    if(dt>0&&dt<6000){ const adv=(S.position.side==='short'?(p-pv.px):(pv.px-p))/pv.px*100;
      if(adv>=0.35){ cancelPendingAdds();
        if(!S._advSpkAt||Date.now()-S._advSpkAt>60000){ S._advSpkAt=Date.now();
          pushLog('server','⚡ انزلاق عكسي '+adv.toFixed(2)+'% خلال '+Math.round(dt/1000)+'ث — أُلغي التسليح حتى يهدأ السعر'); } } } }
  S._harvPrev={px:p,at:Date.now()}; }
function trailHuntAnchor(p){ if(!p||S.status!=='running') return;
  if(!S.huntAnchor){S.huntAnchor=p;return;}
  if(S.config.direction==='short'){ if(p>S.huntAnchor)S.huntAnchor=p; }
  else if(p<S.huntAnchor) S.huntAnchor=p; }
function huntAligned(){ const conf=S.confluence||{momentum:0,idle:false,score:0};
  const short=S.config.direction==='short';
  if(!bookQuality()) return false;
  const r=S.regime||{trend:'range',shock:false};
  const withTrend=!S.position&&((short&&r.trend==='down'&&conf.momentum<=-0.06)||
    (!short&&r.trend==='up'&&conf.momentum>=0.06));
  if(r.shock&&!withTrend) return false;
  if(short&&r.trend==='up') return false;
  if(!short&&r.trend==='down') return false;
  if(conf.idle&&S.tape.length>=8) return false;
  if(short&&conf.momentum>0.06) return false;
  if(!short&&conf.momentum<-0.06) return false;
  if(short&&S.biasScore>=10) return false;
  if(!short&&S.biasScore<=-10) return false;
  if(short&&conf.score>=12) return false;
  if(!short&&conf.score<=-12) return false;
  if(S.position&&S.position.side===S.config.direction){
    const against=(S.position.side==='short'&&S.biasScore>=12)||
      (S.position.side==='long'&&S.biasScore<=-12);
    if(against) return false; }
  // فلتر الحيتان/التدفق: لا تدخل صفقة جديدة عكس تدفق السيولة الكبيرة
  const f=tapeFlow();
  if(f.whaleVol>0.25){ if(short&&f.whale>0.5) return false; if(!short&&f.whale<-0.5) return false; }
  if(short&&f.bias>0.35) return false; if(!short&&f.bias<-0.35) return false;
  // فلتر التمويل: تمويل مرتفع ضد اتجاهك يأكل الربح بصمت — لا دخول
  const fund=S.funding||0;
  if(!short&&fund>0.0004) return false;   // لونغ يدفع تمويلًا مرتفعًا
  if(short&&fund<-0.0004) return false;   // شورت يدفع تمويلًا مرتفعًا
  // انحراف الحيتان: سعر يتحرك عكس تدفقهم = انعكاس وشيك — لا تدخل ضده
  const dv=cvdDivergence();
  if(short&&dv===1) return false;   // تدفق شرائي قوي تحت سعر هابط
  if(!short&&dv===-1) return false; // تدفق بيعي قوي فوق سعر صاعد
  // بوابة الذاكرة القوية (بتجانس بايزي): اتجاه خاسر بعينة حقيقية يُردع —
  // الحظر الكامل للسام فقط في toxicBlocked، هنا تشدد الظرف لا إعدامه
  const mp=memRec(short?'short':'long');
  if(mp&&mp.n>=15&&mp.pnl<0&&smWR(mp)<0.30){
    if(!S._memBlockAt||Date.now()-S._memBlockAt>600000){ S._memBlockAt=Date.now();
      pushLog('info','الذاكرة: اتجاه '+(short?'الشورت':'اللونغ')+' على هذا الزوج خاسر بعينة كافية — ممنوع مؤقتًا'); }
    return false; }
  // بوابة التوقيت والظرف: فترة اليوم أو نوع السوق خاسر تاريخيًا لهذا الزوج/الاتجاه
  if(mp){
    const so=mp.sessions&&mp.sessions[sessionOf(Date.now())];
    if(so&&so.n>=8&&so.pnl<0&&smWR(so)<0.32) return false;
    const rg=S.regime?(S.regime.shock?'صدمة':S.regime.trend==='up'?'صاعد':S.regime.trend==='down'?'هابط':'عرضي'):'عرضي';
    const ro=mp.regimes&&mp.regimes[rg];
    if(ro&&ro.n>=8&&ro.pnl<0&&smWR(ro)<0.32) return false; }
  // تأكيد النشاط: معدل صفقات آخر دقيقة أدنى من نصف وسيطه التاريخي = سوق ميت — ارفض
  const am=S._actMed||0;
  if(am>=10){ const cut=Date.now()-60000, rate=(S._tradeTs||[]).filter(t=>t>=cut).length;
    if(rate<am*0.5) return false; }
  // فلتر قرب الجدار + تطبيع OFI على وسيط أحجام مستويات الدفتر
  const ob=S.orderBook,lp=S.lastPrice||0;
  if(ob&&ob.bids.length&&ob.asks.length&&lp>0){ const medL=medLevelSz(ob);
    if(medL>0){
      // جدار ضخم (5× الوسيط) على مسافة <0.15% بوجه اتجاه الصيد = مصيدة — ارفض
      if(!short){ for(const a of ob.asks){ if(a.price>lp&&(a.price-lp)/lp*100<0.15&&a.size>medL*5) return false; } }
      else { for(const b of ob.bids){ if(b.price<lp&&(lp-b.price)/lp*100<0.15&&b.size>medL*5) return false; } }
      // بوابة تدفق الأوامر (OFI): ضغط أوامر حدّية قوي عكس اتجاه الصيد = لا تدخل
      const nofi=(S._ofi||0)/(medL*20);
      if(short&&nofi>0.6) return false; if(!short&&nofi<-0.6) return false; } }
  // قائد BTC: نبضة قوية في المؤشر عكس اتجاه الصيد على العملات التابعة = ارفض
  if(S._btcImp!=null&&Math.abs(S._btcImp)>=0.10){
    if(short&&S._btcImp>0) return false; if(!short&&S._btcImp<0) return false; }
  return true; }
/* ================================================================
   فلسفة الصيد الجديدة: الإشارات أبواب مستقلة — أي باب يفتح = صفقة.
   لا «لابد أن تنطبق جميعها». المنع فقط بفيتو خطر قوي مثبت.
   ================================================================ */
// فيتو الخطر: أسباب قوية وحدها تمنع فتح صفقة — ما دونها لا يوقف الصيد
function dangerVeto(p){ const short=S.config.direction==='short';
  if(!bookQuality()) return 'دفتر أو سبريد رديء';
  if(toxicBlocked(S.config.direction)) return 'اتجاه سام في الذاكرة (يُعاد اختباره كل ساعة)';
  const r=S.regime||{trend:'range',shock:false}, conf=S.confluence||{momentum:0};
  // صدمة سعرية تقود عكس اتجاه الصيد بقوة
  if(r.shock){ if(short&&conf.momentum>0.3) return 'صدمة صاعدة عنيفة ضد الشورت';
    if(!short&&conf.momentum<-0.3) return 'صدمة هابطة عنيفة ضد اللونغ'; }
  // جدار لاصق مباشرة (<0.10%) بحجم 6 أضعاف الوسيط — مصيدة محققة لا مجرد مقاومة
  const ob=S.orderBook, lp=S.lastPrice||0;
  if(ob&&ob.bids.length&&ob.asks.length&&lp>0){ const medL=medLevelSz(ob);
    if(medL>0){ if(!short){ for(const a of ob.asks.slice(0,5)){ if(a.price>lp&&(a.price-lp)/lp*100<0.10&&a.size>medL*6) return 'جدار بيع لاصق فوق السعر'; } }
      else { for(const b of ob.bids.slice(0,5)){ if(b.price<lp&&(lp-b.price)/lp*100<0.10&&b.size>medL*6) return 'جدار شراء لاصق تحت السعر'; } } } }
  // سيل حيتان هائل عكس الاتجاه (تدفق مهيمن + حجم استثنائي)
  const f=tapeFlow();
  if(f.whaleVol>0.4){ if(short&&f.whale>0.7) return 'سيل شراء حيتان مهيمن'; if(!short&&f.whale<-0.7) return 'سيل بيع حيتان مهيمن'; }
  // تمويل متطرف يأكل الصفقة
  const fund=S.funding||0;
  if(!short&&fund>0.001) return 'تمويل مرتفع جدًا ضد اللونغ';
  if(short&&fund<-0.001) return 'تمويل مرتفع جدًا ضد الشورت';
  return null; }
// كاشف الإشارات: الحيتان/الفجوات/الارتداد/الزخم/الضغط — أي واحدة تكفي لفتح صفقة
function huntSignals(p){ const short=S.config.direction==='short'; const sig=[];
  const conf=S.confluence||{momentum:0,score:0}, r=S.regime||{trend:'range'};
  // 1) المرساة الكلاسيكية: تحرك ≥ عتبة الصيد من القمة/القاع
  if(S.huntAnchor){ const pct=(p-S.huntAnchor)/S.huntAnchor*100, need=effectiveHuntPct();
    if(short?pct<=-need:pct>=need) sig.push('مرساة '+Math.abs(pct).toFixed(2)+'%'); }
  // 2) فجوة السعر عن المارك — صيد ارتداد
  const mk=S.markPrice||0;
  if(mk>0){ const dev=(p-mk)/mk;
    if(Math.abs(dev)>=0.0015&&((dev>0&&short)||(dev<0&&!short))) sig.push('فجوة مارك'); }
  // 3) فخ الاختراق (صيد السلاحف)
  if(S._brk&&Date.now()-S._brk.at<10000){
    if((S._brk.side==='up'&&p<S._brk.level&&short)||(S._brk.side==='down'&&p>S._brk.level&&!short)){
      sig.push('فخ اختراق'); S._brk=null; } }
  // 4) زخم أو اتجاه مساند
  if(short?(conf.momentum<=-0.06):(conf.momentum>=0.06)) sig.push('زخم');
  if((short&&r.trend==='down')||(!short&&r.trend==='up')) sig.push('اتجاه');
  // 5) حيتان مع الاتجاه
  const f=tapeFlow();
  if(f.whaleVol>0.2&&((short&&f.whale<-0.3)||(!short&&f.whale>0.3))) sig.push('حيتان');
  // 6) انحراف سيولة لصالحنا
  const dv=cvdDivergence();
  if((short&&dv===-1)||(!short&&dv===1)) sig.push('انحراف سيولة');
  // 7) ضغط أوامر حدّية (OFI) لصالحنا
  const ob=S.orderBook; if(ob&&ob.bids.length){ const medL=medLevelSz(ob)||1, n=(S._ofi||0)/(medL*20);
    if((short&&n<-0.4)||(!short&&n>0.4)) sig.push('ضغط أوامر'); }
  // 8) قائد BTC يساند الاتجاه
  if(S._btcImp!=null&&Math.abs(S._btcImp)>=0.10&&((short&&S._btcImp<0)||(!short&&S._btcImp>0))) sig.push('قائد BTC');
  return sig; }
function effectiveHuntPct(){ const base=Math.max(0.18,S.config.huntPct,
    (profOf()&&profOf().spread?profOf().spread*2.5:0)); // أرضية سبريد: لا صيد بعائد يأكله الفرق السعري
  const mag=Math.abs(S.biasScore);
  const stalled=S.lastWorkAt&&Date.now()-S.lastWorkAt>90000;
  // سقف 1.8 لحاصل المضاعفات — لا تراكب صدمة×ذاكرة×تقلب يرفع العتبة فوق المتناول
  const mult=Math.min(1.8,(S.regime?S.regime.huntMult:1)*memHuntMult()*profStepMult());
  let scaled=base*mult;
  // اتجاه قوي نظيف متوافق مع اتجاه البوت — سهّل الاقتناص لاستغلال الموجة بدل تفويتها
  const rg=S.regime;
  if(rg){ const sh=S.config.direction==='short';
    const aligned=(sh&&rg.trend==='down')||(!sh&&rg.trend==='up');
    if(aligned&&!rg.shock) scaled*=0.8; }
  // قائد BTC يساند اتجاه الصيد على العملات التابعة — سهّل الاقتناص
  if(S._btcImp!=null&&Math.abs(S._btcImp)>=0.10){ const sh2=S.config.direction==='short';
    if((sh2&&S._btcImp<0)||(!sh2&&S._btcImp>0)) scaled*=0.85; }
  // إعادة دخول ذكية: ربح حديث على نفس الاتجاه والموجة مستمرة — اركب الموجة التالية أسرع
  if(S._lastWinClose&&Date.now()-S._lastWinClose.at<180000&&S._lastWinClose.side===S.config.direction) scaled*=0.7;
  if(S.position&&S.position.side!==S.config.direction) return Math.max(0.08,scaled*0.4);
  if(stalled&&huntAligned()) return Math.max(0.12,scaled*0.55);
  if(mag>=28&&huntAligned()) return Math.max(0.14,scaled*0.75);
  return scaled; }
function huntTooClose(p){ const need=Math.max(MIN_HUNT_GAP,addStepPct(),effectiveHuntPct()*0.9);
  // فحص كل دخول صيد فعلي على حدة — متوسط الدفتر المدمج كان يتأخر خلف السعر
  // فتمر دخولات متلاصقة (0.02%!) بدعوى بُعدها عن المتوسط
  for(const g of S.grid){ if(g.origin!=='hunt'||g.reduceOnly||g.status!=='filled') continue;
    const fp=g.filledPrice||g.price;
    if(fp>0&&Math.abs(p-fp)/fp*100<need) return true; }
  return false; }
function openHuntLots(){ // عدّ تنفيذات الصيد الفعلية — الصف المدمج واحد مهما ضمّ أوامر
  return S.grid.filter(g=>g.origin==='hunt'&&!g.reduceOnly&&g.status==='filled').length; }
// إحماء إلزامي: لا قرار دخول قبل 15 ثانية ووصول دفتر أوامر حقيقي وشريط صفقات —
// الدفتر الفارغ كان يجتاز فحص الجودة تلقائيًا فيدخل البوت بعد 3 ثوانٍ من التشغيل أعمى
function warmedUp(){ if(!S.startedAt||Date.now()-S.startedAt<15000) return false;
  const b=S.orderBook||{}; if((b.bids||[]).length<5||(b.asks||[]).length<5) return false;
  if((S.tape||[]).length<20) return false;
  return true; }
// قراءة سجل مع نسيان تدريجي: عمر نصف 10 أيام — ذنب قديم يبهت أثره ولا يطارد للأبد
// ⚠️ ذاكرة التعلم معطّلة بطلب المالك: كانت قيودها المتراكمة تخنق الصفقات —
// تعيد null دائمًا فتسقط كل البوابات (حجم/خطوة/صيد/حظر) إلى وضعها الحر الطبيعي
function memRec(dir){ return null; }
// معدل نجاح مُصحّح بايزيًا — العينة الصغيرة تُشدّ نحو الحياد بدل حكم متسرع
function smWR(r){ return (r.w+1)/(r.n+2); }
// سُلّم العقوبات معطّل مع ذاكرة التعلم — لا قيود متراكمة من الماضي، القرار من السوق الحي فقط
function sanctionLevel(dir){ return 0; }
function toxicDir(dir){ return sanctionLevel(dir)>=2; }
function toxicBlocked(dir){ const lv=sanctionLevel(dir);
  S._probation=false;
  if(lv<3) return false;
  // إعادة تأهيل: كل ساعة صفقة استكشافية بنصف حجم — الحظر بلا إعادة اختبار عمى دائم
  const r=S.memory.pairs[S.config.symbol+':'+dir];
  if(!r.probAt||Date.now()-r.probAt>3600*1000){ r.probAt=Date.now(); S._probation=true;
    pushLog('server','إعادة تأهيل — صفقة استكشافية بنصف حجم تختبر '+(dir==='short'?'الشورت':'اللونغ')+' المحظور');
    saveAll(); return false; }
  if(!S._toxicLogAt||Date.now()-S._toxicLogAt>300000){ S._toxicLogAt=Date.now();
    pushLog('server','اتجاه سام في الذاكرة — '+(dir==='short'?'الشورت':'اللونغ')+' محظور (يُعاد اختباره كل 3 ساعات)'); }
  return true; }

/* ================================================================
   بصمة العملة — كل عملة تُدرس على حدة وتُكيَّف عليها المعاملات:
   تقلبها المقاس يضبط خطوة الشبكة وعتبة الصيد، وتعلّمها يخصها وحدها
   ================================================================ */
function profOf(){ const mem=S.memory; return mem&&mem.profiles?mem.profiles[S.config.symbol]:null; }
function profFresh(){ const p=profOf(); return p&&Date.now()-p.at<24*3600*1000?p:null; }
function startStudy(short){ S.studyUntil=Date.now()+(short?15000:60000);
  S.studyAcc={vols:[],sps:[],act:0,whales:0}; }
function studying(){ return !!(S.studyUntil&&Date.now()<S.studyUntil); }
function studyTick(){ if(!S.studyUntil) return;
  if(Date.now()>=S.studyUntil){ finishStudy(); return; }
  const a=S.studyAcc; if(!a) return;
  const v=retStdev(); if(v>0) a.vols.push(v);
  const b=S.orderBook||{};
  if((b.bids||[]).length&&(b.asks||[]).length){ const bb=b.bids[0].price,aa=b.asks[0].price,mid=(bb+aa)/2;
    if(mid>0&&aa>bb) a.sps.push((aa-bb)/mid*100); }
  a.act=(S.tape||[]).length;
  if(tapeFlow().whaleVol>0.28) a.whales++; }
function finishStudy(){ S.studyUntil=0; const a=S.studyAcc; S.studyAcc=null; if(!a) return;
  const vols=a.vols.slice().sort((x,y)=>x-y);
  const vol=vols.length?vols[Math.floor(vols.length/2)]:0.05;
  const sp=a.sps.length?a.sps.reduce((x,y)=>x+y,0)/a.sps.length:0.02;
  const mem=S.memory=S.memory||{pairs:{},cycles:0,totalPnl:0};
  mem.profiles=mem.profiles||{};
  mem.profiles[S.config.symbol]={at:Date.now(),vol:Math.round(vol*10000)/10000,
    spread:Math.round(sp*10000)/10000,act:a.act,whales:a.whales};
  saveAll();
  pushLog('server','اكتملت دراسة '+S.config.displaySymbol+' — تقلب '+vol.toFixed(3)+'% · سبريد '+
    sp.toFixed(3)+'% · نشاط شريط '+a.act+' — كُيّفت الخطوة والصيد على خصائصها'); }
// تكييف الخطوة مع تقلب العملة المقاس: المتقلبة خطوة أوسع، الهادئة أضيق — بحدود آمنة
function profStepMult(){ const p=profOf(); if(!p||!p.vol) return 1;
  return clamp(p.vol/0.05,0.7,2.2); }
// التعلم يحسّن الدخول ولا يقيّده فقط: اتجاه ناجح لهذه العملة = اقتناص أسهل · خاسر = تشدد
function memHuntMult(){ const lv=sanctionLevel(S.config.direction);
  if(lv>=2) return 1.15; // تشديد طفيف لا خنق — العقوبة القصوى (المنع) للسام مستوى 3 فقط
  const r=memRec(S.config.direction); if(!r||r.n<6) return 1;
  const wr=smWR(r);
  if(wr>=0.6&&r.pnl>0) return 0.85;
  if(lv===1) return 1.1;
  return 1; }
function packHunt(p){ return {side:S.config.direction==='short'?'sell':'buy',qty:contractsForLevel(p)}; }
function huntTrigger(p){ if(S.status!=='running'||!p) return null;
  if(!warmedUp()) return null;
  if(studying()) return null; // العملة قيد الدراسة — لا دخول قبل اكتمال بصمتها
  // هدنة الخسارة المخففة: خسارتان متتاليتان بنفس الاتجاه خلال دقيقتين فقط — لا شلل 5 دقائق لخسارة واحدة
  if(!S.position&&S._lossStreak&&S._lossStreak.side===S.config.direction&&
    S._lossStreak.n>=2&&Date.now()-S._lossStreak.at<120000) return null;
  const flipping=!!(S.position&&S.config.direction!==S.position.side);
  if(addsBlocked()&&!flipping) return null;
  if(flipping){ if(S.lastHuntAt&&Date.now()-S.lastHuntAt<120000) return null;
    // لا انقلاب فوري بعد دخول حديث — انتظر تأكيدًا (عمر المركز أو ضرر حقيقي)
    const ageOk=S.position&&Date.now()-(S.position.openedAt||0)>120000;
    if(!ageOk&&adversePct(p)<0.3) return null;
    if(toxicBlocked(S.config.direction)) return null;
    return packHunt(p); }
  if(toxicBlocked(S.config.direction)) return null;
  if(S.position){ if(!inAddZone(p)) return null; }
  if(S.startedAt&&Date.now()-S.startedAt<SCOUT_MS&&filledAdds()>=1) return null;
  if(S.lastAddAt&&Date.now()-S.lastAddAt<ADD_COOLDOWN) return null;
  trailHuntAnchor(p);
  if(S.lastHuntAt&&Date.now()-S.lastHuntAt<HUNT_COOLDOWN) return null;
  if(openHuntLots()>=MAX_HUNT_OPEN) return null;
  if(huntTooClose(p)) return null;
  if(S.position){ const pk=packHunt(p);
    const cover=(S.position.side==='short'&&pk.side==='buy')||
      (S.position.side==='long'&&pk.side==='sell');
    return cover?null:pk; }
  // فلسفة الصيد: لا دخول إلا بإشارة، ولا منع إلا بفيتو خطر قوي مثبت
  const veto=dangerVeto(p);
  if(veto){ if(!S._vetoAt||Date.now()-S._vetoAt>120000){ S._vetoAt=Date.now();
      pushLog('info','فيتو خطر منع صفقة: '+veto); } return null; }
  const sig=huntSignals(p); if(!sig.length) return null;
  S._lastSig=sig.slice(0,3).join(' · ');
  return packHunt(p); }
function maybeHunt(p){ const hit=huntTrigger(p); if(!hit) return false;
  S.lastHuntAt=Date.now(); S.huntAnchor=p;
  const fee=feeFor(hit.qty*(S.multiplier||1)*p,true); S.feesPaid+=fee;
  const d=applyDelta(hit.side,hit.qty,p);
  // التنفيذ الحقيقي ملك دورة المحرك وحدها (ترسل الأمر قبل الاستدعاء) —
  // إرسال ثانٍ هنا كان يضاعف حجم كل صفقة صيد على المنصة
  if(d.closedQty>0){ const of=S.journal.find(j=>j.status==='open')?.fees||0;
    noteClose(d.closedQty,p,fee,'صفقة',d.realized-of-fee); S.huntOpen=Math.max(0,(S.huntOpen||0)-1); }
  if(d.addedQty>0){ S.huntCount++; S.huntOpen=(S.huntOpen||0)+1;
    const lotId=noteEntry(d.addedQty,p,fee,'hunt');
    S.grid.push({id:uid('hunt'),clientOid:uid('oid'),side:hit.side,price:p,qty:d.addedQty,
      status:'filled',reduceOnly:false,exchangeOrderId:null,filledAt:Date.now(),
      origin:'hunt',createdAt:Date.now(),lotId});
    notify('صفقة فردية #'+S.huntCount+' — '+(hit.side==='sell'?'بيع':'شراء')+' @ '+fmtPx(p)+
      (S._lastSig?' — '+S._lastSig:''));
    S._lastSig=null; }
  if(S.position&&S.status==='running') syncPositionTp();
  S.lastWorkAt=Date.now(); if(d.addedQty>0) S.lastAddAt=Date.now();
  return true; }
function computeLiq(){ const dir=S.position?S.position.side:S.config.direction;
  const pend=S.grid.filter(g=>!g.reduceOnly&&g.status==='open'&&
    (dir==='short'?g.side==='sell':g.side==='buy'));
  const posQ=S.position?S.position.size:0, pendQ=pend.reduce((a,g)=>a+g.qty,0);
  const q=posQ+pendQ; if(q<=0) return null;
  const pendPx=pend.reduce((a,g)=>a+g.price*g.qty,0);
  const entry=posQ>0?(S.position.entry*posQ+pendPx)/q:(pendQ>0?pendPx/pendQ:0);
  if(!entry) return null;
  const mult=S.multiplier||1, lev=Math.max(1,S.position?S.position.leverage:S.config.leverage);
  const mmr=lev>=75?0.025:lev>=50?0.012:lev>=25?0.008:lev>=10?0.005:0.004;
  const liqFee=Math.max(S.takerFee,0.0005);
  const notional=entry*q*mult;
  // هامش معزول حقيقي = قيمة المركز ÷ الرافعة (+ الربح الجاري) — وليس رصيد الدورة كاملًا
  const margin=notional/lev+(S.position?Math.max(0,S.position.unrealized||0):0);
  if(dir==='long'){ const den=q*mult*(1-mmr-liqFee); if(den<=0) return null;
    const lp=(notional-margin)/den; return lp>0?lp:null; }
  const den=q*mult*(1+mmr+liqFee); if(den<=0) return null;
  return (notional+margin)/den; }
function refreshLiq(){ const local=computeLiq();
  if(S.exLiqPrice&&S.exLiqPrice>0&&!S.grid.some(g=>!g.reduceOnly&&g.status==='armed'&&!g.exchangeOrderId)){
    S.liqPrice=S.exLiqPrice; }
  else S.liqPrice=local;
  if(S.position) S.position.liquidation=S.liqPrice; }
function markUnrealized(p){ if(S.position){
  S.position.unrealized=(S.position.side==='short'?1:-1)*
    (S.position.entry-p)*(S.multiplier||1)*S.position.size;
  // MAE/MFE: أقصى انحراف ضد المركز وأقصى تقدم لصالحه (%) طوال عمره
  const fav=(S.position.side==='short'?(S.position.entry-p):(p-S.position.entry))/S.position.entry*100;
  S._exc=S._exc||{mae:0,mfe:0};
  if(fav>=0) S._exc.mfe=Math.max(S._exc.mfe,fav); else S._exc.mae=Math.max(S._exc.mae,-fav); }
  refreshLiq(); }
function liqDanger(p){ const pos=S.position, liq=pos?(pos.liquidation||S.liqPrice):S.liqPrice;
  if(!pos||!liq||!p) return false;
  return pos.side==='long'?p<=liq*1.015:p>=liq*0.985; }
function reversalAgainst(p){ if(!S.position) return false;
  const r=S.regime||{trend:'range'}, mom=S.confluence?S.confluence.momentum:0;
  const adv=adversePct(p), long=S.position.side==='long';
  const tA=long?r.trend==='down':r.trend==='up';
  const mA=long?mom<=-0.1:mom>=0.1;
  const bA=long?S.biasScore<=-8:S.biasScore>=8;
  if(tA&&mA&&bA&&adv>=0.65) return true;
  if(tA&&mA&&adv>=1.4) return true;
  return false; }
function cancelPendingAdds(){ for(const g of S.grid){
  if(!g.reduceOnly&&(g.status==='armed'||g.status==='open')&&!g.filledAt){
    g.status='cancelled'; g.exchangeOrderId=null; } } }
function circuitBreak(p){ if(!p||S.status!=='running') return;
  const limit=Math.max(1.5,(S.config.cycleBalance||0)*0.04);
  const cyc=(S.realizedPnl||0)+(S.position?S.position.unrealized:0)-(S.feesPaid||0);
  if(cyc<=-limit){ flattenAt(p,'كابح الخسارة'); S.status='paused';
    notify('كابح الخسارة: أُغلق المركز وأُوقف البوت مؤقتًا لحماية رأس المال','warn'); } }
function escapeAdverse(p){ if(S.status!=='running'||!S.position||!p) return;
  const danger=liqDanger(p), flip=reversalAgainst(p);
  // موجة سيولة عكسية قوية: حيتان + زخم ضد المركز معًا = انعكاس حقيقي —
  // اقلب فورًا: شبكة تحمي القديم وشبكة تركب الموجة الجديدة (لا إهمال للمركز ولا تفويت للاتجاه)
  const f=tapeFlow(), pos=S.position;
  const mom=S.confluence?S.confluence.momentum:0;
  const whaleRaw=f.whaleVol>0.28&&(
    (pos.side==='short'&&f.whale>0.5&&mom>0.08)||
    (pos.side==='long'&&f.whale<-0.5&&mom<-0.08));
  // طبعة واحدة لا تصنع موجة: اشترط استمرار الإشارة 5 نبضات متتالية —
  // وإلا انقلب البوت على ضجيج عابر وباع القاع واشترى القمة
  S._waveN=whaleRaw?(S._waveN||0)+1:0;
  const posAge=Date.now()-(pos.openedAt||0);
  const adv=adversePct(p);
  const whaleWave=S._waveN>=5&&(posAge>120000||adv>=0.35||(S.regime&&S.regime.shock));
  if(whaleRaw&&!whaleWave&&S._waveN===5&&(!S._waveLogAt||Date.now()-S._waveLogAt>60000)){
    S._waveLogAt=Date.now(); pushLog('server','موجة حيتان مبكرة — تُراقب ولا انقلاب (مركز حديث بلا ضرر حقيقي)'); }
  if(!danger&&!flip&&!whaleWave) return;
  const cooled=S._flipAt&&Date.now()-S._flipAt<45000;
  if(cooled&&!danger) return;
  const was=S.position.side, nextSide=was==='long'?'short':'long';
  const mode=S.config.directionMode;
  if(danger||mode!=='auto'){
    flattenAt(p,'مركز');
    if(mode==='auto'){ S.config.direction=nextSide;
      S.grid=[...S.grid.filter(g=>g.reduceOnly&&(g.status==='open'||g.status==='armed')),
        ...buildGrid(p,true).map(g=>({...g,lane:'trend'}))];
      S.huntAnchor=p; }
    S._flipAt=Date.now(); S.gridAnchor=p;
    pushLog('server','قرب التصفية — أُغلق '+(was==='long'?'اللونغ':'الشورت')+' ودخل '+
      (nextSide==='short'?'شورت':'لونغ')+' بشبكة واسعة');
    return; }
  cancelPendingAdds();
  S.config.direction=was;
  const hold=buildGrid(p,true).map(g=>({...g,lane:'hold'}));
  S.config.direction=nextSide;
  const trend=buildGrid(p,true).map(g=>({...g,lane:'trend'}));
  S.grid=[...S.grid.filter(g=>g.status==='filled'||(g.reduceOnly&&(g.status==='open'||g.status==='armed'))),
    ...hold,...trend].sort((a,b)=>b.price-a.price);
  S.huntAnchor=p; S._flipAt=Date.now(); S.gridAnchor=p;
  pushLog('server','تغيّر الاتجاه — دخول '+(nextSide==='short'?'شورت':'لونغ')+
    ' فوراً بشبكتين واسعتين'); }
function ensureGrid(){ if(S.status!=='running') return;
  // تنظيف: آلاف الصفوف الملغاة القديمة تُفرز وتُفحص كل نبضة — احتفظ بآخر 100 فقط
  if(S.grid.length>400){ let dead=0;
    S.grid=S.grid.filter(g=>{ if(g.status==='armed'||g.status==='open'||g.status==='filled') return true;
      dead++; return dead<=100; }); }
  const center=S.lastPrice||S.gridAnchor||(S.position?S.position.entry:0);
  if(!center) return;
  sanitizeAdds();
  const dual=S.grid.some(g=>g.lane==='hold'&&(g.status==='armed'||g.status==='open'))&&
    S.grid.some(g=>g.lane==='trend'&&(g.status==='armed'||g.status==='open'));
  if(dual){ if(S.position) syncPositionTp(); return; }
  if(filledAdds()>=effLevels()){ if(S.position) syncPositionTp(); return; }
  const sameSide=!!(S.position&&S.position.side===S.config.direction);
  const short=S.position&&S.position.side==='short';
  const chasing=!!(sameSide&&S.gridAnchor)&&
    ((short&&center>S.gridAnchor)||(!short&&center<S.gridAnchor));
  const jump=lastJumpPct();
  const live=S.grid.filter(g=>!g.reduceOnly&&(g.status==='armed'||g.status==='open'));
  const drift=S.gridAnchor?Math.abs(center-S.gridAnchor)/S.gridAnchor:1;
  const empty=live.length===0, away=drift>S.config.gridStepPct/100*2.2;
  if(S.regime&&S.regime.shock&&!empty&&S.position){
    cancelPendingAdds(); if(S.position) syncPositionTp(); return; }
  const mom=S.confluence?S.confluence.momentum:0;
  const against=!!S.position&&((S.position.side==='long'&&S.regime.trend==='down'&&mom<-0.06)||
    (S.position.side==='short'&&S.regime.trend==='up'&&mom>0.06));
  if(against){ cancelPendingAdds(); if(S.position) syncPositionTp(); return; }
  if(chasing&&jump>0.18) return;
  if(!empty&&!away&&jump<0.12) return;
  if(chasing){ const paused=jump>0.02&&jump<0.16;
    if(!(paused&&inAddZone(center))) return; }
  if(S.position&&!inAddZone(center)&&!empty){ if(S.position) syncPositionTp(); return; }
  cancelPendingAdds();
  const keep=S.grid.filter(g=>g.status==='filled'||g.status==='cancelled'||
    (g.reduceOnly&&(g.status==='open'||g.status==='armed')));
  S.grid=[...keep,...buildGrid(center)].filter(g=>
    keep.includes(g)||g.reduceOnly||g.status!=='armed'||!shouldFill(g,center));
  S.gridAnchor=center; S.grid.sort((a,b)=>b.price-a.price); }
function flattenAt(p,source){ const pos=S.position;
  // إغلاق حقيقي على المنصة أولًا — لا إغلاق محلي صوري في الوضع الحقيقي
  if(pos&&S.config.mode==='live'&&S.keys){
    exCancelAll(S.config.symbol).catch(()=>{});
    exCancelStops(S.config.symbol).catch(()=>{}); S._guardId=null; // أوامر الوقف قناة منفصلة — تُلغى صراحة
    exCloseQty(S.config.symbol,pos.side,pos.size)
      .catch(e=>pushLog('error','الإغلاق الحقيقي فشل: '+(e.message||e))); }
  if(pos&&p>0){ const cs=pos.side==='short'?'buy':'sell';
    const fee=feeFor(pos.size*(S.multiplier||1)*p,true); S.feesPaid+=fee;
    const d=applyDelta(cs,pos.size,p);
    if(d.closedQty>0){ const of=S.journal.find(j=>j.status==='open')?.fees||0;
      noteClose(d.closedQty,p,fee,source,d.realized-of-fee); } }
  for(const row of S.journal){ if(row.status==='open'){ row.status='closed';
    row.exit=p; row.closedAt=Date.now(); if(source==='إيقاف') row.source='إيقاف';
    if(!row.pnl) row.pnl=lotNet(row.side,row.entry,p,row.qty,row.fees); } }
  S.position=null; S.huntOpen=0;
  for(const l of S.grid) if(l.status==='open'||l.status==='armed'){
    l.status='cancelled'; l.exchangeOrderId=null; } }
function pruneGhosts(){ if(S.position) return;
  S.huntOpen=0;
  for(const g of S.grid){ if(g.reduceOnly&&(g.status==='open'||g.status==='armed')){
      g.status='cancelled'; g.exchangeOrderId=null; }
    if(g.status==='filled'&&!g.reduceOnly) g.status='cancelled'; }
  for(const row of S.journal){ if(row.status!=='open') continue;
    row.status='closed'; row.exit=S.lastPrice||row.entry; row.closedAt=Date.now();
    if(!row.pnl&&row.exit) row.pnl=lotNet(row.side,row.entry,row.exit,row.qty,row.fees); } }

/* ================================================================
   تدفق السوق — WebSocket مستمر + REST احتياطي
   ================================================================ */
function applyMeta(m){ S.lastPrice=m.price; S.markPrice=m.markPrice||m.price;
  if(m.makerFee)S.makerFee=m.makerFee; if(m.takerFee)S.takerFee=m.takerFee;
  if(m.funding!=null)S.funding=m.funding; if(m.multiplier)S.multiplier=m.multiplier;
  if(m.tickSize)S.tickSize=m.tickSize; S.lastTickAt=Date.now();
  if(m.orderBook)S.orderBook=m.orderBook;
  if(m.tape){ S.tape=m.tape;
    for(const t of m.tape) S.cvd+=t.side==='buy'?t.size:-t.size; }
  if(m.price>0){ S.priceTrail=[...S.priceTrail,m.price].slice(-120);
    S.emaFast=nextEma(S.emaFast,m.price,2/10);
    S.emaSlow=nextEma(S.emaSlow,m.price,2/22); }
  S.confluence=readConfluence(); S.regime=readRegime(); }

// رسائل القناة اللحظية — تصل دفعًا فور وقوعها (أجزاء من الثانية)
function onStreamTick(d){
  if(d.symbol&&d.symbol!==S.config.symbol) return; // رسالة زوج قديم — تجاهل
  const now=Date.now();
  if(d.price>0){
    S.lastPrice=d.price;
    S.lastTickAt=now;
    if(!S._lastTrailAt||now-S._lastTrailAt>400){
      S.priceTrail=[...S.priceTrail,d.price].slice(-120);
      S.emaFast=nextEma(S.emaFast,d.price,2/10);
      S.emaSlow=nextEma(S.emaSlow,d.price,2/22);
      S._lastTrailAt=now;
      // قمة/قاع نافذة المسار — تسجيل لحظة الاختراق لمحفّز صيد السلاحف (الاختراق الكاذب)
      const tt=S.priceTrail;
      if(tt.length>=20){ let hi=-Infinity,lo=Infinity;
        for(let i=0;i<tt.length-1;i++){ if(tt[i]>hi)hi=tt[i]; if(tt[i]<lo)lo=tt[i]; }
        if(hi>-Infinity&&d.price>hi) S._brk={side:'up',at:now,level:hi};
        else if(lo<Infinity&&d.price<lo) S._brk={side:'down',at:now,level:lo}; } }
    // عينة نشاط السوق كل دقيقة: معدل الصفقات الفعلي في آخر 60 ثانية (طول الشريط مخزّن مشبع لا يقيس شيئًا)
    if(!S._actAt||now-S._actAt>60000){ S._actAt=now;
      const cut=now-60000, rate=(S._tradeTs||[]).filter(t=>t>=cut).length;
      S._actHist=[...(S._actHist||[]),rate].slice(-30);
      const h=[...S._actHist].sort((a,b)=>a-b); S._actMed=h[Math.floor(h.length/2)]||0; }
    // تنفيذ فوري لجني الربح مع كل نبضة سعر — لا انتظار لدورة المحرك
    if(S.status==='running'){ try{ harvestRipe(d.price); }catch(e){} }
  }
  if(d.book){
    // تدفق الأوامر (OFI): ميزان ضغط أفضل عرض/طلب بين اللقطات — راكم 20 عينة أخيرة
    const b0=d.book.bids&&d.book.bids[0], a0=d.book.asks&&d.book.asks[0];
    const pb=S._prevBook;
    if(pb&&pb.b&&pb.a&&b0&&a0){ let ofi=0;
      if(b0.price>pb.b.price) ofi+=b0.size;
      else if(b0.price<pb.b.price) ofi-=pb.b.size;
      else ofi+=b0.size-pb.b.size;
      if(a0.price<pb.a.price) ofi+=a0.size;
      else if(a0.price>pb.a.price) ofi-=pb.a.size;
      else ofi-=a0.size-pb.a.size;
      S._ofiWin=[...(S._ofiWin||[]),ofi].slice(-20);
      S._ofi=S._ofiWin.reduce((x,y)=>x+y,0); }
    S._prevBook={b:b0?{price:b0.price,size:b0.size}:null,a:a0?{price:a0.price,size:a0.size}:null};
    S.orderBook=d.book; }
  if(d.trade&&d.trade.price>0){
    S.lastPrice=d.trade.price; S.lastTickAt=now;
    S.tape=[d.trade,...S.tape].slice(0,32);
    S._tradeTs=[...(S._tradeTs||[]),now].slice(-300); // طوابع زمنية — لقياس معدل الصفقات الحقيقي
    S.cvd+=d.trade.side==='buy'?d.trade.size:-d.trade.size;
    // مسار CVD — لكشف انحراف الحيتان (السعر عكس التدفق)
    if(!S._cvdAt||now-S._cvdAt>1500){ S.cvdTrail=[...(S.cvdTrail||[]),{t:now,cvd:S.cvd}].slice(-90); S._cvdAt=now; } }
  S.confluence=readConfluence(); S.regime=readRegime();
  emitThrottled();
}

async function loadMarket(full){
  const c=S.config.symbol;
  const wsOk=Date.now()-(streamState.lastMsgAt||0)<6000;
  let price=S.lastPrice||0, mark=S.markPrice||price;
  if(!wsOk){ const t=await fetchTicker(c); price=+t.price||price; }
  if(!S._metaAt||Date.now()-S._metaAt>60000){
    const ct=await fetchContract(c); S._metaAt=Date.now();
    mark=+ct.markPrice||mark;
    S.makerFee=normFee(ct.makerFeeRate??ct.makerFeeCoefficient,S.makerFee);
    S.takerFee=normFee(ct.takerFeeRate??ct.takerFeeCoefficient,S.takerFee);
    S.funding=+ct.fundingFeeRate||0;
    S.multiplier=+ct.multiplier||1;
    S.tickSize=+ct.tickSize||0.1;
    if(!price) price=mark;
  }
  const m={price,markPrice:mark};
  const bookStale=Date.now()-(streamState.bookAt||0)>8000;
  const tapeStale=Date.now()-(streamState.tradeAt||0)>8000;
  if(full&&(bookStale||tapeStale||!S.tape.length)){
    const [ob,tp]=await Promise.all([fetchBook(c),fetchTape(c)]);
    if(ob.bids.length||ob.asks.length) m.orderBook=ob;
    if(tp.length&&tapeStale) m.tape=tp;
  }
  // قائد BTC: نبضة المؤشر توجّه صيد العملات التابعة — قراءة REST خفيفة كل دورة كاملة
  if(full&&c!=='XBTUSDTM'){ try{ const bt=await fetchTicker('XBTUSDTM'); const bp=+bt.price||0;
    if(bp>0){ const pv=S._btcPrev;
      if(pv&&pv.px>0){ const dtMin=Math.max(0.05,(Date.now()-pv.at)/60000);
        S._btcImp=Math.round(((bp-pv.px)/pv.px*100)/dtMin*10000)/10000; } // تغير % لكل دقيقة
      S._btcPrev={px:bp,at:Date.now()}; } }catch(e){} }
  return m;
}

async function liveSync(){ if(S.config.mode!=='live'||!S.keys) return;
  const sym=S.config.symbol;
  try{
    const pos=await exPosition(sym);
    if(pos){ if(S.ignoreExchangeUntil&&Date.now()<S.ignoreExchangeUntil&&!S.position){}
      else if(!S.position){ S.position=pos; pushLog('recover','استُعيد مركز '+pos.side+' من المنصة'); }
      else { S.position.size=pos.size; S.position.entry=pos.entry;
        S.position.unrealized=pos.unrealized; S.position.side=pos.side;
        if(pos.liquidation) S.position.liquidation=pos.liquidation; } }
    else if(S.position&&Date.now()>S.ignoreExchangeUntil){ S.position=null; S._guardPx=0; S._guardId=null; }
    if(pos&&pos.liquidation) S.exLiqPrice=pos.liquidation;
    if(S.position) exPlaceStopGuard(); // إيقاف طوارئ على المنصة يحمي المركز حتى لو نام التطبيق
  }catch(e){ pushLog('error','قراءة المركز: '+(e.message||e)); }
  try{
    const exs=await exOrders(sym);
    const byOid=new Map(exs.map(o=>[o.clientOid||'',o]));
    const byId=new Map(exs.map(o=>[o.orderId,o]));
    for(const l of S.grid){ if(l.status==='cancelled'||l.status==='filled') continue;
      const hit=(l.clientOid&&byOid.get(l.clientOid))||
      (l.exchangeOrderId&&byId.get(l.exchangeOrderId));
      if(hit){ l.status='open'; l.exchangeOrderId=hit.orderId; }
      else if(l.status==='open'&&!shouldFill(l,S.lastPrice||0)){
        l.status='armed'; l.exchangeOrderId=null; } }
    // أي مستوى أُلغي أو نُفّذ محليًا وله أمر حي على المنصة — ألغِه هناك فورًا
    for(const l of S.grid){ if((l.status==='cancelled'||l.status==='filled')&&l.exchangeOrderId){
      exCancelOne(l.exchangeOrderId); l.exchangeOrderId=null; } }
    // أوامر المنصة غير المعروفة محليًا: تُستورد فقط عند إقلاع بلا شبكة (استعادة بعد إعادة تشغيل)
    // — وإلا فهي أوامر يتيمة قديمة يحذفها خط التنظيف أدناه بدل إحيائها
    const hasLocalLive=S.grid.some(l=>(l.status==='armed'||l.status==='open'));
    for(const o of exs){ if(S.grid.some(l=>l.exchangeOrderId===o.orderId||
      (o.clientOid&&l.clientOid===o.clientOid))) continue;
      if(hasLocalLive) continue;
      S.grid.push({id:uid('ex'),clientOid:o.clientOid||uid('oid'),side:o.side,
        price:o.price,qty:o.size,status:'open',reduceOnly:o.reduceOnly,
        exchangeOrderId:o.orderId,filledAt:null,origin:'grid'}); }
    const want=S.grid.filter(l=>(l.status==='armed'));
    if(!S.permDenied) for(const l of want){ if(l.exchangeOrderId) continue;
      try{ const r=await exPlaceLimit({clientOid:l.clientOid,symbol:sym,side:l.side,
        price:l.price,qty:l.qty,reduceOnly:l.reduceOnly,leverage:S.config.leverage});
        l.status='open'; l.exchangeOrderId=r.orderId;
      }catch(e){ const m=e.message||String(e);
        if(/access denied|permission/i.test(m)){ if(!S.permDenied){ S.permDenied=true;
          pushLog('error','⛔ المفتاح يقرأ الرصيد لكن بلا صلاحية تداول — من KuCoin: إدارة API ← تعديل المفتاح ← فعّل «التداول» للعقود الآجلة ثم احفظ المفاتيح مجددًا');
          toast('⚠️ فعّل صلاحية التداول في مفتاح KuCoin'); } break; }
        // كبح تكرار نفس الخطأ — مرة كل دقيقة كافية
        if(!window.__lastOrdErrAt||Date.now()-window.__lastOrdErrAt>60000){
          window.__lastOrdErrAt=Date.now();
          pushLog('error','أمر '+l.side+' @ '+fmtPx(l.price)+': '+m); } } }
    const liveIds=new Set(S.grid.filter(l=>l.exchangeOrderId).map(l=>l.exchangeOrderId));
    for(const o of exs){ if(!liveIds.has(o.orderId)){
      try{ await kcPrivate('DELETE','/api/v1/orders/'+o.orderId); }catch(e){} } }
  }catch(e){ pushLog('error','مزامنة الأوامر: '+(e.message||e)); } }
async function liveFlattenIfNeeded(prevSide){ if(S.config.mode!=='live'||!S.keys||!prevSide) return;
  try{ const pos=await exPosition(S.config.symbol);
    if(pos){ await exCancelAll(S.config.symbol); await exClose(S.config.symbol,pos.side);
      pushLog('server','أُغلق المركز الحقيقي على المنصة'); } }catch(e){} }

async function botTick(){
  const running=S.status==='running', paused=S.status==='paused';
  if(!running&&!paused&&S.status!=='idle') return;
  try{
    const full=S.heartbeat%5===0||!S.tape.length;
    // القرارات تعتمد سعر القناة اللحظي؛ REST للإثراء فقط — فشله لا يوقف التنفيذ أبدًا
    let m={price:S.lastPrice||S.markPrice||0,markPrice:S.markPrice||S.lastPrice||0};
    try{ m=await loadMarket(full); }catch(e){}
    const price=S.lastPrice||m.price||0;
    if(S.status==='idle'){ applyMeta(m); emit(); saveAll(); return; }
    S.heartbeat++;
    applyMeta(m);
    studyTick(); // جمع عينات بصمة العملة أثناء الدراسة (ويُنهيها عند اكتمالها)
    resolveDirection(Date.now());
    if(running&&price>0){
      circuitBreak(price);
      escapeAdverse(price);
      harvestRipe(price);
      const due=S.grid.filter(l=>shouldFill(l,price));
      for(const l of selectDueAdds(due,price)){
        const before=filledAdds(); fillLevel(l.id,l.price,false);
        if(filledAdds()>before) S.lastAddAt=Date.now(); }
      const hunt=huntTrigger(price);
      if(hunt&&!S.permDenied){ let done=false;
        if(S.config.mode==='live'&&S.keys){
          try{ await exPlaceMarket(S.config.symbol,hunt.side,hunt.qty); done=true; }
          catch(e){ const m=e.message||String(e);
            if(/access denied|permission/i.test(m)){ S.permDenied=true;
              pushLog('error','⛔ المفتاح بلا صلاحية تداول — فعّل «التداول» للعقود الآجلة في KuCoin ثم احفظ المفاتيح مجددًا');
              toast('⚠️ فعّل صلاحية التداول في مفتاح KuCoin'); }
            else pushLog('error','فشل صيد السوق: '+m); } }
        else done=true;
        if(done) maybeHunt(price); }
      ensureGrid(); harvestRipe(price);
    }
    pruneGhosts(); markUnrealized(price||m.price);
    // مزامنة المنصة كل 5 ثوانٍ تكفي — كل ثانية كانت تستنزف حصة الطلبات والبطارية
    if(!S._liveSyncAt||Date.now()-S._liveSyncAt>5000){ S._liveSyncAt=Date.now();
      try{ await liveSync(); }catch(e){} }
    try{ maybeReport(); }catch(e){}
    emit(); saveAll();
  }catch(e){ S.lastTickAt=Date.now();
    if(!/abort|timeout|429|50[0-4]|fetch/i.test(e.message||'')) pushLog('error',e.message||String(e)); }
}

/* ---------- أوامر التشغيل ---------- */
export function startBot(){ if(S.status==='running') return;
  if(S.config.mode==='live'&&!S.keys){ toast('اربط مفاتيح KuCoin أولاً من الإعدادات'); return; }
  const p=S.lastPrice||S.markPrice;
  if(!p){ toast('تعذر قراءة السعر — تحقق من الاتصال والزوج'); return; }
  if(S.status==='paused'&&S.position){ S.status='running'; pushLog('info','استئناف البوت'); emit(); saveAll(); return; }
  S.status='running'; S.startedAt=Date.now(); S.heartbeat=0;
  // عملة معروفة البصمة = إحماء قصير · جديدة = دراسة 60 ثانية قبل أي دخول
  const pf=profFresh(); startStudy(!!pf);
  if(!pf) pushLog('server','دراسة '+S.config.displaySymbol+' 60 ثانية — قياس التقلب والسبريد والنشاط قبل أي دخول');
  S.huntAnchor=p; S.gridAnchor=S.position?S.position.entry:p; S._flipAt=null;
  resolveDirection(Date.now());
  if(!S.grid.filter(g=>!g.reduceOnly&&(g.status==='armed'||g.status==='open')).length){
    S.grid=[...S.grid.filter(g=>g.reduceOnly&&(g.status==='open'||g.status==='armed')),
      ...buildGrid(S.position?S.position.entry:p)]; }
  pushLog('server','دورة جديدة '+(S.config.direction==='short'?'شورت':'لونغ')+
    (S.regime?' · '+S.regime.label:'')+' — شبكة فعّالة '+effLevels()+' من '+S.config.levels+
    ' (تركيز يناسب رأس المال) والصيد يعمل');
  nativeNotify('TRQ يعمل ✓','البوت متصل بـ KuCoin ويتداول '+(S.config.displaySymbol||S.config.symbol)+' — يستمر حتى في الخلفية',true);
  toast('البوت يعمل الآن'); emit(); saveAll(); }
export function pauseBot(){ if(S.status!=='running') return;
  S.status='paused';
  for(const l of S.grid){ if(!l.reduceOnly&&(l.status==='open'||l.status==='armed')){
    l.status='cancelled'; l.exchangeOrderId=null; }
    else if(l.reduceOnly&&l.status==='open'&&S.config.mode==='live'){
      l.status='armed'; l.exchangeOrderId=null; } }
  if(S.config.mode==='live'&&S.keys) exCancelAll(S.config.symbol).catch(()=>{});
  pushLog('info','إيقاف مؤقت — لا صفقات جديدة، الجني مستمر');
  emit(); saveAll(); }
export async function stopBot(){ if(S.status==='idle') return;
  const p=S.lastPrice||S.markPrice||0, prevSide=S.position?S.position.side:null;
  if(S.config.mode==='live'&&S.keys&&prevSide){
    try{ await exCancelAll(S.config.symbol); await exCancelStops(S.config.symbol); S._guardId=null;
      await exClose(S.config.symbol,prevSide);}catch(e){} }
  flattenAt(p,'إيقاف'); S.status='stopped';
  clearNativeOngoing();
  pushLog('info','إيقاف — أُغلقت كل الصفقات عند السعر الحالي');
  toast('تم إيقاف البوت'); emit(); saveAll(); }
export function newCycle(){ const rolled=Math.max(0.01,
    Math.round((S.config.cycleBalance+S.realizedPnl-S.feesPaid)*100)/100);
  const keep=S.status==='running'||S.status==='paused';
  if(S.config.mode==='live'&&S.keys){ exCancelAll(S.config.symbol).catch(()=>{});
    exCancelStops(S.config.symbol).catch(()=>{}); S._guardId=null;
    // لا تيتيم لمراكز حقيقية: دورة جديدة بمركز مفتوح تُغلقه على المنصة أولًا
    if(S.position) exCloseQty(S.config.symbol,S.position.side,S.position.size)
      .catch(e=>pushLog('error','إغلاق مركز الدورة السابقة فشل: '+(e.message||e))); }
  // نافذة تجاهل 12 ثانية: لا تستعد المركز المغلق للتو كـ«شبح» قبل أن تسوّيه المنصة
  S.ignoreExchangeUntil=Date.now()+12000;
  // قبل المسح: تُحسب حصيلة الدورة الجديدة
  const cfg={...S.config,cycleBalance:rolled};
  const keys=S.keys, snd=S.sound, tone=S.soundTone;
  // مسح كامل: سجل الصفقات + الأرشيف + سجل السيرفر — بداية نظيفة كل دورة
  Object.assign(S,{grid:[],position:null,journal:[],history:[],logs:[],realizedPnl:0,feesPaid:0,
    activeCycle:null,huntCount:0,huntOpen:0,priceTrail:[],emaFast:null,emaSlow:null,
    cvd:0,tape:[],orderBook:{bids:[],asks:[]},status:'idle',startedAt:null,
    huntAnchor:null,gridAnchor:null,liqPrice:null,exLiqPrice:null,_memBlockAt:0});
  S.config=cfg; S.keys=keys; S.sound=snd; S.soundTone=tone;
  pushLog('info','دورة جديدة برصيد $'+rolled.toFixed(2)+' — مُسحت السجلات وبدأت صفحة نظيفة');
  toast('دورة جديدة برصيد $'+rolled.toFixed(2));
  emit(); saveAll(); if(keep) startBot(); }

/* ---------- إجراءات الإعدادات ---------- */
export async function saveKeys(k){
  if(!k.apiKey||!k.apiSecret||!k.passphrase){ toast('أدخل المفتاح والسر والعبارة'); return false; }
  S.keys=k; S.linkOk=false; saveAll(); emit();
  try{ const eq=await exPing();
    S.linkOk=true; S.exEquity=eq; S.permDenied=false;
    toast('تم الربط ✓ الرصيد $'+(eq!=null?eq.toFixed(2):'—'));
  }catch(e){ S.linkOk=false; S.exEquity=null;
    toast('حُفظت المفاتيح لكن فشل الاتصال: '+(e.message||e)); }
  emit(); return S.linkOk;
}
export function clearKeys(){ S.keys=null; S.linkOk=false; S.exEquity=null; saveAll(); emit(); toast('حُذفت المفاتيح'); }
export function saveCfg(v){
  const old=S.config.symbol;
  const oldLv=S.config.leverage, oldN=S.config.levels,
    oldStep=S.config.gridStepPct, oldBal=S.config.cycleBalance;
  const sym=(v.symbol||old).trim().toUpperCase();
  S.config.symbol=sym;
  S.config.displaySymbol=sym.replace(/USDTM$/i,'').replace(/^XBT$/i,'BTC');
  S.config.leverage=clamp(Math.round(+v.leverage||3),1,100);
  S.config.levels=clamp(Math.round(+v.levels||12),1,64);
  S.config.gridStepPct=clamp(+v.gridStepPct||0.3,0.05,5);
  S.config.huntPct=clamp(+v.huntPct||0.15,0.03,5);
  S.config.cycleBalance=Math.max(1,+v.cycleBalance||100);
  S.config.directionMode=v.directionMode;
  S.config.mode=v.mode;
  S.sound=!!v.sound;
  if(v.soundTone) S.soundTone=v.soundTone;
  if(sym!==old){
    // الوضع الحقيقي: ألغِ أوامر الزوج السابق وأوقفه وأغلق مركزه قبل الانتقال — لا أوامر يتيمة بلا رقيب
    if(S.config.mode==='live'&&S.keys){ exCancelAll(old).catch(()=>{});
      exCancelStops(old).catch(()=>{});
      if(S.position) exCloseQty(old,S.position.side,S.position.size)
        .catch(e=>pushLog('error','إغلاق مركز الزوج السابق فشل: '+(e.message||e)));
      pushLog('server','أُلغيت أوامر ووقف '+old+' وأُغلق مركزه قبل الانتقال'); }
    S.grid=[]; S.position=null; S.journal=[];
    S.realizedPnl=0; S.feesPaid=0; S.ignoreExchangeUntil=Date.now()+12000;
    S.tape=[]; S.priceTrail=[]; S.emaFast=S.emaSlow=null; S._metaAt=0;
    S.lastPrice=null; S.markPrice=null; S.lastTickAt=null; S.cvd=0;
    S.orderBook={bids:[],asks:[]}; S.biasScore=0; S.biasReasons=[];
    S.regime=null; S.confluence=null; S.heartbeat=0;
    S.liqPrice=null; S.exLiqPrice=null; S.huntAnchor=null; S.gridAnchor=null;
    // تصفير حالة الإشارات الجديدة — كل عملة تُقرأ من صفر بمعطياتها وحدها
    S._brk=null; S._ofi=0; S._ofiWin=[]; S._prevBook=null;
    S._btcImp=null; S._btcPrev=null; S._actHist=[]; S._actMed=0; S._actAt=0; S._tradeTs=[];
    S._lossStreak=null; S._lastSig=null; S._vetoAt=0; S._guardPx=0; S._guardLogged=false; S._guardId=null;
    setStreamSymbol(sym);
    // بصمة العملة الجديدة: معروفة وحديثة = إحماء قصير · جديدة = دراسة كاملة قبل التداول
    const pf=S.memory&&S.memory.profiles&&S.memory.profiles[sym];
    startStudy(!!(pf&&Date.now()-pf.at<24*3600*1000));
    pushLog('server','انتقال إلى '+S.config.displaySymbol+' — أُلغيت حالة الزوج السابق'+
      (pf&&Date.now()-pf.at<24*3600*1000?' · بصمتها معروفة — إحماء قصير':' · تُدرس 60 ثانية قبل أي دخول')); }
  else if(S.status==='running'&&
    (oldLv!==S.config.leverage||oldN!==S.config.levels||
     oldStep!==S.config.gridStepPct||oldBal!==S.config.cycleBalance)){
    // تغيّر رأس المال/الرافعة/المستويات/الخطوة أثناء التشغيل: أعد بناء الشبكة فورًا
    // بالأحجام والعدد الفعّال المحسوبين من القيم الجديدة — لا تبقَ شبكة بمعاملات قديمة
    cancelPendingAdds();
    if(S.config.mode==='live'&&S.keys) exCancelAll(sym).catch(()=>{});
    const c=S.lastPrice||S.gridAnchor||(S.position?S.position.entry:0);
    if(c>0){ const keep=S.grid.filter(g=>g.status==='filled'||g.status==='cancelled'||
        (g.reduceOnly&&(g.status==='open'||g.status==='armed')));
      S.grid=[...keep,...buildGrid(c)]; S.gridAnchor=c;
      S.grid.sort((a,b)=>b.price-a.price); }
    pushLog('server','أُعيد بناء الشبكة بالمعاملات الجديدة — '+effLevels()+' مستوى فعّال'); }
  saveAll(); emit(); toast('تم حفظ المعاملات');
}

/* ---------- مساعدات العرض ---------- */
export function equity(){ return S.config.cycleBalance+S.realizedPnl+
  (S.position?S.position.unrealized:0)-S.feesPaid; }
export function desk(){ const j=S.journal;
  const closed=j.filter(r=>r.status!=='open'), open=j.filter(r=>r.status==='open');
  return {win:closed.filter(r=>r.pnl>0), lose:closed.filter(r=>r.pnl<0),
    open, winPnl:closed.filter(r=>r.pnl>0).reduce((a,r)=>a+r.pnl,0),
    losePnl:closed.filter(r=>r.pnl<0).reduce((a,r)=>a+r.pnl,0),
    fees:S.feesPaid}; }
export function pulseAge(){ if(!S.lastTickAt) return 'لا نبض';
  const s=Math.max(0,Math.round((Date.now()-S.lastTickAt)/1000));
  return s<5?'الآن':s<60?s+' ث':'قبل '+Math.floor(s/60)+' د'; }
export { fmtPx, fmtUsd, fmtTime };

/* ---------- الإقلاع ---------- */
let booted=false;
export function initEngine(){
  if(booted) return; booted=true;
  if(typeof window!=='undefined') window.__S=S; // للفحص والتشخيص
  loadAll();
  if(S.config.symbol==='BTCUSDTM') S.config.symbol='XBTUSDTM'; // الرمز الصحيح في عقود KuCoin
  S.config.displaySymbol=S.config.symbol.replace(/USDTM$/i,'').replace(/^XBT$/i,'BTC');
  pushLog('info','TRQ Trading جاهز — '+(S.config.mode==='live'?'وضع LIVE':'وضع ورقي'));
  // قناة الدفع المستمرة — تتصل فورًا وتعيد الاتصال تلقائيًا عند أي انقطاع
  startStream(S.config.symbol,{ onMessage:onStreamTick, onStatus:()=>emitThrottled() });
  setInterval(botTick,TICK_MS);
  setInterval(emit,1000);
  setInterval(()=>{ if(S.status==='idle') botTick().catch(()=>{}); },6000);
  // تحديث رصيد المنصة باستمرار عند وجود مفاتيح — يظهر في الإعدادات فورًا
  const refreshBal=()=>{ if(!S.keys) return;
    exPing().then(eq=>{ if(eq!=null){ S.exEquity=eq; S.linkOk=true; } })
      .catch(()=>{ S.linkOk=false; }); };
  refreshBal(); setInterval(refreshBal,20000);
  // رادار الفرص: مسح شامل كل 3 دقائق — أول قراءة بعد استقرار القناة
  setTimeout(()=>{ scanRadar().catch(()=>{}); },22000);
  setInterval(()=>{ scanRadar().catch(()=>{}); },180000);
  // البقاء الصوتي الصامت: يعمل دائمًا على الجوال حتى لا يُخنق المحرك بإطفاء الشاشة
  startSilentKeepAlive();
  // خدمة أمامية + استثناء البطارية — بقاء حقيقي في الخلفية لا يعتمد على الحيل الصوتية وحدها
  startForegroundSvc();
  // قفل المعالج + منعّاش أصلي: لا نوم للمؤقتات، وإنعاش تلقائي إن جمّد النظام الويب فيو
  startNativeKeepAlive();
  logRevival();
  // عند العودة من الخلفية: دورة محرك فورية لتعويض أي فترة خنق + إعادة فحص الأرباح الناضجة
  if(typeof document!=='undefined') document.addEventListener('visibilitychange',()=>{
    if(!document.hidden){ botTick().catch(()=>{});
      if(S.lastPrice) try{ harvestRipe(S.lastPrice); }catch(e){} } });
  emit();
}
