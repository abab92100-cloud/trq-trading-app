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
function beep(){ try{ const c=new (window.AudioContext||window.webkitAudioContext)();
  const o=c.createOscillator(),g=c.createGain(); o.type='sine'; o.frequency.value=880;
  g.gain.value=.06; o.connect(g); g.connect(c.destination); o.start();
  o.stop(c.currentTime+.15);}catch(e){} }
function notify(txt){ if(S.sound){ beep(); try{navigator.vibrate&&navigator.vibrate(120);}catch(e){} }
  pushLog('fill',txt); }

/* ---------- مخزن الحالة + إشعارات الواجهة ---------- */
const listeners=new Set();
let emitTimer=null;
export function subscribe(fn){ listeners.add(fn); return ()=>listeners.delete(fn); }
export function emit(){ for(const fn of [...listeners]) fn(); }
function emitThrottled(){
  if(emitTimer) return;
  emitTimer=setTimeout(()=>{ emitTimer=null; emit(); },300);
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
  ignoreExchangeUntil:0, lastTickAt:null, sound:true,
  toastMsg:null, _metaAt:0, _lastTrailAt:0,
};

/* ---------- التخزين ---------- */
function saveAll(){ try{
  localStorage.setItem('trq:cfg',JSON.stringify(S.config));
  localStorage.setItem('trq:keys',S.keys?JSON.stringify(S.keys):'');
  localStorage.setItem('trq:snd',S.sound?'1':'0');
  const rt={grid:S.grid,position:S.position,journal:S.journal,realizedPnl:S.realizedPnl,
    feesPaid:S.feesPaid,priceTrail:S.priceTrail,cvd:S.cvd,huntCount:S.huntCount,
    lastPrice:S.lastPrice,markPrice:S.markPrice,activeCycle:S.activeCycle,status:S.status,
    history:S.history,logs:S.logs};
  localStorage.setItem('trq:rt',JSON.stringify(rt));
}catch(e){} }
function loadAll(){ try{
  const cfg=JSON.parse(localStorage.getItem('trq:cfg')||'null'); if(cfg) S.config={...S.config,...cfg};
  const ks=localStorage.getItem('trq:keys'); if(ks){ try{S.keys=JSON.parse(ks);}catch(e){S.keys=null;} }
  S.sound=localStorage.getItem('trq:snd')!=='0';
  const rt=JSON.parse(localStorage.getItem('trq:rt')||'null');
  if(rt){ Object.assign(S,{grid:rt.grid||[],position:rt.position||null,journal:rt.journal||[],
    realizedPnl:rt.realizedPnl||0,feesPaid:rt.feesPaid||0,priceTrail:rt.priceTrail||[],
    cvd:rt.cvd||0,huntCount:rt.huntCount||0,lastPrice:rt.lastPrice??null,
    markPrice:rt.markPrice??null,activeCycle:rt.activeCycle||null,status:rt.status||'idle',
    history:rt.history||[],logs:rt.logs||[]}); }
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
let API_BASE = (typeof window !== 'undefined' && (window.Capacitor || location.protocol === 'capacitor:'))
  ? DIRECT_BASE : '/kucoin';
// AbortSignal.timeout غير مدعوم في WebViews القديمة — بديل متوافق
function sig(ms){ const c=new AbortController(); setTimeout(()=>c.abort(),ms); return c.signal; }
async function kcFetch(path,opts){
  if(API_BASE===DIRECT_BASE) return await fetch(API_BASE+path,opts);
  let res;
  try{ res=await fetch(API_BASE+path,opts); }
  catch(e){ API_BASE=DIRECT_BASE; return await fetch(API_BASE+path,opts); }
  // 404 أو استجابة HTML (خادم الجوال المحلي يرجع index.html برمز 200) = لا بروكسي — تحوّل مباشر نهائي
  if(res.status===404 || !(res.headers.get('content-type')||'').toLowerCase().includes('json')){
    API_BASE=DIRECT_BASE; return await fetch(API_BASE+path,opts); }
  return res;
}
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
function exPlaceLimit(intent){
  return kcPrivate('POST','/api/v1/orders',{clientOid:intent.clientOid,symbol:intent.symbol,
    side:intent.side,type:'limit',price:String(intent.price),size:intent.qty,
    leverage:String(intent.leverage),timeInForce:'GTC',reduceOnly:!!intent.reduceOnly,
    marginMode:'ISOLATED'});
}
function exPlaceMarket(symbol,side,qty){
  return kcPrivate('POST','/api/v1/orders',{clientOid:'hunt_'+Date.now().toString(36),
    symbol,side,type:'market',size:qty,leverage:String(S.config.leverage),marginMode:'ISOLATED'});
}
const exCancelAll = symbol => kcPrivate('DELETE','/api/v1/orders?symbol='+encodeURIComponent(symbol)).catch(()=>{});
function exClose(symbol,side){
  return kcPrivate('POST','/api/v1/orders',{clientOid:'cls_'+Date.now().toString(36),
    symbol,type:'market',side:side==='short'?'buy':'sell',closeOrder:true,reduceOnly:true});
}
async function exPing(){
  const a=await kcPrivate('GET','/api/v1/account-overview?currency=USDT');
  return a?Number(a.accountEquity??a.availableBalance??0):null;
}

/* ================================================================
   محرك القرار — شبكة + صيد + جني + انعكاس (مطابق للأصل)
   ================================================================ */
function wavg(f){ let n=0,q=0; for(const x of f){ if(x.qty>0&&x.price>0){n+=x.price*x.qty;q+=x.qty;} }
  return q>0?n/q:0; }
function filledAdds(){ const o=S.journal.filter(j=>j.status==='open');
  const n=o.reduce((a,j)=>a+Math.max(1,j.mergedOrders||1),0); return n>0?n:(S.position?1:0); }
function addsBlocked(){ if(S.status!=='running') return false;
  if(filledAdds()>=Math.max(1,S.config.levels)) return true;
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
  if(S.regime&&S.regime.trend==='up'&&bias.score>-6&&mom>-0.04){
    if(prev!=='long') S._flipAt=now; S.config.direction='long'; return 'long'; }
  if(S.regime&&S.regime.trend==='down'&&bias.score<6&&mom<0.04){
    if(prev!=='short') S._flipAt=now; S.config.direction='short'; return 'short'; }
  if(Math.abs(bias.score)<4) return prev;
  if(bias.direction!==prev&&(S._flipAt==null||now-S._flipAt>4000)&&Math.abs(bias.score)>=14){
    S.config.direction=bias.direction; S._flipAt=now; }
  else if(bias.direction===prev) S.config.direction=bias.direction;
  return S.config.direction;
}
function contractsForLevel(p){ const c=S.config;
  const cv=Math.max(1e-12,(S.multiplier||1)*p);
  const per=Math.max(0,c.cycleBalance)*Math.max(1,c.leverage)/Math.max(1,c.levels);
  const raw=Math.max(1,Math.floor(per/cv));
  return Math.max(1,Math.floor(raw*(S.regime?S.regime.sizeMult:1))); }
function addStepPct(){ return Math.max(0.12,S.config.gridStepPct)*(S.regime?S.regime.stepMult:1); }
function tooClose(a,b,st){ return a>0&&b>0&&Math.abs(a-b)/Math.max(a,b)*100<st*0.55; }
function buildGrid(center,wide){ const c=S.config,tick=S.tickSize||1e-10;
  const step=c.gridStepPct/100*(wide?4:(S.regime?S.regime.stepMult:1));
  const stepPct=wide?Math.max(0.12,c.gridStepPct)*4:addStepPct();
  const qty=contractsForLevel(center),grid=[],n=Math.max(1,c.levels);
  const entry=S.position?S.position.entry:center;
  const from=c.direction==='short'?Math.max(center,entry):Math.min(center,entry);
  for(let i=1;i<=n;i++){
    const raw=c.direction==='short'?from*(1+step*i):from*(1-step*i);
    const price=roundTick(raw,tick); if(!(price>0)) continue;
    if(grid.some(g=>tooClose(g.price,price,stepPct))) continue;
    if(c.direction==='short'&&price<=center*(1+step*0.4)) continue;
    if(c.direction==='long'&&price>=center*(1-step*0.4)) continue;
    grid.push({id:uid('lvl'),clientOid:uid('oid'),
      side:c.direction==='short'?'sell':'buy',price,qty,status:'armed',
      reduceOnly:false,exchangeOrderId:null,filledAt:null,origin:'grid',createdAt:Date.now()});
  }
  return grid.sort((a,b)=>b.price-a.price); }
function sanitizeAdds(){ const cap=Math.max(1,S.config.levels);
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
    return {realized:pnl,closedQty:closeQty,addedQty:leftover}; }
  return {realized:pnl,closedQty:closeQty,addedQty:0}; }
function noteEntry(qty,price,fee,source){ const side=S.position?S.position.side:(source==='hunt'?S.config.direction:'short');
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
  return pnl; }
function placeTpOpposite(f){ if(S.status!=='running') return;
  const notional=Math.max(1e-9,f.qty*(S.multiplier||1)*f.price);
  const cover=(S.makerFee+S.takerFee+MIN_NET_USD/notional)*100;
  const px=S.lastPrice||f.price;
  const uw=!!(S.position&&adversePct(px)>=0.35);
  const st=(uw?Math.max(0.04,cover*0.5):Math.max(cover,f.origin==='hunt'?0.22:0.24))/100;
  const tp=f.side==='sell'?roundTick(f.price*(1-st),S.tickSize):roundTick(f.price*(1+st),S.tickSize);
  if(S.grid.some(g=>(g.status==='armed'||g.status==='open')&&g.reduceOnly&&
    Math.abs(g.price-tp)/tp<1e-6)) return;
  S.grid.push({id:uid('tp'),clientOid:uid('oid'),side:f.side==='sell'?'buy':'sell',
    price:tp,qty:f.qty,status:'armed',reduceOnly:true,exchangeOrderId:null,
    filledAt:null,origin:f.origin||'grid',createdAt:Date.now(),lotId:f.lotId});
  S.grid.sort((a,b)=>b.price-a.price); }
function syncPositionTp(){ if(S.status!=='running'||!S.position) return;
  for(const g of S.grid) if(g.reduceOnly&&(g.status==='armed'||g.status==='open')){
    g.status='cancelled'; g.exchangeOrderId=null; }
  placeTpOpposite({side:S.position.side==='short'?'sell':'buy',price:S.position.entry,
    qty:S.position.size,origin:'grid',lotId:S.journal.find(j=>j.status==='open')?.id}); }
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
    l.lotId=l.lotId||lotId; }
  if(d.closedQty>0){ const src=l.origin==='hunt'?'صفقة':'شبكة';
    const row=l.lotId?S.journal.find(j=>j.id===l.lotId):null;
    const net=row?lotNet(row.side,row.entry,fp,row.qty,row.fees+fee)
               :d.realized-fee;
    noteClose(d.closedQty,fp,fee,src,net,l.lotId);
    if(l.origin==='hunt'&&l.reduceOnly) S.huntOpen=Math.max(0,(S.huntOpen||0)-1);
    const parent=S.grid.find(g=>!g.reduceOnly&&g.status==='filled'&&
      (l.lotId?g.lotId===l.lotId:g.origin===l.origin));
    if(parent) parent.status='cancelled';
    S.huntAnchor=fp; S.lastHuntAt=Date.now()-400; }
  if(S.position&&S.status==='running') syncPositionTp();
  S.lastWorkAt=Date.now();
  notify((l.side==='sell'?'بيع ':'شراء ')+(l.origin==='hunt'?'صفقة':l.reduceOnly?'جني':'شبكة')+' @ '+fmtPx(fp)); }
function selectDueAdds(due,p){ if(addsBlocked()) return [];
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
  const step=Math.max(0.12,S.config.gridStepPct);
  const improve=pos.side==='short'?(p-pos.entry)/pos.entry*100:(pos.entry-p)/pos.entry*100;
  if(improve<step*0.7) return false;
  const mom=S.confluence?S.confluence.momentum:0;
  const r=S.regime||{trend:'range'};
  const against=(pos.side==='long'&&r.trend==='down')||(pos.side==='short'&&r.trend==='up');
  if(against&&((pos.side==='long'&&mom<-0.06)||(pos.side==='short'&&mom>0.06))) return false;
  if(pos.side==='long'&&mom<-0.12) return false;
  if(pos.side==='short'&&mom>0.12) return false;
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
    const minNet=stuck?0.04:MIN_NET_USD;
    if(net<minNet) continue;
    if(!(shouldFill(tp,p)||net>=minNet*1.15)) continue;
    fillLevel(tp.id,p,true); } }
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
  return true; }
function effectiveHuntPct(){ const base=Math.max(0.18,S.config.huntPct);
  const mag=Math.abs(S.biasScore);
  const stalled=S.lastWorkAt&&Date.now()-S.lastWorkAt>90000;
  const scaled=base*(S.regime?S.regime.huntMult:1);
  if(S.position&&S.position.side!==S.config.direction) return Math.max(0.08,scaled*0.4);
  if(stalled&&huntAligned()) return Math.max(0.12,scaled*0.55);
  if(mag>=28&&huntAligned()) return Math.max(0.14,scaled*0.75);
  return scaled; }
function huntTooClose(p){ const need=Math.max(MIN_HUNT_GAP,effectiveHuntPct()*0.9);
  for(const j of S.journal){ if(j.status!=='open'||j.source!=='صفقة') continue;
    if(j.entry>0&&Math.abs(p-j.entry)/j.entry*100<need) return true; }
  const last=[...S.grid].reverse().find(g=>g.origin==='hunt'&&g.status==='filled'&&!g.reduceOnly);
  return !!(last&&last.price>0&&Math.abs(p-last.price)/last.price*100<need); }
function openHuntLots(){ return S.journal.filter(j=>j.status==='open'&&j.source==='صفقة').length; }
function packHunt(p){ return {side:S.config.direction==='short'?'sell':'buy',qty:contractsForLevel(p)}; }
function huntTrigger(p){ if(S.status!=='running'||!p) return null;
  const flipping=!!(S.position&&S.config.direction!==S.position.side);
  if(addsBlocked()&&!flipping) return null;
  if(flipping){ if(S.lastHuntAt&&Date.now()-S.lastHuntAt<120000) return null;
    return packHunt(p); }
  if(S.position){ if(!inAddZone(p)) return null; }
  else if(!huntAligned()) return null;
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
  const pct=(p-S.huntAnchor)/S.huntAnchor*100, need=effectiveHuntPct();
  const hit=S.config.direction==='short'?pct<=-need:pct>=need;
  return hit?packHunt(p):null; }
function maybeHunt(p){ const hit=huntTrigger(p); if(!hit) return false;
  S.lastHuntAt=Date.now(); S.huntAnchor=p;
  const fee=feeFor(hit.qty*(S.multiplier||1)*p,true); S.feesPaid+=fee;
  const d=applyDelta(hit.side,hit.qty,p);
  if(d.closedQty>0){ const of=S.journal.find(j=>j.status==='open')?.fees||0;
    noteClose(d.closedQty,p,fee,'صفقة',d.realized-of-fee); S.huntOpen=Math.max(0,(S.huntOpen||0)-1); }
  if(d.addedQty>0){ S.huntCount++; S.huntOpen=(S.huntOpen||0)+1;
    const lotId=noteEntry(d.addedQty,p,fee,'hunt');
    S.grid.push({id:uid('hunt'),clientOid:uid('oid'),side:hit.side,price:p,qty:d.addedQty,
      status:'filled',reduceOnly:false,exchangeOrderId:null,filledAt:Date.now(),
      origin:'hunt',createdAt:Date.now(),lotId});
    notify('صفقة فردية #'+S.huntCount+' — '+(hit.side==='sell'?'بيع':'شراء')+' @ '+fmtPx(p)); }
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
  const wallet=Math.max(0,S.config.cycleBalance+S.realizedPnl-S.feesPaid+
    (S.position?S.position.unrealized:0));
  if(dir==='long'){ const den=q*mult*(1-mmr-liqFee); if(den<=0) return null;
    const margin=Math.min(wallet,notional*(1-mmr-liqFee-0.002));
    const lp=(notional-margin)/den; return lp>0?lp:null; }
  const den=q*mult*(1+mmr+liqFee); if(den<=0) return null;
  return (notional+wallet)/den; }
function refreshLiq(){ const local=computeLiq();
  if(S.exLiqPrice&&S.exLiqPrice>0&&!S.grid.some(g=>!g.reduceOnly&&g.status==='armed'&&!g.exchangeOrderId)){
    S.liqPrice=S.exLiqPrice; }
  else S.liqPrice=local;
  if(S.position) S.position.liquidation=S.liqPrice; }
function markUnrealized(p){ if(S.position){
  S.position.unrealized=(S.position.side==='short'?1:-1)*
    (S.position.entry-p)*(S.multiplier||1)*S.position.size; }
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
function escapeAdverse(p){ if(S.status!=='running'||!S.position||!p) return;
  const danger=liqDanger(p), flip=reversalAgainst(p);
  if(!danger&&!flip) return;
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
  const center=S.lastPrice||S.gridAnchor||(S.position?S.position.entry:0);
  if(!center) return;
  sanitizeAdds();
  const dual=S.grid.some(g=>g.lane==='hold'&&(g.status==='armed'||g.status==='open'))&&
    S.grid.some(g=>g.lane==='trend'&&(g.status==='armed'||g.status==='open'));
  if(dual){ if(S.position) syncPositionTp(); return; }
  if(filledAdds()>=Math.max(1,S.config.levels)){ if(S.position) syncPositionTp(); return; }
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
      S._lastTrailAt=now; }
  }
  if(d.book) S.orderBook=d.book;
  if(d.trade&&d.trade.price>0){
    S.lastPrice=d.trade.price; S.lastTickAt=now;
    S.tape=[d.trade,...S.tape].slice(0,32);
    S.cvd+=d.trade.side==='buy'?d.trade.size:-d.trade.size; }
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
    else if(S.position&&Date.now()>S.ignoreExchangeUntil){ S.position=null; }
    if(pos&&pos.liquidation) S.exLiqPrice=pos.liquidation;
  }catch(e){ pushLog('error','قراءة المركز: '+(e.message||e)); }
  try{
    const exs=await exOrders(sym);
    const byOid=new Map(exs.map(o=>[o.clientOid||'',o]));
    const byId=new Map(exs.map(o=>[o.orderId,o]));
    for(const l of S.grid){ const hit=(l.clientOid&&byOid.get(l.clientOid))||
      (l.exchangeOrderId&&byId.get(l.exchangeOrderId));
      if(hit){ l.status='open'; l.exchangeOrderId=hit.orderId; }
      else if(l.status==='open'&&!shouldFill(l,S.lastPrice||0)){
        l.status='armed'; l.exchangeOrderId=null; } }
    for(const o of exs){ if(S.grid.some(l=>l.exchangeOrderId===o.orderId||
      (o.clientOid&&l.clientOid===o.clientOid))) continue;
      S.grid.push({id:uid('ex'),clientOid:o.clientOid||uid('oid'),side:o.side,
        price:o.price,qty:o.size,status:'open',reduceOnly:o.reduceOnly,
        exchangeOrderId:o.orderId,filledAt:null,origin:'grid'}); }
    const want=S.grid.filter(l=>(l.status==='armed'));
    for(const l of want){ if(l.exchangeOrderId) continue;
      try{ const r=await exPlaceLimit({clientOid:l.clientOid,symbol:sym,side:l.side,
        price:l.price,qty:l.qty,reduceOnly:l.reduceOnly,leverage:S.config.leverage});
        l.status='open'; l.exchangeOrderId=r.orderId;
      }catch(e){ pushLog('error','أمر '+l.side+' @ '+fmtPx(l.price)+': '+(e.message||e)); } }
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
    const m=await loadMarket(full);
    if(S.status==='idle'){ applyMeta(m); emit(); saveAll(); return; }
    S.heartbeat++;
    applyMeta(m);
    resolveDirection(Date.now());
    if(running){
      escapeAdverse(m.price);
      harvestRipe(m.price);
      const due=S.grid.filter(l=>shouldFill(l,m.price));
      for(const l of selectDueAdds(due,m.price)){
        const before=filledAdds(); fillLevel(l.id,l.price,false);
        if(filledAdds()>before) S.lastAddAt=Date.now(); }
      const hunt=huntTrigger(m.price);
      if(hunt){ let done=false;
        if(S.config.mode==='live'&&S.keys){
          try{ await exPlaceMarket(S.config.symbol,hunt.side,hunt.qty); done=true; }
          catch(e){ pushLog('error','فشل صيد السوق: '+(e.message||e)); } }
        else done=true;
        if(done) maybeHunt(m.price); }
      ensureGrid(); harvestRipe(m.price);
    }
    pruneGhosts(); markUnrealized(m.price);
    await liveSync();
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
  S.huntAnchor=p; S.gridAnchor=S.position?S.position.entry:p; S._flipAt=null;
  resolveDirection(Date.now());
  if(!S.grid.filter(g=>!g.reduceOnly&&(g.status==='armed'||g.status==='open')).length){
    S.grid=[...S.grid.filter(g=>g.reduceOnly&&(g.status==='open'||g.status==='armed')),
      ...buildGrid(S.position?S.position.entry:p)]; }
  pushLog('server','دورة جديدة '+(S.config.direction==='short'?'شورت':'لونغ')+
    (S.regime?' · '+S.regime.label:'')+' — الشبكة والصيد يعملان');
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
    try{ await exCancelAll(S.config.symbol); await exClose(S.config.symbol,prevSide);}catch(e){} }
  flattenAt(p,'إيقاف'); S.status='stopped';
  pushLog('info','إيقاف — أُغلقت كل الصفقات عند السعر الحالي');
  toast('تم إيقاف البوت'); emit(); saveAll(); }
export function newCycle(){ const rolled=Math.max(0.01,
    Math.round((S.config.cycleBalance+S.realizedPnl-S.feesPaid)*100)/100);
  const keep=S.status==='running'||S.status==='paused';
  if(S.config.mode==='live'&&S.keys) exCancelAll(S.config.symbol).catch(()=>{});
  // أرشفة صفقات الدورة المنتهية بدل مسحها — السجل يبقى تراكميًا عبر الدورات
  S.history=[...S.journal.filter(j=>j.status!=='open'),...S.history].slice(0,300);
  const cfg={...S.config,cycleBalance:rolled};
  const keys=S.keys, snd=S.sound, hist=S.history, logs=S.logs;
  Object.assign(S,{grid:[],position:null,journal:[],realizedPnl:0,feesPaid:0,
    activeCycle:null,huntCount:0,huntOpen:0,priceTrail:[],emaFast:null,emaSlow:null,
    cvd:0,tape:[],orderBook:{bids:[],asks:[]},status:'idle',startedAt:null,
    huntAnchor:null,gridAnchor:null,liqPrice:null,exLiqPrice:null});
  S.config=cfg; S.keys=keys; S.sound=snd; S.history=hist; S.logs=logs;
  pushLog('info','دورة جديدة — رصيد '+rolled.toFixed(2)+' — سجل الصفقات محفوظ');
  toast('دورة جديدة برصيد $'+rolled.toFixed(2));
  emit(); saveAll(); if(keep) startBot(); }

/* ---------- إجراءات الإعدادات ---------- */
export async function saveKeys(k){
  if(!k.apiKey||!k.apiSecret||!k.passphrase){ toast('أدخل المفتاح والسر والعبارة'); return false; }
  S.keys=k; S.linkOk=false; saveAll(); emit();
  try{ const eq=await exPing();
    S.linkOk=true; S.exEquity=eq;
    toast('تم الربط ✓ الرصيد $'+(eq!=null?eq.toFixed(2):'—'));
  }catch(e){ S.linkOk=false; S.exEquity=null;
    toast('حُفظت المفاتيح لكن فشل الاتصال: '+(e.message||e)); }
  emit(); return S.linkOk;
}
export function clearKeys(){ S.keys=null; S.linkOk=false; S.exEquity=null; saveAll(); emit(); toast('حُذفت المفاتيح'); }
export function saveCfg(v){
  const old=S.config.symbol;
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
  if(sym!==old){
    S.grid=[]; S.position=null; S.journal=[];
    S.realizedPnl=0; S.feesPaid=0; S.ignoreExchangeUntil=Date.now()+12000;
    S.tape=[]; S.priceTrail=[]; S.emaFast=S.emaSlow=null; S._metaAt=0;
    S.lastPrice=null; S.markPrice=null; S.lastTickAt=null; S.cvd=0;
    S.orderBook={bids:[],asks:[]}; S.biasScore=0; S.biasReasons=[];
    S.regime=null; S.confluence=null; S.heartbeat=0;
    S.liqPrice=null; S.exLiqPrice=null; S.huntAnchor=null; S.gridAnchor=null;
    setStreamSymbol(sym);
    pushLog('server','انتقال إلى '+S.config.displaySymbol+' — أُلغيت حالة الزوج السابق'); }
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
  emit();
}
