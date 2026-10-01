// @ts-nocheck
/* ================================================================
   KuCoin Futures — عميل WebSocket عام (تدفق مستمر بلا انقطاع)
   - يجلب رمز bullet-public عبر البروكسي المحلي (/kucoin)
   - يتصل بخادم الدفع wss://ws-api-futures.kucoin.com
   - يشترك في: السعر اللحظي + دفتر الأوامر + الصفقات المنفذة
   - نبض ping/pong حسب مواصفات KuCoin + إعادة اتصال تلقائية
   ================================================================ */
let REST = '/kucoin';

export const streamState = {
  connected:false, connecting:false, lastMsgAt:0, bookAt:0, tradeAt:0,
  msgs:0, reconnects:0, error:null,
};

let ws=null, pingTimer=null, watchTimer=null, retryTimer=null;
let symbol=null, onMessage=null, onStatus=null;
let stopped=true, attempts=0, connectSeq=0;

async function getBullet(){
  async function tryFetch(base){
    const res=await fetch(base+'/api/v1/bullet-public',{method:'POST',cache:'no-store'});
    if(!res.ok) throw new Error('bullet http '+res.status);
    return res;
  }
  let res;
  try{ res=await tryFetch(REST); }
  catch(e){ REST='https://api-futures.kucoin.com'; res=await tryFetch(REST); }
  const j=await res.json();
  if(j.code!=='200000') throw new Error(j.msg||('bullet '+j.code));
  const srv=(j.data.instanceServers||[])[0]||{};
  return { token:j.data.token, endpoint:srv.endpoint,
    pingInterval:srv.pingInterval||18000, pingTimeout:srv.pingTimeout||10000 };
}

function topics(sym){
  return [
    '/contractMarket/tickerV2:'+sym,      // السعر اللحظي
    '/contractMarket/level2Depth5:'+sym,  // دفتر الأوامر (أفضل 5 مستويات)
    '/contractMarket/execution:'+sym,     // الصفقات المنفذة لحظيًا
  ];
}

function subscribe(){
  if(!ws||ws.readyState!==1||!symbol) return;
  for(const t of topics(symbol)){
    ws.send(JSON.stringify({id:Date.now()+''+Math.floor(Math.random()*1000),
      type:'subscribe', topic:t, privateChannel:false, response:true}));
  }
}

function clearTimers(){
  if(pingTimer){clearInterval(pingTimer);pingTimer=null;}
  if(watchTimer){clearInterval(watchTimer);watchTimer=null;}
}

function handleData(j){
  streamState.lastMsgAt=Date.now(); streamState.msgs++;
  if(j.type==='pong'||j.type==='ack'||j.type==='welcome') return;
  if(j.type!=='message'||!j.topic||!j.data) return;
  const d=j.data;
  const msgSym=j.topic.includes(':')?j.topic.split(':')[1]:null;
  if(msgSym&&symbol&&msgSym!==symbol) return; // تجاهل رسائل الزوج القديم بعد التبديل
  if(j.topic.startsWith('/contractMarket/ticker')){
    let price=+d.price||0;
    const bb=+d.bestBidPrice||0, ba=+d.bestAskPrice||0;
    if(!price&&bb>0&&ba>0) price=(bb+ba)/2; // منتصف السبريد عند غياب آخر صفقة
    if(price>0&&onMessage) onMessage({symbol:msgSym,price, side:d.side, size:+d.size||0});
  }else if(j.topic.startsWith('/contractMarket/level2')){
    const bids=(d.bids||[]).map(r=>({price:+r[0],size:+r[1]})).filter(r=>r.price>0);
    const asks=(d.asks||[]).map(r=>({price:+r[0],size:+r[1]})).filter(r=>r.price>0);
    if(bids.length||asks.length){ streamState.bookAt=Date.now();
      if(onMessage) onMessage({symbol:msgSym,book:{bids,asks}}); }
  }else if(j.topic.startsWith('/contractMarket/execution')){
    streamState.tradeAt=Date.now();
    if(onMessage) onMessage({symbol:msgSym,trade:{side:String(d.side||''),size:+d.size||0,price:+d.price||0}});
  }
}

async function connect(){
  if(stopped) return;
  const seq=++connectSeq;
  streamState.connecting=true; streamState.error=null;
  if(onStatus) onStatus({...streamState});
  try{
    const b=await getBullet();
    if(stopped||seq!==connectSeq) return;
    const socket=new WebSocket(b.endpoint+'?token='+b.token+'&connectId='+seq+'_'+Date.now().toString(36));
    ws=socket;
    socket.onopen=()=>{
      if(seq!==connectSeq){ try{socket.close();}catch(e){} return; }
      attempts=0;
      streamState.connected=true; streamState.connecting=false;
      subscribe();
      pingTimer=setInterval(()=>{
        try{ socket.send(JSON.stringify({id:String(Date.now()),type:'ping'})); }catch(e){}
      },Math.max(5000,b.pingInterval-2000));
      // مراقب النبض: إن صمت الخادم أكثر من المهلة نُغلق لإجبار إعادة الاتصال
      watchTimer=setInterval(()=>{
        if(Date.now()-streamState.lastMsgAt>b.pingInterval+b.pingTimeout+4000){
          try{socket.close();}catch(e){}
        }
      },5000);
      streamState.lastMsgAt=Date.now();
      if(onStatus) onStatus({...streamState});
    };
    socket.onmessage=(ev)=>{ try{ handleData(JSON.parse(ev.data)); }catch(e){} };
    socket.onerror=()=>{ streamState.error='خطأ في القناة'; };
    socket.onclose=()=>{
      if(seq!==connectSeq) return;
      clearTimers();
      streamState.connected=false; streamState.connecting=false;
      if(onStatus) onStatus({...streamState});
      scheduleRetry();
    };
  }catch(e){
    streamState.connecting=false; streamState.error=e.message||String(e);
    if(onStatus) onStatus({...streamState});
    scheduleRetry();
  }
}

function scheduleRetry(){
  if(stopped) return;
  attempts++;
  streamState.reconnects++;
  const wait=Math.min(15000,1000*Math.pow(1.6,Math.min(attempts,8)));
  if(retryTimer) clearTimeout(retryTimer);
  retryTimer=setTimeout(connect,wait);
}

export function startStream(sym,handlers){
  stopStream();
  stopped=false; attempts=0;
  symbol=sym; onMessage=handlers.onMessage; onStatus=handlers.onStatus;
  connect();
}

export function setStreamSymbol(sym){
  if(sym===symbol) return;
  const old=symbol;
  symbol=sym;
  if(ws&&ws.readyState===1){
    // إلغاء اشتراك الزوج القديم حتى لا تختلط بياناته مع الجديد
    if(old) for(const t of topics(old)){
      try{ ws.send(JSON.stringify({id:'u'+Date.now()+t.length,type:'unsubscribe',topic:t,privateChannel:false,response:false})); }catch(e){}
    }
    subscribe();
  }
}

export function stopStream(){
  stopped=true;
  if(retryTimer){clearTimeout(retryTimer);retryTimer=null;}
  clearTimers();
  connectSeq++;
  if(ws){ try{ws.close();}catch(e){} ws=null; }
  streamState.connected=false; streamState.connecting=false;
}

// عند عودة التبويب للواجهة: تحقق فوري من حياة القناة
if(typeof document!=='undefined'){
  document.addEventListener('visibilitychange',()=>{
    if(!document.hidden&&!stopped){
      const silent=Date.now()-streamState.lastMsgAt;
      if(!ws||ws.readyState!==1||silent>30000){ if(retryTimer)clearTimeout(retryTimer); connect(); }
    }
  });
}
