/* TRQ Trading — Service Worker
   يخزّن واجهة التطبيق للعمل السريع، ويمرّر بيانات KuCoin دائمًا من الشبكة */
const CACHE = 'trq-v3';

self.addEventListener('install', e => {
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  // حذف كل النسخ القديمة المخزنة — التحديثات تصل فورًا ولا يبقى كود قديم
  e.waitUntil(
    caches.keys()
      .then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  // بيانات المنصة والقناة: شبكة دائمًا، بلا تخزين
  if (url.pathname.startsWith('/kucoin') || url.hostname.includes('kucoin.com')) return;
  if (e.request.method !== 'GET') return;
  e.respondWith(
    fetch(e.request).then(res => {
      if (res.ok && url.origin === location.origin && url.pathname.includes('/assets/')) {
        const copy = res.clone();
        caches.open(CACHE).then(c => c.put(e.request, copy));
      }
      return res;
    }).catch(() => caches.match(e.request).then(r => r || caches.match('./index.html')))
  );
});
