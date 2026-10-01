import type { CapacitorConfig } from '@capacitor/cli';

const config: CapacitorConfig = {
  appId: 'com.trq.trading',
  appName: 'TRQ Trading',
  webDir: 'dist',
  backgroundColor: '#0a0e14',
  plugins: {
    // توجيه كل طلبات fetch عبر طبقة الجوال الأصلية — يتجاوز قيود CORS في WebView نهائيًا
    CapacitorHttp: { enabled: true },
  },
};

export default config;
