import path from "path"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"
import { inspectAttr } from 'kimi-plugin-inspect-react'

// https://vite.dev/config/
export default defineConfig({
  base: './',
  plugins: [inspectAttr(), react()],
  build: {
    target: 'es2017',      // توافق أوسع مع WebView القديمة في الجوال
    cssTarget: 'chrome61',
  },
  server: {
    port: 3000,
    host: true, // إتاحة الوصول من الجوال على نفس الشبكة
    proxy: {
      // بروكسي محلي لتجاوز حظر CORS — كل طلبات /kucoin تُمرَّر إلى خوادم KuCoin الآجلة
      '/kucoin': {
        target: 'https://api-futures.kucoin.com',
        changeOrigin: true,
        secure: true,
        rewrite: (p) => p.replace(/^\/kucoin/, ''),
      },
    },
  },
  preview: {
    proxy: {
      '/kucoin': {
        target: 'https://api-futures.kucoin.com',
        changeOrigin: true,
        secure: true,
        rewrite: (p) => p.replace(/^\/kucoin/, ''),
      },
    },
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
});
