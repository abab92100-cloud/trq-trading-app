import path from "path"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

export default defineConfig({
  base: './',
  plugins: [react()],
  server: {
    port: 3000,
    host: true,
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
