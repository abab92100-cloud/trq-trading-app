#!/data/data/com.termux/files/usr/bin/bash
# ══════════════════════════════════════════════════════════════
# TRQ Trading — مثبّت ومشغّل سيرفر الجوال بأمر واحد
# الاستخدام (انسخه كاملًا في Termux):
#   curl -sL https://github.com/abab92100-cloud/trq-trading-app/releases/latest/download/trq-setup.sh | bash
# ══════════════════════════════════════════════════════════════
set -e

echo "═══ TRQ — سيرفر الجوال ═══"

# 1) Node.js — يُثبَّت تلقائيًا إن لم يوجد
if ! command -v node >/dev/null 2>&1; then
  echo "• تثبيت Node.js..."
  pkg update -y
  pkg install -y nodejs-lts
fi

# 2) تحميل أحدث نسخة من السيرفر دائمًا (رابط latest لا يتقادم)
mkdir -p "$HOME/trq" && cd "$HOME/trq"
echo "• تحميل أحدث سيرفر..."
curl -L -s -o trq-server.mjs "https://github.com/abab92100-cloud/trq-trading-app/releases/latest/download/trq-server.mjs"

# 3) منع نوم المعالج — السيرفر يعمل والشاشة مطفأة
echo "• تفعيل قفل الاستيقاظ..."
termux-wake-lock 2>/dev/null || true

echo "• السيرفر يعمل الآن — افتح تطبيق TRQ وستظهر شارة «سيرفر»"
echo "  (أبقِ هذه النافذة مفتوحة — لإيقاف السيرفر: Ctrl+C)"
echo "──────────────────────────────────────────────"
exec node trq-server.mjs
