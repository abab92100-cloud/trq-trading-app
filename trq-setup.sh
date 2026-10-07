#!/data/data/com.termux/files/usr/bin/bash
# ══════════════════════════════════════════════════════════════
# TRQ Trading — مثبّت ومشغّل سيرفر الجوال بأمر واحد
# الاستخدام (انسخه كاملًا في Termux):
#   curl -sL https://github.com/abab92100-cloud/trq-trading-app/releases/latest/download/trq-setup.sh | bash
# مهم: إن كان السيرفر القديم يعمل في النافذة (سطور تتكرر) اضغط Ctrl+C أولًا ثم الصق الأمر
# إن لم يوجد curl أصلًا:
#   pkg install -y curl && curl -sL https://github.com/abab92100-cloud/trq-trading-app/releases/latest/download/trq-setup.sh | bash
# ══════════════════════════════════════════════════════════════

echo "═══ TRQ — سيرفر الجوال ═══"

# 0) فحص الاتصال أولًا — خطأ واضح بدل صمت محيّر
echo "• فحص الاتصال بالإنترنت..."
if ! curl -sI --max-time 12 https://github.com >/dev/null 2>&1; then
  echo "✗ لا يصل الجوال إلى GitHub — تحقق من الإنترنت (أو جرّب بيانات الجوال بدل الواي فاي) ثم أعد الأمر"
  exit 1
fi
echo "  الاتصال سليم ✓"

# 1) Node.js — يُثبَّت تلقائيًا إن لم يوجد
#    (</dev/null يمنع pkg من التهام مجرى السكربت أثناء التنفيذ عبر الأنبوب)
if ! command -v node >/dev/null 2>&1; then
  echo "• تثبيت Node.js (أول مرة فقط — قد يستغرق دقيقة)..."
  pkg update -y </dev/null
  pkg install -y nodejs-lts </dev/null
fi
echo "  Node $(node -v) ✓"

# 2) تحميل أحدث نسخة من السيرفر دائمًا (رابط latest لا يتقادم) + إعادة محاولة
mkdir -p "$HOME/trq" && cd "$HOME/trq"
echo "• تحميل أحدث سيرفر..."
if ! curl -L -s --fail --retry 3 --max-time 60 -o trq-server.mjs "https://github.com/abab92100-cloud/trq-trading-app/releases/latest/download/trq-server.mjs"; then
  echo "✗ فشل تحميل السيرفر — أعد المحاولة بعد دقيقة"
  exit 1
fi
echo "  حُمّل $(wc -c < trq-server.mjs) بايت ✓"

# 3) إيقاف أي سيرفر قديم — تشغيل الأمر مرتين كان يجعل النسخة الجديدة تصطدم
#    بالمنفذ المشغول فتنهار وتعيد المحاولة بلا نهاية وكأن الأمر «لا يعمل»
echo "• إيقاف أي نسخة قديمة من السيرفر..."
pkill -f trq-server.mjs 2>/dev/null || true
sleep 2

# 4) منع نوم المعالج — السيرفر يعمل والشاشة مطفأة
echo "• تفعيل قفل الاستيقاظ..."
termux-wake-lock 2>/dev/null || true

# 4ب) تحذير البطارية — بدونه يقتل أندرويد السيرفر بعد ~30 دقيقة خلفية
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  ⚠ خطوة إلزامية مرة واحدة حتى لا يتوقف السيرفر وأنت نائم:  ║"
echo "║  إعدادات الجوال ← التطبيقات ← Termux ← البطارية          ║"
echo "║  ← اختر «غير مقيَّد / Unrestricted»                       ║"
echo "║  وفي «العناية بالبطارية» أضف Termux إلى قائمة            ║"
echo "║  «التطبيقات التي لا تُثبَّط أبدًا»                         ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

echo "• السيرفر يعمل الآن — افتح تطبيق TRQ وستظهر شارة «سيرفر»"
echo "  (أبقِ هذه النافذة مفتوحة — إن توقف السيرفر يعود تلقائيًا خلال 3 ثوانٍ)"
echo "──────────────────────────────────────────────"

# 5) حارس إعادة التشغيل الذاتي: أي توقف مفاجئ = انطلاق من جديد خلال 3 ثوانٍ
#    والحالة محفوظة في ملف المخزن فيستأنف من حيث توقف لا من الصفر
while true; do
  node trq-server.mjs
  echo "⚠ السيرفر توقف — إعادة تشغيل خلال 3 ثوانٍ... (للإيقاف النهائي: Ctrl+C)"
  sleep 3
done
