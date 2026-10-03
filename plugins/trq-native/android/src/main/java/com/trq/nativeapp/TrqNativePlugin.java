package com.trq.nativeapp;

import android.content.Context;
import android.os.Handler;
import android.os.Looper;
import android.os.PowerManager;

import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;

/**
 * TRQ Native Keep-Alive
 * 1) Partial WakeLock: يمنع نوم المعالج عند إطفاء الشاشة — مؤقتات المحرك لا تتجمد.
 * 2) Native Watchdog: مؤقّت أصلي خارج الويب فيو يفحص نبض المحرك كل 30 ثانية،
 *    وإن توقف النبض أكثر من 90 ثانية (تجميد/خنق) يعيد تحميل التطبيق تلقائيًا
 *    فيُعاد الإقلاع والاتصال بالقناة دون تدخل المستخدم.
 */
@CapacitorPlugin(name = "TrqNative")
public class TrqNativePlugin extends Plugin {

    private PowerManager.WakeLock wakeLock;
    private final Handler watchdog = new Handler(Looper.getMainLooper());
    private volatile long lastHeartbeat = System.currentTimeMillis();
    private volatile boolean watchdogRunning = false;

    private final Runnable watchdogTask = new Runnable() {
        @Override
        public void run() {
            if (!watchdogRunning) return;
            long silent = System.currentTimeMillis() - lastHeartbeat;
            if (silent > 90000) {
                lastHeartbeat = System.currentTimeMillis();
                try {
                    if (getBridge() != null && getBridge().getWebView() != null) {
                        // علامة إنعاش تُقرأ بعد الإقلاع لتسجيل الحدث في سجل البوت
                        getBridge().getWebView().evaluateJavascript(
                            "try{localStorage.setItem('trq_revived',String(Date.now()))}catch(e){};window.location.reload()",
                            null);
                    }
                } catch (Exception ignored) { }
            }
            watchdog.postDelayed(this, 30000);
        }
    };

    @PluginMethod
    public void acquireWakeLock(PluginCall call) {
        try {
            if (wakeLock == null) {
                PowerManager pm = (PowerManager) getContext().getSystemService(Context.POWER_SERVICE);
                wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "trq:engine");
                wakeLock.setReferenceCounted(false);
            }
            if (!wakeLock.isHeld()) wakeLock.acquire();
            call.resolve();
        } catch (Exception e) {
            call.reject("wakelock: " + e.getMessage());
        }
    }

    @PluginMethod
    public void releaseWakeLock(PluginCall call) {
        try {
            if (wakeLock != null && wakeLock.isHeld()) wakeLock.release();
        } catch (Exception ignored) { }
        call.resolve();
    }

    @PluginMethod
    public void startWatchdog(PluginCall call) {
        lastHeartbeat = System.currentTimeMillis();
        if (!watchdogRunning) {
            watchdogRunning = true;
            watchdog.postDelayed(watchdogTask, 30000);
        }
        call.resolve();
    }

    @PluginMethod
    public void stopWatchdog(PluginCall call) {
        watchdogRunning = false;
        watchdog.removeCallbacks(watchdogTask);
        call.resolve();
    }

    @PluginMethod
    public void heartbeat(PluginCall call) {
        lastHeartbeat = System.currentTimeMillis();
        call.resolve();
    }

    @Override
    protected void handleOnDestroy() {
        watchdogRunning = false;
        watchdog.removeCallbacks(watchdogTask);
        try {
            if (wakeLock != null && wakeLock.isHeld()) wakeLock.release();
        } catch (Exception ignored) { }
    }
}
