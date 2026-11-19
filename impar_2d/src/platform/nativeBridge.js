/**
 * Ponte nativa opcional para Capacitor (Android/iOS) com fallback seguro para Web e Desktop (WebView2).
 * Mantém impar_2d/ 100% livre de imports de pacotes npm para funcionar sem bundler.
 */

export const PUBLIC_WEB_URL = 'https://enyosaldanha26.github.io/impar/';
const PREFS_STORAGE_KEY = 'impar_prefs_v1';

export function isNativeApp() {
  return Boolean(
    typeof window !== 'undefined' &&
      window.Capacitor &&
      typeof window.Capacitor.isNativePlatform === 'function' &&
      window.Capacitor.isNativePlatform()
  );
}

function getPlugins() {
  return (typeof window !== 'undefined' && window.Capacitor && window.Capacitor.Plugins) || {};
}

export function getInviteUrl(roomCode) {
  const cleanCode = String(roomCode || '').trim().toUpperCase();
  if (!cleanCode) return PUBLIC_WEB_URL;

  try {
    const loc = window.location;
    const host = (loc.hostname || '').toLowerCase();
    const isLocalHost =
      isNativeApp() ||
      loc.protocol === 'file:' ||
      loc.protocol === 'capacitor:' ||
      host === 'localhost' ||
      host === '127.0.0.1' ||
      host === 'impar2d.local';

    if (isLocalHost) {
      return `${PUBLIC_WEB_URL}?sala=${encodeURIComponent(cleanCode)}`;
    }

    const url = new URL(loc.href);
    url.searchParams.set('sala', cleanCode);
    return url.toString();
  } catch {
    return `${PUBLIC_WEB_URL}?sala=${encodeURIComponent(cleanCode)}`;
  }
}

export async function copyText(text) {
  const value = String(text ?? '');
  if (!value) return false;

  try {
    if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
      await navigator.clipboard.writeText(value);
      return true;
    }
  } catch {
    // Segue para fallback abaixo
  }

  try {
    const ta = document.createElement('textarea');
    ta.value = value;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.top = '-9999px';
    ta.style.left = '-9999px';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    const ok = document.execCommand('copy');
    ta.remove();
    return Boolean(ok);
  } catch {
    return false;
  }
}

export async function shareRoomInvite({ roomCode, roomName } = {}) {
  const url = getInviteUrl(roomCode);
  const label = roomName ? `"${roomName}"` : 'minha mesa';
  const { Share } = getPlugins();

  if (isNativeApp() && Share && typeof Share.share === 'function') {
    try {
      await Share.share({
        title: 'ÍMPAR — Convite de Sala',
        text: `Entre na ${label} no jogo ÍMPAR! Código: ${roomCode}`,
        url,
        dialogTitle: 'Convidar para a sala',
      });
      return { ok: true, method: 'share' };
    } catch (err) {
      const msg = String(err?.message || '').toLowerCase();
      if (msg.includes('cancel') || msg.includes('abort') || msg.includes('dismiss')) {
        return { ok: false, method: 'cancelled' };
      }
      // Fallback para clipboard se o Share falhar por outro motivo
    }
  }

  const copied = await copyText(url);
  return { ok: copied, method: 'clipboard', url };
}

export function hapticImpact(style = 'LIGHT') {
  try {
    const { Haptics } = getPlugins();
    if (Haptics && typeof Haptics.impact === 'function') {
      Haptics.impact({ style }).catch(() => {});
    }
  } catch {
    // Ignora em dispositivos sem suporte
  }
}

export function hapticNotification(type = 'SUCCESS') {
  try {
    const { Haptics } = getPlugins();
    if (Haptics && typeof Haptics.notification === 'function') {
      Haptics.notification({ type }).catch(() => {});
    }
  } catch {
    // Ignora em dispositivos sem suporte
  }
}

export function loadPrefs() {
  try {
    const raw = window.localStorage?.getItem(PREFS_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

export function savePrefs(partial) {
  if (!partial || typeof partial !== 'object') return;
  try {
    const current = loadPrefs();
    const next = { ...current, ...partial };
    window.localStorage?.setItem(PREFS_STORAGE_KEY, JSON.stringify(next));
  } catch {
    // Ignora se localStorage estiver bloqueado
  }
}

export function parseInviteCodeFromUrl(rawUrl) {
  if (!rawUrl) return '';
  try {
    const str = String(rawUrl).trim();
    const parsed = new URL(str);
    const queryCode = (parsed.searchParams.get('sala') || '').trim().toUpperCase();
    if (queryCode) return queryCode;

    // Suporte a impar://sala/XXXXX
    if (parsed.protocol === 'impar:') {
      const parts = [parsed.hostname, ...parsed.pathname.split('/')].filter(Boolean);
      const salaIdx = parts.findIndex((p) => p.toLowerCase() === 'sala');
      if (salaIdx >= 0 && parts[salaIdx + 1]) {
        return parts[salaIdx + 1].trim().toUpperCase();
      }
      if (parts[0] && parts[0].toLowerCase() !== 'sala') {
        return parts[0].trim().toUpperCase();
      }
    }
  } catch {
    const match = String(rawUrl).match(/[?&]sala=([A-Za-z0-9]+)/i);
    if (match && match[1]) return match[1].toUpperCase();
  }
  return '';
}

let nativeInitialized = false;

export async function initNativePlatform({ onBackButton, onInviteCode, onAppResume } = {}) {
  if (!isNativeApp() || nativeInitialized) return;
  nativeInitialized = true;

  const { App, SplashScreen, SystemBars } = getPlugins();

  try {
    if (SystemBars && typeof SystemBars.setStyle === 'function') {
      await SystemBars.setStyle({ style: 'LIGHT' });
    }
  } catch {
    // Ignora caso não suportado na versão da WebView
  }

  try {
    if (App && typeof App.addListener === 'function') {
      App.addListener('backButton', () => {
        onBackButton?.();
      });
      App.addListener('appUrlOpen', ({ url }) => {
        const code = parseInviteCodeFromUrl(url);
        if (code) onInviteCode?.(code);
      });
      App.addListener('appStateChange', ({ isActive }) => {
        if (isActive) onAppResume?.();
      });
    }
  } catch {
    // Ignora falha ao registrar listeners nativos
  }

  try {
    if (SplashScreen && typeof SplashScreen.hide === 'function') {
      setTimeout(() => {
        SplashScreen.hide().catch(() => {});
      }, 150);
    }
  } catch {
    // Ignora falha ao esconder splash
  }
}

export function exitApp() {
  try {
    const { App } = getPlugins();
    if (App && typeof App.exitApp === 'function') {
      App.exitApp();
    }
  } catch {
    // Ignora em navegadores comuns
  }
}
