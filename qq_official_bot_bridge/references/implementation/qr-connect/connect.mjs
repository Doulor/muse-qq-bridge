// QR-code credential binding for the QQ bridge.
//
// Uses Tencent's official @tencent-connect/qqbot-connector: it shows a QR
// code, the owner scans it with QQ and picks the bot, and the AppID /
// AppSecret come back through the callback — the secret never passes
// through chat or a log line. On success the credentials are written to
// <bridge>/credentials.env with mode 0600.
//
// Usage:
//   cd qr-connect && npm install && node connect.mjs
// The bridge directory defaults to the parent of this implementation
// directory's bridge home; override with QQ_BRIDGE_HOME.
import { startQrConnect } from '@tencent-connect/qqbot-connector';
import QRCode from 'qrcode';
import { writeFileSync, chmodSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const BASE = process.env.QQ_BRIDGE_HOME
  || resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const QR_PNG = resolve(dirname(fileURLToPath(import.meta.url)), 'qr.png');
const EVENTS = resolve(dirname(fileURLToPath(import.meta.url)), 'events.log');
const ENV_FILE = resolve(BASE, 'credentials.env');

function logEvent(line) {
  const stamp = new Date().toISOString();
  writeFileSync(EVENTS, `[${stamp}] ${line}\n`, { flag: 'a' });
}

const hardStop = setTimeout(() => {
  logEvent('TIMEOUT no scan within 15 minutes, exiting');
  process.exit(3);
}, 15 * 60 * 1000);

startQrConnect({
  async onQrDisplayed(url) {
    try {
      await QRCode.toFile(QR_PNG, url, { width: 560, margin: 2 });
      logEvent('QR_READY qr.png regenerated');
    } catch (e) {
      logEvent('QR_RENDER_FAILED ' + e.message);
    }
  },
  onQrExpired() {
    logEvent('QR_EXPIRED refreshing');
  },
  onSuccess(creds) {
    clearTimeout(hardStop);
    const first = creds[0];
    const body = `QQBOT_APP_ID=${first.appId}\nQQBOT_APP_SECRET=${first.appSecret}\n`;
    writeFileSync(ENV_FILE, body, { mode: 0o600 });
    chmodSync(ENV_FILE, 0o600);
    const masked = first.appId.length > 6 ? first.appId.slice(0, 4) + '***' + first.appId.slice(-2) : '***';
    writeFileSync(resolve(BASE, 'qr-connect', 'bound.json'), JSON.stringify({ appIdMasked: masked, count: creds.length, at: new Date().toISOString() }, null, 2));
    logEvent('BOUND credentials.env written (0600), appId ' + masked);
    process.exit(0);
  },
  onFailure(err) {
    clearTimeout(hardStop);
    logEvent('FAILED ' + err.message);
    process.exit(1);
  },
}, { displayQrCodeToConsole: false });

logEvent('STARTED waiting for scan');
