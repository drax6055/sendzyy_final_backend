'use strict';

const crypto = require('crypto');

const ALGORITHM = 'aes-256-cbc';
const IV_LENGTH = 16; // AES block size

/**
 * Returns a 32-byte (256-bit) key Buffer from WEBHOOK_SECRET_KEY env var.
 * WEBHOOK_SECRET_KEY must be a 64-character hex string (32 bytes).
 * Falls back to a hardcoded dev key with a console warning.
 */
function getKey() {
  const hexKey = process.env.WEBHOOK_SECRET_KEY;
  if (hexKey && hexKey.length === 64) {
    return Buffer.from(hexKey, 'hex');
  }
  console.warn(
    '[cryptoUtils] WEBHOOK_SECRET_KEY not set or invalid. ' +
    'Using insecure dev fallback key. Set a 64-char hex string in production.'
  );
  // 32-byte dev fallback (never use in production)
  return Buffer.from('0000000000000000000000000000000000000000000000000000000000000000', 'hex');
}

/**
 * Encrypts a plaintext string using AES-256-CBC.
 * @param {string} plaintext
 * @returns {string} "ivHex:encryptedHex"
 */
function encryptSecret(plaintext) {
  const key = getKey();
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);
  const encrypted = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  return `${iv.toString('hex')}:${encrypted.toString('hex')}`;
}

/**
 * Decrypts a stored AES-256-CBC value.
 * @param {string} stored "ivHex:encryptedHex"
 * @returns {string} plaintext
 */
function decryptSecret(stored) {
  if (!stored || !stored.includes(':')) {
    // Legacy plain-text secret — return as-is
    return stored;
  }
  try {
    const [ivHex, encryptedHex] = stored.split(':');
    const key = getKey();
    const iv = Buffer.from(ivHex, 'hex');
    const encryptedText = Buffer.from(encryptedHex, 'hex');
    const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
    const decrypted = Buffer.concat([decipher.update(encryptedText), decipher.final()]);
    return decrypted.toString('utf8');
  } catch (err) {
    console.error('[cryptoUtils] decryptSecret failed (wrong key or corrupt data):', err.message);
    throw new Error('Failed to decrypt webhook secret. Check WEBHOOK_SECRET_KEY env var.');
  }
}

/**
 * Masks a secret, showing only the last 8 characters.
 * @param {string} secret
 * @returns {string} masked string
 */
function maskSecret(secret) {
  if (!secret || secret.length < 8) return '********';
  return '*'.repeat(secret.length - 8) + secret.slice(-8);
}

module.exports = { encryptSecret, decryptSecret, maskSecret };
