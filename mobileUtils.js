/**
 * Mobile number utilities for lead ingestion.
 * Requirements: 1.7, 12.3, 12.4
 */

/**
 * Normalise a raw mobile number string.
 * 1. Strip all non-digit characters.
 * 2. If the result is exactly 10 digits, prepend '91' (Indian country code).
 * 3. Return the resulting digit string.
 * 4. If input is null/undefined/empty, return ''.
 *
 * @param {string|null|undefined} raw
 * @returns {string}
 */
function normaliseMobileNumber(raw) {
  if (raw == null || raw === '') return '';
  const digits = String(raw).replace(/\D/g, '');
  if (digits.length === 10) return `91${digits}`;
  return digits;
}

/**
 * Validate a normalised mobile number.
 * Returns true if the number has between 7 and 15 digits (inclusive).
 *
 * @param {string} normalised
 * @returns {boolean}
 */
function validateMobileNumber(normalised) {
  if (!normalised) return false;
  return normalised.length >= 7 && normalised.length <= 15;
}

module.exports = { normaliseMobileNumber, validateMobileNumber };
