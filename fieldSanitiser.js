/**
 * fieldSanitiser.js
 *
 * Sanitises lead string fields by stripping HTML tags, trimming whitespace,
 * and enforcing maximum length limits per field.
 *
 * Requirement 12.5
 */

const FIELD_LIMITS = {
  name: 100,
  email: 254,
  companyName: 200,
  formName: 100,
};

/**
 * Strip HTML tags from a string and trim whitespace.
 * @param {string} value
 * @returns {string}
 */
function stripHtml(value) {
  return value.replace(/<[^>]*>/g, '').trim();
}

/**
 * Sanitise lead fields: strip HTML tags, trim, and enforce length limits.
 * Non-string values are returned as-is.
 * mobileNumber is NOT handled here (handled by mobileUtils).
 *
 * @param {{ name?: any, email?: any, companyName?: any, formName?: any }} fields
 * @returns {{ name?: any, email?: any, companyName?: any, formName?: any }}
 */
function sanitiseLeadFields(fields) {
  const result = {};

  for (const [key, value] of Object.entries(fields)) {
    if (typeof value !== 'string') {
      result[key] = value;
      continue;
    }

    let sanitised = stripHtml(value);

    const limit = FIELD_LIMITS[key];
    if (limit !== undefined && sanitised.length > limit) {
      sanitised = sanitised.slice(0, limit);
    }

    result[key] = sanitised;
  }

  return result;
}

module.exports = { sanitiseLeadFields };
