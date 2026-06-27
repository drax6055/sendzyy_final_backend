/**
 * LeadPayloadParser — pure function, no side effects, no DB calls.
 * Accepts a raw JSON payload and a source hint ('shopify' | 'wordpress')
 * and returns a normalised lead object.
 *
 * Validates: Requirements 4.2, 4.3, 5.2, 5.3, 13.1, 13.2, 13.3, 13.5, 13.6
 */

/**
 * @param {object} rawPayload - The raw incoming JSON payload
 * @param {string} source     - 'shopify' or 'wordpress'
 * @returns {{ name: string, mobileNumber: string, email: string, companyName: string, formName: string, source: string, metadata: object }}
 */
function parseLeadPayload(rawPayload, source) {
  const payload = rawPayload || {};

  if (source === 'shopify') {
    return parseShopifyPayload(payload);
  }

  if (source === 'wordpress') {
    return parseWordPressPayload(payload);
  }

  // Unknown source — return empty mapped fields, preserve everything in metadata
  return {
    name: '',
    mobileNumber: '',
    email: '',
    companyName: '',
    formName: '',
    source: source || '',
    metadata: rawPayload,
  };
}

// ── Shopify ──────────────────────────────────────────────────────────────────

function parseShopifyPayload(payload) {
  // Distinguish customers/create (no billing_address) from orders/create (has billing_address)
  if (payload.billing_address) {
    return parseShopifyOrder(payload);
  }
  return parseShopifyCustomer(payload);
}

/** Shopify customers/create */
function parseShopifyCustomer(payload) {
  const firstName = payload.first_name || '';
  const lastName = payload.last_name || '';
  const name = `${firstName} ${lastName}`.trim() || payload.name || '';

  return {
    name,
    mobileNumber: payload.phone || payload.mobile || '',
    email: payload.email || '',
    companyName: (payload.default_address && payload.default_address.company) || payload.company || '',
    formName: payload.form_name || '',
    source: 'shopify',
    metadata: payload,
  };
}

/** Shopify orders/create */
function parseShopifyOrder(payload) {
  const billing = payload.billing_address || {};

  return {
    name: billing.name || '',
    mobileNumber: billing.phone || '',
    email: payload.email || '',
    companyName: billing.company || '',
    formName: 'order',
    source: 'shopify',
    metadata: payload,
  };
}

// ── WordPress ─────────────────────────────────────────────────────────────────

function parseWordPressPayload(payload) {
  // Distinguish WooCommerce (has billing key) from generic WordPress
  if (payload.billing) {
    return parseWooCommerce(payload);
  }
  return parseWordPressGeneric(payload);
}

/** WordPress generic (Contact Form 7, Gravity Forms, custom) */
function parseWordPressGeneric(payload) {
  return {
    name: payload.name || payload['your-name'] || '',
    mobileNumber: payload.phone || payload['your-phone'] || payload.mobile || '',
    email: payload.email || payload['your-email'] || '',
    companyName: payload.company || '',
    formName: payload.form_name || payload.form_id || '',
    source: 'wordpress',
    metadata: payload,
  };
}

/** WooCommerce woocommerce_new_order */
function parseWooCommerce(payload) {
  const billing = payload.billing || {};
  const firstName = billing.first_name || '';
  const lastName = billing.last_name || '';
  const name = `${firstName} ${lastName}`.trim();

  return {
    name,
    mobileNumber: billing.phone || '',
    email: billing.email || '',
    companyName: billing.company || '',
    formName: 'woocommerce_order',
    source: 'wordpress',
    metadata: payload,
  };
}

module.exports = { parseLeadPayload };
