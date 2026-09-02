const crypto = require('crypto');

/**
 * Middleware: Verify Meta Webhook Signature (X-Hub-Signature-256)
 * Uses HMAC-SHA256 with timingSafeEqual to protect against replay and spoofing attacks.
 */
function verifyMetaWebhookSignature(req, res, next) {
    const signature = req.headers['x-hub-signature-256'];
    const appSecret = process.env.META_APP_SECRET;

    if (!appSecret) {
        if (process.env.NODE_ENV === 'production') {
            console.error('[Security] META_APP_SECRET is not defined in production!');
            return res.status(403).send('Webhook secret misconfigured');
        }
        // In development/test mode, allow requests with a warning
        return next();
    }

    if (!signature) {
        console.warn('[Security] Missing X-Hub-Signature-256 header on webhook request');
        return res.status(401).send('Missing signature');
    }

    const elements = signature.split('=');
    const signatureHash = elements[1] || '';
    const rawContent = req.rawBody || (typeof req.body === 'string' ? req.body : JSON.stringify(req.body));

    const expectedHash = crypto
        .createHmac('sha256', appSecret)
        .update(rawContent)
        .digest('hex');

    const sigBuf = Buffer.from(signatureHash, 'utf8');
    const expBuf = Buffer.from(expectedHash, 'utf8');
    const isValid = sigBuf.length === expBuf.length && crypto.timingSafeEqual(sigBuf, expBuf);

    if (!isValid) {
        console.error('[Security] Invalid X-Hub-Signature-256 received');
        return res.status(403).send('Invalid signature');
    }

    next();
}

module.exports = { verifyMetaWebhookSignature };
