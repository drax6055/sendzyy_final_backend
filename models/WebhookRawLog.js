const mongoose = require('mongoose');

/**
 * WebhookRawLog Schema
 * Provides write-ahead persistence for incoming raw webhook payloads before acknowledging Meta with 200 OK.
 * 30-Day TTL (2,592,000s) matching Meta Business Manager reporting & audit window.
 */
const webhookRawLogSchema = new mongoose.Schema({
    payload: { type: mongoose.Schema.Types.Mixed, required: true },
    status: {
        type: String,
        enum: ['pending', 'processing', 'processed', 'failed'],
        default: 'pending',
        index: true
    },
    attempts: { type: Number, default: 0 },
    error: { type: String, default: null },
    receivedAt: {
        type: Date,
        default: Date.now,
        expires: 2592000 // 30 days TTL in seconds
    }
}, { timestamps: true });

webhookRawLogSchema.index({ status: 1, createdAt: 1 });

module.exports = mongoose.model('WebhookRawLog', webhookRawLogSchema);
