/**
 * Migration 002: Add retry tracking fields to existing recipients
 *
 * For recipients missing phaseNumber: set phaseNumber = null
 * For recipients missing deliveryTimestamp: set deliveryTimestamp = null
 * For recipients missing retryHistory: set retryHistory = []
 *
 * This migration is idempotent — safe to run multiple times.
 * Requirements: Backward compatibility (Task 15.2)
 */

'use strict';

require('dotenv').config();
const mongoose = require('mongoose');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/bulk_whatsapp';

// Minimal recipient schema — only the fields we need for the migration
const recipientSchema = new mongoose.Schema({
    tenantId: String,
    campaignId: String,
    wamid: String,
    to: String,
    status: String,
    phaseNumber: mongoose.Schema.Types.Mixed,
    deliveryTimestamp: mongoose.Schema.Types.Mixed,
    retryHistory: mongoose.Schema.Types.Mixed,
}, { strict: false });

const Recipient = mongoose.model('Recipient', recipientSchema);

async function migrate() {
    console.log('[002] Connecting to MongoDB:', MONGODB_URI.replace(/\/\/.*@/, '//***@'));
    await mongoose.connect(MONGODB_URI);
    console.log('[002] Connected.');

    // ── Step 1: Set phaseNumber = null where missing ──────────────────────────
    const missingPhaseNumber = await Recipient.countDocuments({
        phaseNumber: { $exists: false }
    });
    console.log(`[002] Recipients missing phaseNumber field: ${missingPhaseNumber}`);

    if (missingPhaseNumber > 0) {
        const result = await Recipient.updateMany(
            { phaseNumber: { $exists: false } },
            { $set: { phaseNumber: null } }
        );
        console.log(`[002] Set phaseNumber=null on ${result.modifiedCount} recipients.`);
    }

    // ── Step 2: Set deliveryTimestamp = null where missing ────────────────────
    const missingDeliveryTimestamp = await Recipient.countDocuments({
        deliveryTimestamp: { $exists: false }
    });
    console.log(`[002] Recipients missing deliveryTimestamp field: ${missingDeliveryTimestamp}`);

    if (missingDeliveryTimestamp > 0) {
        const result = await Recipient.updateMany(
            { deliveryTimestamp: { $exists: false } },
            { $set: { deliveryTimestamp: null } }
        );
        console.log(`[002] Set deliveryTimestamp=null on ${result.modifiedCount} recipients.`);
    }

    // ── Step 3: Set retryHistory = [] where missing ───────────────────────────
    const missingRetryHistory = await Recipient.countDocuments({
        retryHistory: { $exists: false }
    });
    console.log(`[002] Recipients missing retryHistory field: ${missingRetryHistory}`);

    if (missingRetryHistory > 0) {
        const result = await Recipient.updateMany(
            { retryHistory: { $exists: false } },
            { $set: { retryHistory: [] } }
        );
        console.log(`[002] Set retryHistory=[] on ${result.modifiedCount} recipients.`);
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    const totalRecipients = await Recipient.countDocuments({});
    console.log(`[002] Migration complete. Total recipients in collection: ${totalRecipients}`);
}

migrate()
    .then(() => {
        console.log('[002] Done.');
        process.exit(0);
    })
    .catch(err => {
        console.error('[002] Migration failed:', err);
        process.exit(1);
    })
    .finally(() => {
        mongoose.disconnect().catch(() => {});
    });
