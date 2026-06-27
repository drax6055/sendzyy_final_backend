/**
 * Migration Runner
 *
 * Runs all migrations in order. Each migration is idempotent and safe to re-run.
 *
 * Usage (from the backend/ directory):
 *   node migrations/index.js
 *
 * Or run individual migrations:
 *   node migrations/001_migrate_campaigns_retry_fields.js
 *   node migrations/002_migrate_recipients_retry_fields.js
 */

'use strict';

require('dotenv').config();
const mongoose = require('mongoose');
const path = require('path');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/bulk_whatsapp';

// Ordered list of migration modules (relative to this file)
const MIGRATIONS = [
    './001_migrate_campaigns_retry_fields',
    './002_migrate_recipients_retry_fields',
];

async function runAll() {
    console.log('=== Campaign Retry Phases — Database Migrations ===');
    console.log(`Connecting to: ${MONGODB_URI.replace(/\/\/.*@/, '//***@')}`);

    await mongoose.connect(MONGODB_URI);
    console.log('Connected to MongoDB.\n');

    for (const migrationPath of MIGRATIONS) {
        const name = path.basename(migrationPath);
        console.log(`--- Running migration: ${name} ---`);

        try {
            // Each migration script calls process.exit() when run standalone,
            // so we require() the core logic by importing the module's migrate()
            // function directly. Since the scripts are written as standalone
            // executables, we re-implement the runner here using the same logic
            // but without re-connecting (connection is already open).
            await runMigration(migrationPath);
            console.log(`--- Completed: ${name} ---\n`);
        } catch (err) {
            console.error(`--- FAILED: ${name} ---`);
            console.error(err);
            process.exit(1);
        }
    }

    console.log('=== All migrations completed successfully. ===');
}

/**
 * Run a single migration by executing its logic inline.
 * We duplicate the migration logic here so the runner doesn't trigger
 * the standalone process.exit() calls in each script.
 */
async function runMigration(migrationPath) {
    const name = path.basename(migrationPath);

    if (name.startsWith('001')) {
        await migrateCampaigns();
    } else if (name.startsWith('002')) {
        await migrateRecipients();
    } else {
        throw new Error(`Unknown migration: ${name}`);
    }
}

// ── Campaign migration logic (mirrors 001_migrate_campaigns_retry_fields.js) ──

const campaignSchema = new mongoose.Schema({
    tenantId: String,
    id: String,
    successCount: Number,
    failureCount: Number,
    retryConfig: mongoose.Schema.Types.Mixed,
    status: String,
    currentPhase: Number,
    phaseStats: mongoose.Schema.Types.Mixed,
}, { strict: false, timestamps: true });

// Use existing model if already registered (idempotent require)
const Campaign = mongoose.models.Campaign || mongoose.model('Campaign', campaignSchema);

async function migrateCampaigns() {
    // retryConfig
    const missingRetryConfig = await Campaign.countDocuments({
        $or: [{ retryConfig: { $exists: false } }, { retryConfig: null }]
    });
    console.log(`[001] Campaigns missing retryConfig: ${missingRetryConfig}`);
    if (missingRetryConfig > 0) {
        const r = await Campaign.updateMany(
            { $or: [{ retryConfig: { $exists: false } }, { retryConfig: null }] },
            { $set: { retryConfig: { version: 0, phases: [] } } }
        );
        console.log(`[001] Set retryConfig on ${r.modifiedCount} campaigns.`);
    }

    // status
    const missingStatus = await Campaign.countDocuments({
        $or: [{ status: { $exists: false } }, { status: null }, { status: '' }]
    });
    console.log(`[001] Campaigns missing status: ${missingStatus}`);
    if (missingStatus > 0) {
        const r = await Campaign.updateMany(
            { $or: [{ status: { $exists: false } }, { status: null }, { status: '' }] },
            { $set: { status: 'completed' } }
        );
        console.log(`[001] Set status='completed' on ${r.modifiedCount} campaigns.`);
    }

    // currentPhase
    const missingCurrentPhase = await Campaign.countDocuments({
        $or: [{ currentPhase: { $exists: false } }, { currentPhase: null }]
    });
    console.log(`[001] Campaigns missing currentPhase: ${missingCurrentPhase}`);
    if (missingCurrentPhase > 0) {
        const r = await Campaign.updateMany(
            { $or: [{ currentPhase: { $exists: false } }, { currentPhase: null }] },
            { $set: { currentPhase: 1 } }
        );
        console.log(`[001] Set currentPhase=1 on ${r.modifiedCount} campaigns.`);
    }

    // phaseStats
    const missingPhaseStats = await Campaign.countDocuments({
        $or: [{ phaseStats: { $exists: false } }, { phaseStats: null }]
    });
    console.log(`[001] Campaigns missing phaseStats: ${missingPhaseStats}`);
    if (missingPhaseStats > 0) {
        const campaigns = await Campaign.find(
            { $or: [{ phaseStats: { $exists: false } }, { phaseStats: null }] },
            { _id: 1, successCount: 1, failureCount: 1 }
        ).lean();

        let withCounts = 0;
        let withoutCounts = 0;

        const bulkOps = campaigns.map(campaign => {
            const hasSuccessCount = typeof campaign.successCount === 'number';
            const hasFailureCount = typeof campaign.failureCount === 'number';
            let phaseStats;
            if (hasSuccessCount || hasFailureCount) {
                withCounts++;
                phaseStats = [{
                    phaseNumber: 1,
                    successCount: campaign.successCount || 0,
                    failureCount: campaign.failureCount || 0,
                    executedAt: null,
                    completedAt: null
                }];
            } else {
                withoutCounts++;
                phaseStats = [];
            }
            return {
                updateOne: {
                    filter: { _id: campaign._id },
                    update: { $set: { phaseStats } }
                }
            };
        });

        if (bulkOps.length > 0) {
            const r = await Campaign.bulkWrite(bulkOps);
            console.log(`[001] Set phaseStats on ${r.modifiedCount} campaigns.`);
            console.log(`[001]   - ${withCounts} with existing counts migrated as phase 1 entry.`);
            console.log(`[001]   - ${withoutCounts} set to empty phaseStats array.`);
        }
    }

    const total = await Campaign.countDocuments({});
    console.log(`[001] Done. Total campaigns: ${total}`);
}

// ── Recipient migration logic (mirrors 002_migrate_recipients_retry_fields.js) ─

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

const Recipient = mongoose.models.Recipient || mongoose.model('Recipient', recipientSchema);

async function migrateRecipients() {
    // phaseNumber
    const missingPhaseNumber = await Recipient.countDocuments({ phaseNumber: { $exists: false } });
    console.log(`[002] Recipients missing phaseNumber: ${missingPhaseNumber}`);
    if (missingPhaseNumber > 0) {
        const r = await Recipient.updateMany(
            { phaseNumber: { $exists: false } },
            { $set: { phaseNumber: null } }
        );
        console.log(`[002] Set phaseNumber=null on ${r.modifiedCount} recipients.`);
    }

    // deliveryTimestamp
    const missingDeliveryTimestamp = await Recipient.countDocuments({ deliveryTimestamp: { $exists: false } });
    console.log(`[002] Recipients missing deliveryTimestamp: ${missingDeliveryTimestamp}`);
    if (missingDeliveryTimestamp > 0) {
        const r = await Recipient.updateMany(
            { deliveryTimestamp: { $exists: false } },
            { $set: { deliveryTimestamp: null } }
        );
        console.log(`[002] Set deliveryTimestamp=null on ${r.modifiedCount} recipients.`);
    }

    // retryHistory
    const missingRetryHistory = await Recipient.countDocuments({ retryHistory: { $exists: false } });
    console.log(`[002] Recipients missing retryHistory: ${missingRetryHistory}`);
    if (missingRetryHistory > 0) {
        const r = await Recipient.updateMany(
            { retryHistory: { $exists: false } },
            { $set: { retryHistory: [] } }
        );
        console.log(`[002] Set retryHistory=[] on ${r.modifiedCount} recipients.`);
    }

    const total = await Recipient.countDocuments({});
    console.log(`[002] Done. Total recipients: ${total}`);
}

// ── Entry point ───────────────────────────────────────────────────────────────

runAll()
    .then(() => {
        process.exit(0);
    })
    .catch(err => {
        console.error('Migration runner failed:', err);
        process.exit(1);
    })
    .finally(() => {
        mongoose.disconnect().catch(() => {});
    });
