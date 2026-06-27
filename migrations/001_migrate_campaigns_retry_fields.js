/**
 * Migration 001: Add retry fields to existing campaigns
 *
 * For campaigns missing retryConfig: set retryConfig = { version: 0, phases: [] }
 * For campaigns missing status: set status = 'completed'
 * For campaigns missing phaseStats: migrate from successCount/failureCount if present
 * For campaigns missing currentPhase: set currentPhase = 1
 *
 * This migration is idempotent — safe to run multiple times.
 * Requirements: Backward compatibility (Task 15.1)
 */

'use strict';

require('dotenv').config();
const mongoose = require('mongoose');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/bulk_whatsapp';

// Minimal campaign schema — only the fields we need for the migration
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

const Campaign = mongoose.model('Campaign', campaignSchema);

async function migrate() {
    console.log('[001] Connecting to MongoDB:', MONGODB_URI.replace(/\/\/.*@/, '//***@'));
    await mongoose.connect(MONGODB_URI);
    console.log('[001] Connected.');

    // ── Step 1: Add retryConfig where missing ────────────────────────────────
    const missingRetryConfig = await Campaign.countDocuments({
        $or: [
            { retryConfig: { $exists: false } },
            { retryConfig: null }
        ]
    });
    console.log(`[001] Campaigns missing retryConfig: ${missingRetryConfig}`);

    if (missingRetryConfig > 0) {
        const result = await Campaign.updateMany(
            {
                $or: [
                    { retryConfig: { $exists: false } },
                    { retryConfig: null }
                ]
            },
            {
                $set: {
                    retryConfig: { version: 0, phases: [] }
                }
            }
        );
        console.log(`[001] Set retryConfig on ${result.modifiedCount} campaigns.`);
    }

    // ── Step 2: Set status = 'completed' where missing ────────────────────────
    const missingStatus = await Campaign.countDocuments({
        $or: [
            { status: { $exists: false } },
            { status: null },
            { status: '' }
        ]
    });
    console.log(`[001] Campaigns missing status: ${missingStatus}`);

    if (missingStatus > 0) {
        const result = await Campaign.updateMany(
            {
                $or: [
                    { status: { $exists: false } },
                    { status: null },
                    { status: '' }
                ]
            },
            {
                $set: { status: 'completed' }
            }
        );
        console.log(`[001] Set status='completed' on ${result.modifiedCount} campaigns.`);
    }

    // ── Step 3: Set currentPhase = 1 where missing ───────────────────────────
    const missingCurrentPhase = await Campaign.countDocuments({
        $or: [
            { currentPhase: { $exists: false } },
            { currentPhase: null }
        ]
    });
    console.log(`[001] Campaigns missing currentPhase: ${missingCurrentPhase}`);

    if (missingCurrentPhase > 0) {
        const result = await Campaign.updateMany(
            {
                $or: [
                    { currentPhase: { $exists: false } },
                    { currentPhase: null }
                ]
            },
            {
                $set: { currentPhase: 1 }
            }
        );
        console.log(`[001] Set currentPhase=1 on ${result.modifiedCount} campaigns.`);
    }

    // ── Step 4: Migrate phaseStats where missing ─────────────────────────────
    // For campaigns that have successCount/failureCount but no phaseStats,
    // create a phase 1 entry from those counts.
    const missingPhaseStats = await Campaign.countDocuments({
        $or: [
            { phaseStats: { $exists: false } },
            { phaseStats: null }
        ]
    });
    console.log(`[001] Campaigns missing phaseStats: ${missingPhaseStats}`);

    if (missingPhaseStats > 0) {
        // Fetch campaigns that need phaseStats migration
        const campaigns = await Campaign.find(
            {
                $or: [
                    { phaseStats: { $exists: false } },
                    { phaseStats: null }
                ]
            },
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
            const result = await Campaign.bulkWrite(bulkOps);
            console.log(`[001] Set phaseStats on ${result.modifiedCount} campaigns.`);
            console.log(`[001]   - ${withCounts} campaigns migrated with existing counts as phase 1 entry.`);
            console.log(`[001]   - ${withoutCounts} campaigns set to empty phaseStats array.`);
        }
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    const totalCampaigns = await Campaign.countDocuments({});
    console.log(`[001] Migration complete. Total campaigns in collection: ${totalCampaigns}`);
}

migrate()
    .then(() => {
        console.log('[001] Done.');
        process.exit(0);
    })
    .catch(err => {
        console.error('[001] Migration failed:', err);
        process.exit(1);
    })
    .finally(() => {
        mongoose.disconnect().catch(() => {});
    });
