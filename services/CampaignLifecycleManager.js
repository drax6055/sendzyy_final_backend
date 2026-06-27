/**
 * CampaignLifecycleManager Service
 *
 * Manages the full lifecycle of a campaign from creation through completion:
 *   - Captures the active retry configuration snapshot at campaign creation time
 *   - Handles phase 1 completion: updates stats, transitions status, schedules phase 2
 *   - Handles campaign completion: cancels pending phases, marks failed messages,
 *     calculates final delivery statistics
 *
 * All Mongoose models and external services are injected via the constructor so
 * the class is fully unit-testable without a live database.
 *
 * Requirements: 1.4, 2.1, 2.4, 2.5, 3.1, 5.1, 5.2, 5.3, 5.4, 5.5, 8.1, 8.2
 */
const SocketEmitter = require('./SocketEmitter');

class CampaignLifecycleManager {
    /**
     * @param {Object} deps
     * @param {import('mongoose').Model} deps.Campaign
     * @param {import('mongoose').Model} deps.Recipient
     * @param {Object} deps.configurationManager  - ConfigurationManager instance
     * @param {Object} [deps.retryScheduler]       - RetryScheduler instance (optional)
     */
    constructor({ Campaign, Recipient, configurationManager, retryScheduler } = {}) {
        if (!Campaign) throw new Error('Campaign model is required');
        if (!Recipient) throw new Error('Recipient model is required');
        if (!configurationManager) throw new Error('configurationManager is required');

        this.Campaign = Campaign;
        this.Recipient = Recipient;
        this.configurationManager = configurationManager;
        this.retryScheduler = retryScheduler || null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Task 7.1 — Campaign creation: capture retry configuration snapshot
    // Requirements: 1.4, 2.1, 8.1, 8.2
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Build the retry-config snapshot and initial phase fields to embed in a
     * new Campaign document.
     *
     * Call this before (or during) campaign creation and merge the returned
     * object into the Campaign.create() payload.
     *
     * @returns {Promise<Object>} Fields to merge into the campaign document:
     *   { retryConfig, status, currentPhase, phaseStats }
     *
     * Requirements: 1.4, 2.1, 8.1, 8.2
     */
    async buildCampaignCreationFields(tenantId) {
        if (!tenantId) throw new Error('tenantId is required');

        // Query the currently active RetryConfiguration for this tenant
        let activeConfig = await this.configurationManager.getActiveConfiguration(tenantId);

        // If no configuration exists yet, fall back to a zero-phase default
        if (!activeConfig) {
            console.warn(
                '[CampaignLifecycleManager] No active retry configuration found. ' +
                'Creating default zero-phase configuration.'
            );
            activeConfig = await this.configurationManager.createConfiguration({ phases: [] }, tenantId);
        }

        return {
            // Immutable snapshot of the config at creation time (Req 8.2, 8.3, 8.4)
            retryConfig: {
                version: activeConfig.version,
                phases: (activeConfig.phases || []).map(p => ({
                    phaseNumber: p.phaseNumber,
                    intervalHours: p.intervalHours
                }))
            },
            status: 'initial',       // Req 2.1
            currentPhase: 1,         // Req 2.1
            phaseStats: [{           // Pre-initialise phase 1 stats entry
                phaseNumber: 1,
                successCount: 0,
                failureCount: 0,
                executedAt: new Date()
            }]
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Task 7.3 — Phase 1 completion logic
    // Requirements: 2.4, 2.5, 3.1
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Handle the completion of phase 1 for a campaign.
     *
     * Called by the scheduler AFTER the grace period (phase 1 retry interval),
     * NOT immediately after sending. This gives delivery webhooks time to arrive.
     *
     * Steps:
     *  1. Count successful and failed/stale messages for phase 1
     *  2. Update phaseStats[0] with the final counts and completedAt timestamp
     *  3. If failures OR stale 'sent' messages exist AND retries are configured
     *     → status = 'retrying', schedule phase 2 via RetryScheduler
     *  4. If no undelivered messages → status = 'completed'
     *
     * @param {string} campaignId  - Campaign identifier (campaign.id field)
     * @param {string} tenantId    - Tenant identifier
     * @returns {Promise<Object>}  Result: { status, successCount, failureCount, scheduledPhase? }
     *
     * Requirements: 2.4, 2.5, 3.1
     */
    async handlePhase1Completion(campaignId, tenantId) {
        if (!campaignId) throw new Error('campaignId is required');
        if (!tenantId) throw new Error('tenantId is required');

        const campaign = await this.Campaign.findOne({ id: campaignId, tenantId });
        if (!campaign) throw new Error(`Campaign not found: ${campaignId}`);

        // Count phase 1 outcomes from the Recipient collection.
        // Successful = phaseNumber = 1 (set by MessageTracker on delivery webhook).
        // Failed     = phaseNumber is null AND (status = 'failed' OR status = 'sent').
        //   - 'failed': hard API rejection from WhatsApp.
        //   - 'sent': no delivery confirmation within the grace period — treat as failed for retry.
        //   This method is only called by the scheduler AFTER the grace period, so any
        //   remaining 'sent' messages are genuinely undelivered and should be retried.
        const [successCount, failureCount] = await Promise.all([
            this.Recipient.countDocuments({ campaignId, phaseNumber: 1 }),
            this.Recipient.countDocuments({
                campaignId,
                phaseNumber: null,
                $or: [{ status: 'failed' }, { status: 'sent' }]
            })
        ]);

        const completedAt = new Date();

        // Update phaseStats for phase 1 (Req 2.4)
        await this.Campaign.updateOne(
            { id: campaignId, 'phaseStats.phaseNumber': 1 },
            {
                $set: {
                    'phaseStats.$.successCount': successCount,
                    'phaseStats.$.failureCount': failureCount,
                    'phaseStats.$.completedAt': completedAt
                }
            }
        );

        const hasRetryPhases = campaign.retryConfig &&
            Array.isArray(campaign.retryConfig.phases) &&
            campaign.retryConfig.phases.length > 0;

        // Req 2.5: transition to 'retrying' if failures exist and retries configured
        if (failureCount > 0 && hasRetryPhases) {
            await this.Campaign.updateOne(
                { id: campaignId },
                { $set: { status: 'retrying' } }
            );

            console.log(JSON.stringify({
                service: 'CampaignLifecycleManager',
                event: 'campaign_status_transition',
                campaignId,
                fromStatus: 'initial',
                toStatus: 'retrying',
                successCount,
                failureCount,
                timestamp: new Date().toISOString()
            }));

            // Req 3.1: schedule phase 2
            let scheduledPhase = null;
            if (this.retryScheduler) {
                try {
                    scheduledPhase = await this.retryScheduler.schedulePhase(
                        campaignId,
                        2,           // next phase number
                        completedAt  // schedule relative to phase 1 completion
                    );
                    console.log(JSON.stringify({
                        service: 'CampaignLifecycleManager',
                        event: 'phase_scheduled',
                        campaignId,
                        phaseNumber: 2,
                        scheduledAt: scheduledPhase.scheduledAt.toISOString(),
                        timestamp: new Date().toISOString()
                    }));
                } catch (schedErr) {
                    console.error(
                        `[CampaignLifecycleManager] Failed to schedule phase 2 for campaign ${campaignId}: ` +
                        schedErr.message
                    );
                }
            }

            return { status: 'retrying', successCount, failureCount, scheduledPhase };
        }

        // No failures (or no retry phases configured) → complete the campaign
        await this._completeCampaign(campaignId, tenantId, completedAt);

        return { status: 'completed', successCount, failureCount };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Task 7.5 — Campaign completion logic
    // Requirements: 5.1, 5.2, 5.3, 5.4, 5.5
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Explicitly complete a campaign (e.g. after the final retry phase).
     *
     * This is the public entry-point for completing a campaign from outside
     * (e.g. PhaseExecutor calls this after the last phase).  Internally it
     * delegates to _completeCampaign.
     *
     * @param {string} campaignId
     * @param {string} tenantId
     * @returns {Promise<Object>} Final statistics
     *
     * Requirements: 5.1, 5.2, 5.3, 5.4, 5.5
     */
    async completeCampaign(campaignId, tenantId) {
        if (!campaignId) throw new Error('campaignId is required');
        if (!tenantId) throw new Error('tenantId is required');
        return this._completeCampaign(campaignId, tenantId, new Date());
    }

    /**
     * Internal completion handler shared by handlePhase1Completion and completeCampaign.
     *
     * Steps:
     *  1. Transition campaign status to 'completed'  (Req 5.1, 5.2)
     *  2. Cancel all pending scheduled phases         (Req 5.3)
     *  3. Mark remaining failed messages as permanently failed (Req 5.4)
     *  4. Calculate and store final delivery statistics       (Req 5.5)
     *
     * @param {string} campaignId
     * @param {string} tenantId
     * @param {Date}   completedAt
     * @returns {Promise<Object>} { totalSuccess, totalFailure, overallSuccessRate }
     * @private
     */
    async _completeCampaign(campaignId, tenantId, completedAt) {
        // 1. Transition to completed (Req 5.1, 5.2)
        await this.Campaign.updateOne(
            { id: campaignId },
            { $set: { status: 'completed' } }
        );

        console.log(JSON.stringify({
            service: 'CampaignLifecycleManager',
            event: 'campaign_status_transition',
            campaignId,
            toStatus: 'completed',
            timestamp: new Date().toISOString()
        }));

        // 2. Cancel pending scheduled phases (Req 5.3)
        if (this.retryScheduler) {
            try {
                const cancelled = await this.retryScheduler.cancelPendingPhases(campaignId);
                if (cancelled > 0) {
                    console.log(
                        `[CampaignLifecycleManager] Cancelled ${cancelled} pending phase(s) for campaign ${campaignId}`
                    );
                }
            } catch (err) {
                console.error(
                    `[CampaignLifecycleManager] Failed to cancel pending phases for campaign ${campaignId}: ${err.message}`
                );
            }
        }

        // 3. Mark remaining undelivered messages as permanently failed (Req 5.4)
        // Messages with phaseNumber = null were never successfully delivered.
        const permanentFailureResult = await this.Recipient.updateMany(
            { campaignId, phaseNumber: null },
            { $set: { status: 'failed', failedAt: completedAt.toISOString() } }
        );

        if (permanentFailureResult.modifiedCount > 0) {
            console.log(
                `[CampaignLifecycleManager] Marked ${permanentFailureResult.modifiedCount} ` +
                `message(s) as permanently failed for campaign ${campaignId}`
            );
        }

        // 4. Calculate and store final delivery statistics (Req 5.5)
        const finalStats = await this._calculateFinalStats(campaignId);

        await this.Campaign.updateOne(
            { id: campaignId },
            {
                $set: {
                    successCount: finalStats.totalSuccess,
                    failureCount: finalStats.totalFailure
                }
            }
        );

        console.log(
            `[CampaignLifecycleManager] Final stats for campaign ${campaignId}: ` +
            `${finalStats.totalSuccess} delivered, ${finalStats.totalFailure} failed ` +
            `(${finalStats.overallSuccessRate.toFixed(1)}% success rate)`
        );

        // Emit campaign:completed event for real-time UI updates (Task 14.1)
        SocketEmitter.emitCampaignCompleted(tenantId, campaignId, {
            totalSuccess: finalStats.totalSuccess,
            totalFailure: finalStats.totalFailure,
            overallSuccessRate: finalStats.overallSuccessRate,
            phases: []
        });

        return finalStats;
    }

    /**
     * Aggregate final delivery statistics across all phases.
     *
     * totalSuccess = sum of phaseStats[*].successCount
     * totalFailure = count of recipients with phaseNumber = null at completion
     * overallSuccessRate = (totalSuccess / totalRecipients) * 100
     *
     * @param {string} campaignId
     * @returns {Promise<Object>} { totalSuccess, totalFailure, totalRecipients, overallSuccessRate }
     * @private
     *
     * Requirements: 5.5
     */
    async _calculateFinalStats(campaignId) {
        const campaign = await this.Campaign.findOne({ id: campaignId });

        const totalSuccess = (campaign && campaign.phaseStats)
            ? campaign.phaseStats.reduce((sum, ps) => sum + (ps.successCount || 0), 0)
            : 0;

        const totalFailure = await this.Recipient.countDocuments({
            campaignId,
            phaseNumber: null
        });

        const totalRecipients = totalSuccess + totalFailure;
        const overallSuccessRate = totalRecipients > 0
            ? (totalSuccess / totalRecipients) * 100
            : 0;

        return { totalSuccess, totalFailure, totalRecipients, overallSuccessRate };
    }
}

module.exports = CampaignLifecycleManager;
