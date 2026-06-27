const mongoose = require('mongoose');

/**
 * RetryScheduler Service
 *
 * Manages scheduling, cancellation, and execution of retry phases for campaigns.
 * Persists scheduled phases to MongoDB for restart resilience and uses atomic
 * findOneAndUpdate to prevent concurrent execution of the same phase.
 *
 * Requirements: 3.1, 3.2, 3.4, 3.5, 5.3, 10.1, 10.2
 */
class RetryScheduler {
    /**
     * Initialize RetryScheduler with required models and optional phase executor.
     *
     * @param {Object} deps - Dependency injection object
     * @param {mongoose.Model} deps.ScheduledRetryPhase - Mongoose model for scheduled phases
     * @param {mongoose.Model} deps.Campaign - Mongoose model for campaigns
     * @param {Object} [deps.phaseExecutor] - Optional PhaseExecutor instance (can be set later)
     */
    constructor({ ScheduledRetryPhase, Campaign, phaseExecutor } = {}) {
        if (!ScheduledRetryPhase) {
            throw new Error('ScheduledRetryPhase model is required');
        }
        if (!Campaign) {
            throw new Error('Campaign model is required');
        }

        this.ScheduledRetryPhase = ScheduledRetryPhase;
        this.Campaign = Campaign;
        this.phaseExecutor = phaseExecutor || null;
    }

    /**
     * Schedule the next retry phase for a campaign.
     *
     * Looks up the campaign's retryConfig to find the intervalHours for the given
     * phaseNumber. Phase 2 uses retryConfig.phases[0].intervalHours, phase 3 uses
     * retryConfig.phases[1].intervalHours, etc.
     *
     * scheduledAt = completionTime + intervalHours * 3600000 (ms)
     *
     * Only schedules if phaseNumber <= retryConfig.phases.length + 1
     * (initial phase 1 + up to 5 retry phases).
     *
     * @param {string} campaignId - Campaign identifier
     * @param {number} phaseNumber - Phase number to schedule (2-6)
     * @param {Date} completionTime - Completion time of the previous phase
     * @returns {Promise<Object>} Created ScheduledRetryPhase document
     * @throws {Error} If campaignId is missing, phaseNumber is invalid, campaign not found,
     *                 or phaseNumber exceeds the maximum configured phases
     *
     * Requirements: 3.1, 3.2, 3.4, 3.5
     */
    async schedulePhase(campaignId, phaseNumber, completionTime) {
        // Validate inputs
        if (!campaignId || typeof campaignId !== 'string') {
            throw new Error('campaignId must be a non-empty string');
        }

        if (typeof phaseNumber !== 'number' || !Number.isInteger(phaseNumber) || phaseNumber < 1) {
            throw new Error('phaseNumber must be an integer >= 1');
        }

        if (!(completionTime instanceof Date) || isNaN(completionTime.getTime())) {
            throw new Error('completionTime must be a valid Date object');
        }

        // Look up the campaign to get retryConfig
        const campaign = await this.Campaign.findOne({ id: campaignId });
        if (!campaign) {
            throw new Error(`Campaign not found: ${campaignId}`);
        }

        const retryConfig = campaign.retryConfig;
        const maxPhases = (retryConfig && retryConfig.phases ? retryConfig.phases.length : 0) + 1;

        // Validate phaseNumber does not exceed max configured phases
        // phaseNumber must be <= phases.length + 1 (initial phase 1 + up to 5 retry phases)
        if (phaseNumber > maxPhases) {
            throw new Error(
                `Phase ${phaseNumber} exceeds maximum configured phases (${maxPhases})`
            );
        }

        // Find the interval for this phase.
        // phaseNumber 1 (grace period evaluation) → retryConfig.phases[0].intervalHours
        // phaseNumber 2 → retryConfig.phases[0], phaseNumber 3 → retryConfig.phases[1], etc.
        const phaseIndex = phaseNumber === 1 ? 0 : phaseNumber - 2;
        const phaseConfig = retryConfig && retryConfig.phases && retryConfig.phases[phaseIndex];

        if (!phaseConfig) {
            if (phaseNumber === 1) {
                // No retry phases configured — skip phase-1 grace-period scheduling.
                // The caller will handle this gracefully (no retries will be attempted).
                console.warn(JSON.stringify({
                    service: 'RetryScheduler',
                    event: 'phase1_scheduling_skipped',
                    campaignId,
                    reason: 'no_retry_phases_configured',
                    message: 'Campaign has no retry phases — skipping phase-1 grace-period scheduling',
                    timestamp: new Date().toISOString()
                }));
                return null;
            }
            throw new Error(
                `No retry configuration found for phase ${phaseNumber} (index ${phaseIndex})`
            );
        }
        const intervalMs = phaseConfig.intervalHours * 3600000;
        const scheduledAt = new Date(completionTime.getTime() + intervalMs);

        // Create the ScheduledRetryPhase document
        const scheduledPhase = await this.ScheduledRetryPhase.create({
            campaignId,
            tenantId: campaign.tenantId,
            phaseNumber,
            scheduledAt,
            status: 'pending'
        });

        console.log(JSON.stringify({
            service: 'RetryScheduler',
            event: 'phase_scheduled',
            campaignId,
            phaseNumber,
            scheduledAt: scheduledAt.toISOString(),
            intervalHours: phaseConfig.intervalHours,
            timestamp: new Date().toISOString()
        }));

        return scheduledPhase;
    }

    /**
     * Clean up orphaned pending phases.
     *
     * Queries ScheduledRetryPhase documents where status = 'pending' and
     * scheduledAt is older than 7 days ago. For each found phase, checks whether
     * the associated campaign still exists and is not in 'completed' status.
     * If the campaign is missing or already completed, the phase is cancelled.
     *
     * This is intended to be called by a daily maintenance cron job to prevent
     * stale pending phases from accumulating in the database.
     *
     * @returns {Promise<number>} Count of cancelled orphaned phases
     *
     * Requirements: System maintenance
     */
    async cleanupOrphanedPhases() {
        const now = new Date();
        const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

        // Find pending phases scheduled more than 7 days ago
        const stalePhases = await this.ScheduledRetryPhase.find({
            status: 'pending',
            scheduledAt: { $lt: sevenDaysAgo }
        });

        if (stalePhases.length === 0) {
            console.log(JSON.stringify({
                service: 'RetryScheduler',
                event: 'cleanup_orphaned_phases_none',
                message: 'No orphaned phases found',
                timestamp: now.toISOString()
            }));
            return 0;
        }

        console.log(JSON.stringify({
            service: 'RetryScheduler',
            event: 'cleanup_orphaned_phases_start',
            staleCount: stalePhases.length,
            timestamp: now.toISOString()
        }));

        let cancelledCount = 0;

        for (const phase of stalePhases) {
            let reason = null;

            // Check if the campaign exists and is not completed
            const campaign = await this.Campaign.findOne({ id: phase.campaignId });

            if (!campaign) {
                reason = 'campaign_not_found';
            } else if (campaign.status === 'completed') {
                reason = 'campaign_completed';
            }

            if (reason) {
                await this.ScheduledRetryPhase.findByIdAndUpdate(phase._id, {
                    $set: { status: 'cancelled' }
                });

                cancelledCount++;

                console.log(JSON.stringify({
                    service: 'RetryScheduler',
                    event: 'cleanup_orphaned_phase_cancelled',
                    campaignId: phase.campaignId,
                    phaseNumber: phase.phaseNumber,
                    scheduledAt: phase.scheduledAt.toISOString(),
                    reason,
                    timestamp: now.toISOString()
                }));
            }
        }

        console.log(JSON.stringify({
            service: 'RetryScheduler',
            event: 'cleanup_orphaned_phases_complete',
            staleCount: stalePhases.length,
            cancelledCount,
            timestamp: now.toISOString()
        }));

        return cancelledCount;
    }

    /**
     * Cancel all pending scheduled phases for a campaign.
     *
     * Used when a campaign completes early (all messages delivered) or reaches
     * the maximum phase count. Updates all pending phases to 'cancelled' status.
     *
     * @param {string} campaignId - Campaign identifier
     * @returns {Promise<number>} Count of cancelled phases
     * @throws {Error} If campaignId is missing
     *
     * Requirements: 5.3
     */
    async cancelPendingPhases(campaignId) {
        if (!campaignId || typeof campaignId !== 'string') {
            throw new Error('campaignId must be a non-empty string');
        }

        const result = await this.ScheduledRetryPhase.updateMany(
            { campaignId, status: 'pending' },
            { $set: { status: 'cancelled' } }
        );

        const cancelledCount = result.modifiedCount;

        if (cancelledCount > 0) {
            console.log(
                `[RetryScheduler] Cancelled ${cancelledCount} pending phase(s) for campaign ${campaignId}`
            );
        }

        return cancelledCount;
    }

    /**
     * Process all due retry phases.
     *
     * Queries ScheduledRetryPhase documents where status = 'pending' and
     * scheduledAt <= now. For each due phase, checks if another phase for the
     * same campaign is already executing (concurrent execution prevention).
     * If concurrent execution is detected, the phase is rescheduled for 5
     * minutes later and a warning is logged.
     *
     * Otherwise, atomically updates status to 'executing' (using findOneAndUpdate)
     * to prevent concurrent execution, then triggers the PhaseExecutor.
     *
     * If no phaseExecutor is available, logs a warning and skips execution
     * (the phase remains in 'executing' status and will need manual recovery).
     *
     * @returns {Promise<void>}
     *
     * Requirements: 3.3, 10.1, 10.2
     */
    async processDuePhases() {
        const now = new Date();

        // Find all due pending phases
        const duePhases = await this.ScheduledRetryPhase.find({
            status: 'pending',
            scheduledAt: { $lte: now }
        });

        if (duePhases.length === 0) {
            return;
        }

        console.log(JSON.stringify({
            service: 'RetryScheduler',
            event: 'process_due_phases_start',
            dueCount: duePhases.length,
            timestamp: now.toISOString()
        }));

        for (const phase of duePhases) {
            // Check for concurrent execution: is another phase for this campaign already executing?
            const concurrentPhase = await this.ScheduledRetryPhase.findOne({
                campaignId: phase.campaignId,
                status: 'executing',
                _id: { $ne: phase._id }
            });

            if (concurrentPhase) {
                // Concurrent execution detected — reschedule for 5 minutes later
                const rescheduledAt = new Date(Date.now() + 5 * 60 * 1000);
                await this.ScheduledRetryPhase.findByIdAndUpdate(phase._id, {
                    $set: { scheduledAt: rescheduledAt }
                });

                console.warn(JSON.stringify({
                    service: 'RetryScheduler',
                    event: 'concurrent_execution_detected',
                    campaignId: phase.campaignId,
                    phaseNumber: phase.phaseNumber,
                    executingPhaseNumber: concurrentPhase.phaseNumber,
                    rescheduledAt: rescheduledAt.toISOString(),
                    message: 'Phase already executing for campaign, rescheduled for 5 minutes later',
                    timestamp: new Date().toISOString()
                }));
                continue;
            }

            await this._executePhase(phase);
        }
    }

    /**
     * Atomically claim and execute a single due phase.
     *
     * Uses findOneAndUpdate to atomically transition status from 'pending' to
     * 'executing', preventing concurrent execution of the same phase.
     *
     * @param {Object} phase - ScheduledRetryPhase document
     * @returns {Promise<void>}
     * @private
     */
    async _executePhase(phase) {
        // Atomically claim the phase: only proceed if it's still 'pending'
        const claimed = await this.ScheduledRetryPhase.findOneAndUpdate(
            { _id: phase._id, status: 'pending' },
            { $set: { status: 'executing' } },
            { new: true }
        );

        if (!claimed) {
            // Another process already claimed this phase
            console.log(
                `[RetryScheduler] Phase ${phase.phaseNumber} for campaign ${phase.campaignId} already claimed by another process, skipping`
            );
            return;
        }

        console.log(JSON.stringify({
            service: 'RetryScheduler',
            event: 'phase_execution_start',
            campaignId: phase.campaignId,
            phaseNumber: phase.phaseNumber,
            scheduledAt: phase.scheduledAt.toISOString(),
            timestamp: new Date().toISOString()
        }));

        // Check if phaseExecutor is available
        if (!this.phaseExecutor) {
            console.warn(
                `[RetryScheduler] No phaseExecutor available. Phase ${phase.phaseNumber} for campaign ${phase.campaignId} is marked as executing but will not be processed.`
            );
            return;
        }

        try {
            await this.phaseExecutor.executePhase(phase.campaignId, phase.phaseNumber);

            // Mark as completed
            await this.ScheduledRetryPhase.findByIdAndUpdate(phase._id, {
                $set: {
                    status: 'completed',
                    executedAt: new Date()
                }
            });

            console.log(JSON.stringify({
                service: 'RetryScheduler',
                event: 'phase_execution_complete',
                campaignId: phase.campaignId,
                phaseNumber: phase.phaseNumber,
                timestamp: new Date().toISOString()
            }));
        } catch (err) {
            // Mark as failed and record error
            await this.ScheduledRetryPhase.findByIdAndUpdate(phase._id, {
                $set: {
                    status: 'failed',
                    executedAt: new Date(),
                    errorMessage: err.message || String(err)
                }
            });

            console.error(JSON.stringify({
                service: 'RetryScheduler',
                event: 'phase_execution_failed',
                campaignId: phase.campaignId,
                phaseNumber: phase.phaseNumber,
                error: err.message,
                stack: err.stack,
                timestamp: new Date().toISOString()
            }));
        }
    }
}

module.exports = RetryScheduler;
