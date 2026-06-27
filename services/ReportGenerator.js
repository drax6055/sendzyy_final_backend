const mongoose = require('mongoose');

/**
 * ReportGenerator Service
 * 
 * Generates phase-wise delivery reports for campaigns with retry phases.
 * Aggregates statistics from campaign phaseStats and scheduled phases.
 * 
 * Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7
 */
class ReportGenerator {
    /**
     * Initialize ReportGenerator with required models
     * @param {mongoose.Model} Campaign - Campaign model
     * @param {mongoose.Model} ScheduledRetryPhase - ScheduledRetryPhase model
     * @param {mongoose.Model} [Recipient] - Recipient model (optional, falls back to db lookup)
     */
    constructor(Campaign, ScheduledRetryPhase, Recipient = null) {
        if (!Campaign) {
            throw new Error('Campaign model is required');
        }
        if (!ScheduledRetryPhase) {
            throw new Error('ScheduledRetryPhase model is required');
        }
        this.Campaign = Campaign;
        this.ScheduledRetryPhase = ScheduledRetryPhase;
        this.Recipient = Recipient;
    }

    /**
     * Generate phase-wise delivery report for a campaign
     * 
     * Aggregates:
     * - Phase-wise success/failure counts and rates
     * - Cumulative success count across all phases
     * - Overall success rate for the campaign
     * - Scheduled phases with pending status
     * - Time intervals between phases
     * 
     * @param {string} campaignId - Campaign identifier
     * @param {string} tenantId - Tenant identifier (for authorization)
     * @returns {Promise<Object>} Report object with phase statistics
     * @throws {Error} If campaign not found or inputs invalid
     * 
     * Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7
     */
    async generatePhaseReport(campaignId, tenantId) {
        // Validate inputs
        if (!campaignId || typeof campaignId !== 'string') {
            throw new Error('campaignId must be a non-empty string');
        }
        if (!tenantId || typeof tenantId !== 'string') {
            throw new Error('tenantId must be a non-empty string');
        }

        // Fetch campaign
        const campaign = await this.Campaign.findOne({ 
            id: campaignId,
            tenantId 
        });

        if (!campaign) {
            throw new Error(`Campaign not found: ${campaignId}`);
        }

        // Get total unique recipients count — count distinct recipients in the Recipient collection
        // rather than campaign.totalCount which gets inflated by retry sends.
        let totalRecipients;
        if (this.Recipient) {
            totalRecipients = await this.Recipient.countDocuments({ campaignId });
        } else {
            totalRecipients = campaign.totalCount || 0;
        }

        // Build phase report array
        const phases = [];
        let cumulativeSuccess = 0;

        // Process executed phases from phaseStats (Requirements 7.2, 7.3)
        // Use live Recipient counts for accuracy — phaseStats snapshots may be stale
        // if delivery webhooks arrived after the snapshot was taken.
        if (campaign.phaseStats && Array.isArray(campaign.phaseStats)) {
            for (const phaseStat of campaign.phaseStats) {

                // Live counts from Recipient collection
                let liveSuccess = phaseStat.successCount;
                let liveFailure = phaseStat.failureCount;

                if (this.Recipient) {
                    // successCount = recipients delivered in this phase
                    liveSuccess = await this.Recipient.countDocuments({
                        campaignId,
                        phaseNumber: phaseStat.phaseNumber
                    });

                    // failureCount = recipients attempted in this phase but not yet delivered
                    // (phaseNumber still null, with a retryHistory entry for this phase)
                    liveFailure = await this.Recipient.countDocuments({
                        campaignId,
                        phaseNumber: null,
                        'retryHistory.phaseNumber': phaseStat.phaseNumber
                    });
                }

                const totalAttempts = liveSuccess + liveFailure;
                const successRate = totalAttempts > 0
                    ? (liveSuccess / totalAttempts) * 100
                    : 0;

                cumulativeSuccess += liveSuccess;

                // Get interval hours for this phase (Requirement 7.7)
                let intervalHours = null;
                if (campaign.retryConfig && 
                    campaign.retryConfig.phases && 
                    phaseStat.phaseNumber > 1) {
                    // For phase N, get interval from phase N-1 in config
                    const configPhase = campaign.retryConfig.phases.find(
                        p => p.phaseNumber === phaseStat.phaseNumber - 1
                    );
                    if (configPhase) {
                        intervalHours = configPhase.intervalHours;
                    }
                }

                phases.push({
                    phaseNumber: phaseStat.phaseNumber,
                    successCount: liveSuccess,
                    failureCount: liveFailure,
                    successRate: parseFloat(successRate.toFixed(2)),
                    executedAt: phaseStat.executedAt,
                    completedAt: phaseStat.completedAt,
                    status: 'completed',
                    intervalHours
                });
            }
        }

        // Query scheduled phases with pending status (Requirement 7.6)
        // Only show pending phases if the campaign is still active (not completed/error).
        // Include phaseNumber=1 (grace-period evaluation) — when pending, it means a retry
        // is coming and should be shown to the user as an upcoming retry phase.
        const scheduledPhases = campaign.status === 'completed' || campaign.status === 'error'
            ? []
            : await this.ScheduledRetryPhase.find({
                campaignId,
                status: 'pending',
            }).sort({ phaseNumber: 1 });

        // Add scheduled phases to report
        for (const scheduledPhase of scheduledPhases) {
            // Get interval hours for this phase.
            // phaseNumber 1 (grace-period) → retryConfig.phases[0]
            // phaseNumber 2 → retryConfig.phases[0], phaseNumber 3 → retryConfig.phases[1], etc.
            let intervalHours = null;
            if (campaign.retryConfig && campaign.retryConfig.phases) {
                const phaseIndex = scheduledPhase.phaseNumber === 1 ? 0 : scheduledPhase.phaseNumber - 2;
                const configPhase = campaign.retryConfig.phases[phaseIndex];
                if (configPhase) intervalHours = configPhase.intervalHours;
            }

            // Phase 1 is the grace-period evaluation — display it as "Retry Phase 2 upcoming"
            // by using phaseNumber 2 in the report so the user sees the correct next phase label.
            const displayPhaseNumber = scheduledPhase.phaseNumber === 1 ? 2 : scheduledPhase.phaseNumber;

            // Avoid duplicating a phase already in phaseStats
            const alreadyAdded = phases.some(p => p.phaseNumber === displayPhaseNumber);
            if (!alreadyAdded) {
                phases.push({
                    phaseNumber: displayPhaseNumber,
                    scheduledAt: scheduledPhase.scheduledAt,
                    status: 'pending',
                    intervalHours
                });
            }
        }

        // Sort phases by phase number
        phases.sort((a, b) => a.phaseNumber - b.phaseNumber);

        // Calculate overall success rate (Requirement 7.5)
        const overallSuccessRate = totalRecipients > 0 
            ? (cumulativeSuccess / totalRecipients) * 100 
            : 0;

        // Calculate remaining retries
        const maxPhases = campaign.retryConfig?.phases?.length || 0;
        const remainingRetries = Math.max(0, maxPhases + 1 - campaign.currentPhase);

        // Build report object
        const report = {
            campaignId: campaign.id,
            status: campaign.status,
            currentPhase: campaign.currentPhase,
            totalRecipients, // Requirement 7.1
            phases, // Requirements 7.2, 7.3, 7.6, 7.7
            cumulativeSuccess, // Requirement 7.4
            overallSuccessRate: parseFloat(overallSuccessRate.toFixed(2)), // Requirement 7.5
            remainingRetries
        };

        return report;
    }

    /**
     * Get messages for a specific phase with pagination
     * 
     * @param {string} campaignId - Campaign identifier
     * @param {string} tenantId - Tenant identifier (for authorization)
     * @param {number|null} phaseNumber - Phase number to filter (null for all)
     * @param {number} page - Page number (1-indexed)
     * @param {number} limit - Items per page
     * @returns {Promise<Object>} Paginated message list
     * @throws {Error} If inputs invalid
     * 
     * Requirements: 6.4
     */
    async getMessagesByPhase(campaignId, tenantId, phaseNumber = null, page = 1, limit = 50) {
        // Validate inputs
        if (!campaignId || typeof campaignId !== 'string') {
            throw new Error('campaignId must be a non-empty string');
        }
        if (!tenantId || typeof tenantId !== 'string') {
            throw new Error('tenantId must be a non-empty string');
        }
        if (phaseNumber !== null && (typeof phaseNumber !== 'number' || phaseNumber < 1 || !Number.isInteger(phaseNumber))) {
            throw new Error('phaseNumber must be a positive integer or null');
        }
        if (typeof page !== 'number' || page < 1 || !Number.isInteger(page)) {
            throw new Error('page must be a positive integer');
        }
        if (typeof limit !== 'number' || limit < 1 || limit > 1000 || !Number.isInteger(limit)) {
            throw new Error('limit must be a positive integer between 1 and 1000');
        }

        // Verify campaign exists and belongs to tenant
        const campaign = await this.Campaign.findOne({ 
            id: campaignId,
            tenantId 
        });

        if (!campaign) {
            throw new Error(`Campaign not found: ${campaignId}`);
        }

        // Build query filter
        const filter = {
            campaignId,
            tenantId
        };

        if (phaseNumber !== null) {
            filter.phaseNumber = phaseNumber;
        }

        // Get Recipient model from Campaign model's connection or from stored reference
        const Recipient = this.Recipient || this.Campaign.db.model('Recipient');

        // Calculate skip for pagination
        const skip = (page - 1) * limit;

        // Query messages with pagination
        const messages = await Recipient.find(filter)
            .sort({ deliveryTimestamp: 1, _id: 1 })
            .skip(skip)
            .limit(limit)
            .select('to status phaseNumber deliveryTimestamp sentAt deliveredAt readAt failedAt retryHistory')
            .lean();

        // Get total count for pagination
        const total = await Recipient.countDocuments(filter);

        return {
            messages,
            pagination: {
                page,
                limit,
                total,
                totalPages: Math.ceil(total / limit)
            }
        };
    }
}

module.exports = ReportGenerator;
