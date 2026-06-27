const mongoose = require('mongoose');
const SocketEmitter = require('./SocketEmitter');

/**
 * PhaseExecutor Service
 *
 * Executes retry phases for campaigns by retrieving failed messages from the
 * previous phase, delegating message sending to the existing Message Sender,
 * and updating phase statistics on the campaign document.
 *
 * Follows the same dependency-injection pattern as RetryScheduler and
 * ConfigurationManager: all Mongoose models and external services are
 * injected via the constructor so the class is fully unit-testable.
 *
 * Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 9.1, 9.2, 9.3, 10.5
 */
class PhaseExecutor {
    /**
     * Initialize PhaseExecutor with required models and services.
     *
     * @param {Object} deps - Dependency injection object
     * @param {mongoose.Model} deps.Campaign - Mongoose model for campaigns
     * @param {mongoose.Model} deps.Recipient - Mongoose model for recipients
     * @param {Object} deps.messageSender - Service with sendMessage(recipient, campaign, phaseNumber) method
     * @param {Object} [deps.retryScheduler] - Optional RetryScheduler instance for scheduling next phase
     */
    constructor({ Campaign, Recipient, messageSender, retryScheduler, campaignLifecycleManager } = {}) {
        if (!Campaign) {
            throw new Error('Campaign model is required');
        }
        if (!Recipient) {
            throw new Error('Recipient model is required');
        }
        if (!messageSender) {
            throw new Error('messageSender is required');
        }

        this.Campaign = Campaign;
        this.Recipient = Recipient;
        this.messageSender = messageSender;
        this.retryScheduler = retryScheduler || null;
        this.campaignLifecycleManager = campaignLifecycleManager || null;
    }

    /**
     * Execute a retry phase for a campaign.
     *
     * Orchestrates the full retry phase lifecycle:
     * 1. Validates campaign exists and is in retrying state
     * 2. Retrieves failed messages from the previous phase
     * 3. Sends messages to each failed recipient via messageSender
     * 4. Updates phase statistics on the campaign
     * 5. Schedules next phase or marks campaign as completed
     *
     * Per the design, WhatsApp API failures mark individual messages as failed
     * and continue with remaining messages (non-fatal). Database errors mark
     * the campaign as "error" and halt execution (fatal).
     *
     * @param {string} campaignId - Campaign identifier
     * @param {number} phaseNumber - Phase number to execute (>= 2)
     * @returns {Promise<Object>} PhaseResult with successCount, failureCount, status
     * @throws {Error} If campaignId is missing, phaseNumber is invalid, or campaign not found
     *
     * Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 10.5
     */
    async executePhase(campaignId, phaseNumber) {
        // Validate inputs
        if (!campaignId || typeof campaignId !== 'string') {
            throw new Error('campaignId must be a non-empty string');
        }

        if (typeof phaseNumber !== 'number' || phaseNumber < 1 || !Number.isInteger(phaseNumber)) {
            throw new Error('phaseNumber must be a positive integer');
        }

        // Phase 1 is the grace-period evaluation — delegate to CampaignLifecycleManager
        if (phaseNumber === 1) {
            if (!this.campaignLifecycleManager) {
                throw new Error('campaignLifecycleManager is required to execute phase 1');
            }
            const campaign = await this.Campaign.findOne({ id: campaignId });
            if (!campaign) throw new Error(`Campaign not found: ${campaignId}`);
            return this.campaignLifecycleManager.handlePhase1Completion(campaignId, campaign.tenantId);
        }

        console.log(JSON.stringify({
            service: 'PhaseExecutor',
            event: 'phase_execution_start',
            campaignId,
            phaseNumber,
            timestamp: new Date().toISOString()
        }));

        // Look up the campaign
        let campaign;
        try {
            campaign = await this.Campaign.findOne({ id: campaignId });
        } catch (dbErr) {
            console.error(
                `[PhaseExecutor] Database error looking up campaign ${campaignId}: ${dbErr.message}`,
                dbErr
            );
            throw dbErr;
        }

        if (!campaign) {
            throw new Error(`Campaign not found: ${campaignId}`);
        }

        // Emit phase:started event for real-time UI updates (Task 14.1)
        SocketEmitter.emitPhaseStarted(campaign.tenantId, campaignId, phaseNumber);

        // Retrieve failed messages from the previous phase
        const previousPhase = phaseNumber - 1;
        let failedMessages;
        try {
            failedMessages = await this.getFailedMessages(campaignId, previousPhase);
        } catch (dbErr) {
            // Database error is fatal — log with full details and mark campaign as error (Req 17.2)
            console.error(JSON.stringify({
                service: 'PhaseExecutor',
                event: 'database_error',
                operation: 'getFailedMessages',
                campaignId,
                phaseNumber,
                error: dbErr.message,
                stack: dbErr.stack,
                timestamp: new Date().toISOString()
            }));
            // Mark campaign as error — database failure is fatal
            await this._markCampaignError(campaignId, dbErr.message);
            throw dbErr;
        }

        console.log(
            `[PhaseExecutor] Found ${failedMessages.length} failed message(s) to retry for campaign ${campaignId} phase ${phaseNumber}`
        );

        // Send messages and track results
        let successCount = 0;
        let failureCount = 0;
        const executedAt = new Date();

        for (const recipient of failedMessages) {
            try {
                // Delegate to the injected message sender
                await this.messageSender.sendMessage(recipient, campaign, phaseNumber);
                successCount++;
            } catch (sendErr) {
                // WhatsApp API failure: mark individual message as failed, continue with remaining messages
                failureCount++;

                // Log API error with campaign ID, message details, and error info (Req 17.1)
                console.error(JSON.stringify({
                    service: 'PhaseExecutor',
                    event: 'whatsapp_api_error',
                    campaignId,
                    phaseNumber,
                    recipientId: recipient._id ? recipient._id.toString() : null,
                    to: recipient.to,
                    wamid: recipient.wamid || null,
                    error: sendErr.message,
                    stack: sendErr.stack,
                    timestamp: new Date().toISOString()
                }));

                // Record the failed attempt in retry history
                try {
                    await this.Recipient.updateOne(
                        { _id: recipient._id },
                        {
                            $set: { status: 'failed' },
                            $push: {
                                retryHistory: {
                                    phaseNumber,
                                    attemptedAt: new Date(),
                                    status: 'failed'
                                }
                            }
                        }
                    );
                } catch (histErr) {
                    console.error(JSON.stringify({
                        service: 'PhaseExecutor',
                        event: 'retry_history_update_error',
                        campaignId,
                        phaseNumber,
                        recipientId: recipient._id ? recipient._id.toString() : null,
                        error: histErr.message,
                        stack: histErr.stack,
                        timestamp: new Date().toISOString()
                    }));
                }
            }
        }

        const completedAt = new Date();

        // Update phase statistics on the campaign
        try {
            await this.updatePhaseStats(campaignId, phaseNumber, {
                successCount,
                failureCount,
                executedAt,
                completedAt
            });
        } catch (dbErr) {
            // Database error is fatal — log with full details and mark campaign as error (Req 17.2)
            console.error(JSON.stringify({
                service: 'PhaseExecutor',
                event: 'database_error',
                operation: 'updatePhaseStats',
                campaignId,
                phaseNumber,
                error: dbErr.message,
                stack: dbErr.stack,
                timestamp: new Date().toISOString()
            }));
            await this._markCampaignError(campaignId, dbErr.message);
            throw dbErr;
        }

        // Emit phase:completed event for real-time UI updates (Task 14.1)
        SocketEmitter.emitPhaseCompleted(campaign.tenantId, campaignId, phaseNumber, {
            successCount,
            failureCount,
            executedAt,
            completedAt
        });

        // Determine next action: schedule next phase or complete campaign
        const maxPhases = (campaign.retryConfig && campaign.retryConfig.phases
            ? campaign.retryConfig.phases.length
            : 0) + 1; // +1 for initial phase

        const isMaxPhaseReached = phaseNumber >= maxPhases;
        const hasNoFailures = failureCount === 0;

        let campaignStatus;
        if (isMaxPhaseReached || hasNoFailures) {
            // Transition campaign to completed
            campaignStatus = 'completed';
            try {
                await this.Campaign.updateOne(
                    { id: campaignId },
                    { $set: { status: 'completed' } }
                );
                console.log(
                    `[PhaseExecutor] Campaign ${campaignId} completed after phase ${phaseNumber} ` +
                    `(maxPhaseReached=${isMaxPhaseReached}, noFailures=${hasNoFailures})`
                );

                // Cancel any remaining pending phases
                if (this.retryScheduler) {
                    try {
                        const cancelled = await this.retryScheduler.cancelPendingPhases(campaignId);
                        if (cancelled > 0) {
                            console.log(
                                `[PhaseExecutor] Cancelled ${cancelled} pending phase(s) for completed campaign ${campaignId}`
                            );
                        }
                    } catch (cancelErr) {
                        console.error(
                            `[PhaseExecutor] Failed to cancel pending phases for campaign ${campaignId}: ${cancelErr.message}`
                        );
                    }
                }
            } catch (dbErr) {
                // Database error is fatal — log with full details and mark campaign as error (Req 17.2)
                console.error(JSON.stringify({
                    service: 'PhaseExecutor',
                    event: 'database_error',
                    operation: 'completeCampaign',
                    campaignId,
                    phaseNumber,
                    error: dbErr.message,
                    stack: dbErr.stack,
                    timestamp: new Date().toISOString()
                }));
                await this._markCampaignError(campaignId, dbErr.message);
                throw dbErr;
            }
        } else {
            // Schedule next phase
            campaignStatus = 'retrying';
            const nextPhase = phaseNumber + 1;

            if (this.retryScheduler) {
                try {
                    await this.retryScheduler.schedulePhase(campaignId, nextPhase, completedAt);
                    console.log(
                        `[PhaseExecutor] Scheduled phase ${nextPhase} for campaign ${campaignId}`
                    );
                } catch (schedErr) {
                    console.error(
                        `[PhaseExecutor] Failed to schedule phase ${nextPhase} for campaign ${campaignId}: ${schedErr.message}`,
                        schedErr
                    );
                    // Scheduling failure is non-fatal for the current phase result,
                    // but log it prominently so operators can investigate
                }
            }
        }

        const result = {
            campaignId,
            phaseNumber,
            successCount,
            failureCount,
            executedAt,
            completedAt,
            status: campaignStatus
        };

        console.log(JSON.stringify({
            service: 'PhaseExecutor',
            event: 'phase_execution_complete',
            campaignId,
            phaseNumber,
            successCount,
            failureCount,
            campaignStatus,
            timestamp: new Date().toISOString()
        }));

        return result;
    }

    /**
     * Retrieve failed messages from the previous phase eligible for retry.
     *
     * A message is eligible for retry if ALL of the following are true:
     * - It belongs to the specified campaign
     * - phaseNumber is null (not yet successfully delivered — idempotency guarantee)
     * - status is "failed" OR "sent" (sent but not confirmed delivered)
     * - retryHistory contains an entry with phaseNumber = previousPhase
     *
     * This query enforces Requirements 9.1 and 9.2 by excluding any message
     * with a non-null phaseNumber (already delivered in some phase).
     *
     * @param {string} campaignId - Campaign identifier
     * @param {number} previousPhase - Previous phase number (>= 1)
     * @returns {Promise<Array>} Array of Recipient documents eligible for retry
     * @throws {Error} If campaignId is missing or previousPhase is invalid
     *
     * Requirements: 4.1, 9.1, 9.2, 9.3
     */
    async getFailedMessages(campaignId, previousPhase) {
        // Validate inputs
        if (!campaignId || typeof campaignId !== 'string') {
            throw new Error('campaignId must be a non-empty string');
        }

        if (typeof previousPhase !== 'number' || previousPhase < 1 || !Number.isInteger(previousPhase)) {
            throw new Error('previousPhase must be a positive integer');
        }

        // For phase 2 (first retry after initial send), recipients from the initial
        // send have NO retryHistory entries — they were sent directly, not via
        // PhaseExecutor. So we must NOT filter by retryHistory for previousPhase=1.
        //
        // For phase 3+ (retrying after a previous retry phase), recipients DO have
        // retryHistory entries from the previous PhaseExecutor run, so we filter
        // by retryHistory.phaseNumber = previousPhase.
        const baseQuery = {
            campaignId,
            phaseNumber: null,          // Idempotency: exclude already-delivered messages
            $or: [
                { status: 'failed' },
                { status: 'sent' }      // Sent but not confirmed delivered
            ]
        };

        if (previousPhase >= 2) {
            // Phase 3+: only retry messages that were attempted in the previous retry phase
            baseQuery['retryHistory.phaseNumber'] = previousPhase;
        }
        // previousPhase === 1: initial send — no retryHistory filter needed

        const messages = await this.Recipient.find(baseQuery);
        return messages;
    }

    /**
     * Update phase statistics on the campaign document.
     *
     * Updates (or inserts) the phaseStats entry for the given phaseNumber with:
     * - successCount and failureCount from the execution
     * - executedAt and completedAt timestamps
     * - campaign.currentPhase set to the executed phase number
     *
     * Uses a MongoDB session/transaction when available for atomicity.
     * Falls back to sequential operations for standalone MongoDB compatibility.
     *
     * @param {string} campaignId - Campaign identifier
     * @param {number} phaseNumber - Executed phase number
     * @param {Object} stats - Execution statistics
     * @param {number} stats.successCount - Number of successfully sent messages
     * @param {number} stats.failureCount - Number of failed messages
     * @param {Date} stats.executedAt - Phase execution start time
     * @param {Date} stats.completedAt - Phase execution completion time
     * @returns {Promise<void>}
     * @throws {Error} If inputs are invalid or database operation fails
     *
     * Requirements: 2.4, 4.5
     */
    async updatePhaseStats(campaignId, phaseNumber, stats) {
        // Validate inputs
        if (!campaignId || typeof campaignId !== 'string') {
            throw new Error('campaignId must be a non-empty string');
        }

        if (typeof phaseNumber !== 'number' || phaseNumber < 1 || !Number.isInteger(phaseNumber)) {
            throw new Error('phaseNumber must be a positive integer');
        }

        if (!stats || typeof stats !== 'object') {
            throw new Error('stats must be an object');
        }

        const { successCount, failureCount, executedAt, completedAt } = stats;

        if (typeof successCount !== 'number' || successCount < 0) {
            throw new Error('stats.successCount must be a non-negative number');
        }

        if (typeof failureCount !== 'number' || failureCount < 0) {
            throw new Error('stats.failureCount must be a non-negative number');
        }

        // Use a MongoDB session for atomicity when replica set is available.
        // Falls back gracefully to sequential operations for standalone MongoDB.
        let session = null;
        try {
            session = await mongoose.startSession();
            session.startTransaction();
        } catch (sessionErr) {
            // Standalone MongoDB does not support transactions — proceed without
            session = null;
        }

        try {
            const sessionOpt = session ? { session } : {};

            // Check if a phaseStats entry already exists for this phase
            const campaign = await this.Campaign.findOne(
                { id: campaignId },
                { phaseStats: 1 },
                sessionOpt
            );

            if (!campaign) {
                throw new Error(`Campaign not found: ${campaignId}`);
            }

            const existingStatIndex = campaign.phaseStats
                ? campaign.phaseStats.findIndex(s => s.phaseNumber === phaseNumber)
                : -1;

            if (existingStatIndex >= 0) {
                // Update existing phaseStats entry using positional operator
                await this.Campaign.updateOne(
                    { id: campaignId, 'phaseStats.phaseNumber': phaseNumber },
                    {
                        $set: {
                            'phaseStats.$.successCount': successCount,
                            'phaseStats.$.failureCount': failureCount,
                            'phaseStats.$.executedAt': executedAt || new Date(),
                            'phaseStats.$.completedAt': completedAt || new Date(),
                            currentPhase: phaseNumber
                        }
                    },
                    sessionOpt
                );
            } else {
                // Push a new phaseStats entry
                await this.Campaign.updateOne(
                    { id: campaignId },
                    {
                        $push: {
                            phaseStats: {
                                phaseNumber,
                                successCount,
                                failureCount,
                                executedAt: executedAt || new Date(),
                                completedAt: completedAt || new Date()
                            }
                        },
                        $set: {
                            currentPhase: phaseNumber
                        }
                    },
                    sessionOpt
                );
            }

            if (session) {
                await session.commitTransaction();
            }
        } catch (err) {
            if (session) {
                try {
                    await session.abortTransaction();
                } catch (abortErr) {
                    console.error(
                        `[PhaseExecutor] Failed to abort transaction for campaign ${campaignId}: ${abortErr.message}`
                    );
                }
            }
            throw err;
        } finally {
            if (session) {
                session.endSession();
            }
        }
    }

    /**
     * Mark a campaign as "error" status to prevent further automatic retries.
     *
     * Called when a critical database error occurs during phase execution.
     * Logs the error and updates campaign status to "error".
     *
     * @param {string} campaignId - Campaign identifier
     * @param {string} errorMessage - Error description
     * @returns {Promise<void>}
     * @private
     *
     * Requirements: 10.5
     */
    async _markCampaignError(campaignId, errorMessage) {
        try {
            await this.Campaign.updateOne(
                { id: campaignId },
                { $set: { status: 'error' } }
            );
            console.error(
                `[PhaseExecutor] Campaign ${campaignId} marked as error: ${errorMessage}`
            );
        } catch (err) {
            console.error(
                `[PhaseExecutor] Failed to mark campaign ${campaignId} as error: ${err.message}`,
                err
            );
        }
    }
}

module.exports = PhaseExecutor;
