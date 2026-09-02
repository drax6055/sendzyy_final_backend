const mongoose = require('mongoose');

/**
 * MessageTracker Service
 * 
 * Tracks message delivery across retry phases with idempotency guarantees.
 * Ensures successfully delivered messages are never retried by atomically
 * updating phaseNumber and deliveryTimestamp.
 * 
 * Requirements: 2.2, 6.1, 6.2, 6.4, 9.5
 */
class MessageTracker {
    /**
     * Initialize MessageTracker with Recipient model
     * @param {mongoose.Model} Recipient - Mongoose model for recipients
     */
    constructor(Recipient) {
        if (!Recipient) {
            throw new Error('Recipient model is required');
        }
        this.Recipient = Recipient;
    }

    /**
     * Record successful message delivery
     * 
     * Atomically updates phaseNumber and deliveryTimestamp for a message.
     * This operation is idempotent - once phaseNumber is set, it cannot be changed.
     * 
     * The atomic update ensures:
     * - Message is excluded from future retry phases immediately
     * - No race conditions between webhook processing and retry scheduling
     * - Idempotency: duplicate webhooks don't change the phaseNumber
     * 
     * @param {string} wamid - WhatsApp message ID (unique identifier)
     * @param {number} phaseNumber - Delivery phase number (1-6)
     * @param {Date} timestamp - Delivery timestamp
     * @returns {Promise<Object>} Update result with modifiedCount
     * @throws {Error} If wamid is missing or phaseNumber is invalid
     * 
     * Requirements: 2.2, 6.1, 6.2, 9.5
     */
    async recordDelivery(wamid, phaseNumber, timestamp) {
        // Validate inputs
        if (!wamid || typeof wamid !== 'string') {
            throw new Error('wamid must be a non-empty string');
        }

        if (typeof phaseNumber !== 'number' || phaseNumber < 1 || !Number.isInteger(phaseNumber)) {
            throw new Error('phaseNumber must be a positive integer');
        }

        if (!(timestamp instanceof Date) || isNaN(timestamp.getTime())) {
            throw new Error('timestamp must be a valid Date object');
        }

        // Atomic update: only update if phaseNumber is currently null
        // This ensures idempotency - once set, phaseNumber cannot be changed
        const result = await this.Recipient.updateOne(
            { 
                wamid,
                phaseNumber: null  // Only update if not already set
            },
            {
                $set: {
                    phaseNumber,
                    deliveryTimestamp: timestamp
                }
            }
        );

        return result;
    }

    /**
     * Check if message was already delivered
     * 
     * A message is considered delivered if it has a non-null phaseNumber.
     * This indicates the message was successfully delivered in some phase
     * and should be excluded from all future retry attempts.
     * 
     * @param {string} wamid - WhatsApp message ID
     * @returns {Promise<boolean>} True if message has been delivered (phaseNumber is not null)
     * @throws {Error} If wamid is missing
     * 
     * Requirements: 9.1, 9.2
     */
    async isDelivered(wamid) {
        if (!wamid || typeof wamid !== 'string') {
            throw new Error('wamid must be a non-empty string');
        }

        const recipient = await this.Recipient.findOne(
            { wamid },
            { phaseNumber: 1 }  // Only fetch phaseNumber field for efficiency
        );

        // Message is delivered if it exists and has a non-null phaseNumber
        return recipient !== null && recipient.phaseNumber !== null;
    }

    /**
     * Get messages by phase
     * 
     * Retrieves all messages that were successfully delivered in a specific phase.
     * Used for reporting and analytics to show phase-wise delivery breakdown.
     * 
     * @param {string} campaignId - Campaign identifier
     * @param {number} phaseNumber - Phase number to query
     * @returns {Promise<Array>} Array of recipient documents
     * @throws {Error} If campaignId is missing or phaseNumber is invalid
     * 
     * Requirements: 6.4
     */
    async getMessagesByPhase(campaignId, phaseNumber) {
        // Validate inputs
        if (!campaignId || typeof campaignId !== 'string') {
            throw new Error('campaignId must be a non-empty string');
        }

        if (typeof phaseNumber !== 'number' || phaseNumber < 1 || !Number.isInteger(phaseNumber)) {
            throw new Error('phaseNumber must be a positive integer');
        }

        // Query messages delivered in the specified phase
        const messages = await this.Recipient.find({
            campaignId,
            phaseNumber
        }).sort({ deliveryTimestamp: 1 });  // Sort by delivery time

        return messages;
    }

    /**
     * Get failed messages eligible for retry
     * 
     * Retrieves messages that failed in the previous phase and are eligible
     * for retry in the next phase. A message is eligible if:
     * - It belongs to the specified campaign
     * - phaseNumber is null (not yet successfully delivered)
     * - Status is 'failed' or 'sent' (sent but not confirmed delivered)
     * - It was attempted in the previous phase (has retryHistory entry)
     * 
     * This method enforces idempotency by excluding any message with a
     * non-null phaseNumber, ensuring successfully delivered messages are
     * never retried.
     * 
     * @param {string} campaignId - Campaign identifier
     * @param {number} previousPhase - Previous phase number
     * @returns {Promise<Array>} Array of recipient documents eligible for retry
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

        // Query failed messages from previous phase
        const messages = await this.Recipient.find({
            campaignId,
            phaseNumber: null,  // Only messages not yet delivered (idempotency)
            $or: [
                { status: 'failed' },
                { status: 'sent' }  // Sent but not confirmed delivered
            ],
            'retryHistory.phaseNumber': previousPhase  // Attempted in previous phase
        });

        return messages;
    }

    /**
     * Add retry history entry for a message
     * 
     * Records a retry attempt in the message's retryHistory array.
     * This tracks all retry attempts across phases for analytics and debugging.
     * 
     * @param {string} wamid - WhatsApp message ID
     * @param {number} phaseNumber - Phase number of the retry attempt
     * @param {string} status - Result status ('sent' or 'failed')
     * @returns {Promise<Object>} Update result
     * @throws {Error} If inputs are invalid
     * 
     * Requirements: 4.3
     */
    async addRetryHistory(wamid, phaseNumber, status) {
        // Validate inputs
        if (!wamid || typeof wamid !== 'string') {
            throw new Error('wamid must be a non-empty string');
        }

        if (typeof phaseNumber !== 'number' || phaseNumber < 1 || !Number.isInteger(phaseNumber)) {
            throw new Error('phaseNumber must be a positive integer');
        }

        if (!['sent', 'failed'].includes(status)) {
            throw new Error('status must be either "sent" or "failed"');
        }

        // Add retry history entry
        const result = await this.Recipient.updateOne(
            { wamid },
            {
                $push: {
                    retryHistory: {
                        phaseNumber,
                        attemptedAt: new Date(),
                        status
                    }
                }
            }
        );

        return result;
    }

    /**
     * Get delivery statistics for a campaign phase
     * 
     * Calculates success and failure counts for a specific phase.
     * Used for reporting and phase statistics tracking.
     * 
     * @param {string} campaignId - Campaign identifier
     * @param {number} phaseNumber - Phase number
     * @returns {Promise<Object>} Statistics object with successCount and failureCount
     * @throws {Error} If inputs are invalid
     * 
     * Requirements: 2.4, 6.5
     */
    async getPhaseStatistics(campaignId, phaseNumber) {
        // Validate inputs
        if (!campaignId || typeof campaignId !== 'string') {
            throw new Error('campaignId must be a non-empty string');
        }

        if (typeof phaseNumber !== 'number' || phaseNumber < 1 || !Number.isInteger(phaseNumber)) {
            throw new Error('phaseNumber must be a positive integer');
        }

        // Count successful deliveries in this phase
        const successCount = await this.Recipient.countDocuments({
            campaignId,
            phaseNumber
        });

        // Count failures in this phase (messages with retry history for this phase but no phaseNumber)
        const failureCount = await this.Recipient.countDocuments({
            campaignId,
            phaseNumber: null,
            'retryHistory.phaseNumber': phaseNumber
        });

        return {
            successCount,
            failureCount
        };
    }

    /**
     * Atomically processes incoming webhook status updates (sent, delivered, read, failed).
     * Enforces DB-level atomicity, prevents duplicate campaign counter increments,
     * prevents out-of-order state regression, and backfills deliveredAt safely.
     * 
     * @param {Object} statusUpdate - Status update object from Meta
     * @param {Object} context - Model dependencies and broadcast functions
     */
    async processStatusUpdateAtomic(statusUpdate, context = {}) {
        const STATUS_RANK = { sent: 1, delivered: 2, read: 3, failed: 4 };

        const wamid = statusUpdate?.id;
        const incomingStatus = statusUpdate?.status;
        const incomingRank = STATUS_RANK[incomingStatus];
        const incomingTimestamp = statusUpdate?.timestamp
            ? new Date(parseInt(statusUpdate.timestamp, 10) * 1000)
            : new Date();

        if (!wamid || !incomingRank) return { success: false, reason: 'invalid_status_payload' };

        const Recipient = context.Recipient || this.Recipient;
        const StatusMapping = context.StatusMapping || mongoose.model('StatusMapping');
        const Campaign = context.Campaign || mongoose.model('Campaign');
        const Message = context.Message || mongoose.model('Message');
        const broadcastMessages = context.broadcastMessages || (async () => {});
        const broadcastCampaigns = context.broadcastCampaigns || (async () => {});

        const mapping = await StatusMapping.findOne({ wamid }).lean();
        if (!mapping?.tenantId) {
            return { success: false, reason: 'unmapped_wamid' };
        }

        const tenantId = mapping.tenantId;
        const campaignId = mapping.campaignId;
        const campaignFilter = campaignId ? { tenantId, id: campaignId } : null;

        // ---------------------------------------------------------
        // CASE 0: SENT
        // ---------------------------------------------------------
        if (incomingStatus === 'sent') {
            const updated = await Recipient.findOneAndUpdate(
                { wamid, sentAt: null },
                { $set: { sentAt: incomingTimestamp.toISOString(), status: incomingStatus } },
                { returnDocument: 'after' }
            );

            if (!updated) {
                console.debug(`[Webhook][Dedup] Duplicate 'sent' for ${wamid} — skipped`);
                return { success: true, duplicate: true, status: 'sent' };
            }

            if (campaignFilter) {
                await Campaign.updateOne(campaignFilter, { $inc: { successCount: 1 } });
                await broadcastCampaigns(tenantId);
            }
            await Message.updateOne(
                { wamid, status: { $nin: ['delivered', 'read'] } },
                { $set: { status: 'sent' } }
            );
            await broadcastMessages(tenantId, mapping.to);
            return { success: true, duplicate: false, status: 'sent' };
        }

        // ---------------------------------------------------------
        // CASE A: DELIVERED
        // ---------------------------------------------------------
        if (incomingStatus === 'delivered') {
            const updatedRecipient = await Recipient.findOneAndUpdate(
                { wamid, deliveredAt: null },
                {
                    $set: {
                        deliveredAt: incomingTimestamp.toISOString(),
                        deliveryTimestamp: incomingTimestamp,
                        status: incomingStatus
                    }
                },
                { returnDocument: 'after' }
            );

            if (!updatedRecipient) {
                console.debug(`[Webhook][Dedup] Duplicate 'delivered' for ${wamid} — skipped`);
                return { success: true, duplicate: true, status: 'delivered' };
            }

            if (campaignFilter) {
                await Campaign.updateOne(campaignFilter, { $inc: { deliveredCount: 1 } });
            }
            await Message.updateOne({ wamid, status: { $ne: 'read' } }, { $set: { status: 'delivered' } });
            await broadcastMessages(tenantId, mapping.to);

            if (campaignFilter) {
                const camp = await Campaign.findOne(campaignFilter, { currentPhase: 1 });
                if (camp) {
                    await this.recordDelivery(wamid, camp.currentPhase, incomingTimestamp);
                }
                await broadcastCampaigns(tenantId);
            }
            return { success: true, duplicate: false, status: 'delivered' };
        }

        // ---------------------------------------------------------
        // CASE B: READ
        // ---------------------------------------------------------
        if (incomingStatus === 'read') {
            // Aggregation-pipeline update: only fills deliveredAt if it was NOT already set.
            // Prevents a true, earlier deliveredAt from being overwritten by the later read timestamp.
            const updatedRecipient = await Recipient.findOneAndUpdate(
                { wamid, readAt: null },
                [
                    {
                        $set: {
                            readAt: incomingTimestamp.toISOString(),
                            status: 'read',
                            deliveredAt: { $ifNull: ['$deliveredAt', incomingTimestamp.toISOString()] },
                            deliveryTimestamp: { $ifNull: ['$deliveryTimestamp', incomingTimestamp] }
                        }
                    }
                ],
                { returnDocument: 'before', updatePipeline: true }
            );

            if (!updatedRecipient) {
                console.debug(`[Webhook][Dedup] Duplicate 'read' for ${wamid} — skipped`);
                return { success: true, duplicate: true, status: 'read' };
            }

            const wasAlreadyDelivered = Boolean(updatedRecipient.deliveredAt);
            const campaignInc = { readCount: 1 };
            if (!wasAlreadyDelivered) {
                campaignInc.deliveredCount = 1; // out-of-order read arrived before delivered
            }

            if (campaignFilter) {
                await Campaign.updateOne(campaignFilter, { $inc: campaignInc });
            }
            await Message.updateOne({ wamid }, { $set: { status: 'read' } });
            await broadcastMessages(tenantId, mapping.to);

            if (campaignFilter) {
                const camp = await Campaign.findOne(campaignFilter, { currentPhase: 1 });
                if (camp) {
                    await this.recordDelivery(wamid, camp.currentPhase, incomingTimestamp);
                }
                await broadcastCampaigns(tenantId);
            }
            return { success: true, duplicate: false, status: 'read' };
        }

        // ---------------------------------------------------------
        // CASE C: FAILED
        // ---------------------------------------------------------
        if (incomingStatus === 'failed') {
            const errObj = statusUpdate.errors?.[0];
            const errorDetails = errObj?.error_data?.details || errObj?.message || errObj?.title || 'Meta delivery failure';

            // Guard: never downgrade a recipient that already reached delivered/read
            const updatedRecipient = await Recipient.findOneAndUpdate(
                { wamid, failedAt: null, status: { $nin: ['delivered', 'read'] } },
                { $set: { failedAt: incomingTimestamp.toISOString(), status: 'failed' } },
                { returnDocument: 'after' }
            );

            if (!updatedRecipient) {
                console.debug(`[Webhook][Dedup/Guard] 'failed' for ${wamid} skipped (duplicate or already delivered/read)`);
                return { success: true, duplicate: true, status: 'failed' };
            }

            if (campaignFilter) {
                await Campaign.updateOne(campaignFilter, { $inc: { failureCount: 1 } });
            }
            await Message.updateOne({ wamid }, { $set: { status: 'failed', errorDetails } });
            await broadcastMessages(tenantId, mapping.to);
            if (campaignFilter) await broadcastCampaigns(tenantId);
            return { success: true, duplicate: false, status: 'failed' };
        }

        return { success: false, reason: 'unhandled_status' };
    }
}

module.exports = MessageTracker;

