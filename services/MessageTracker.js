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
}

module.exports = MessageTracker;
