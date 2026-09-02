const WebhookRawLog = require('../models/WebhookRawLog');

/**
 * WebhookIngestionService
 * Manages write-ahead persistence, background job processing, and crash recovery.
 * Standalone service imported by both server.js and scheduler.js to prevent circular dependencies.
 */
class WebhookIngestionService {
    constructor() {
        this.routerHandler = null;
        this.context = {};
    }

    /**
     * Register router processor function and context
     * @param {Function} handler - Function that takes (payload, context) and routes the webhook
     * @param {Object} context - Optional context references (models, broadcasters)
     */
    setHandler(handler, context = {}) {
        this.routerHandler = handler;
        this.context = context;
    }

    /**
     * Write-ahead persistence: Enqueues raw webhook payload before 200 OK acknowledgment
     * @param {Object} payload - Incoming webhook body
     * @returns {Promise<string|null>} Created log _id
     */
    async enqueueRawWebhook(payload) {
        try {
            const rawLog = await WebhookRawLog.create({
                payload,
                status: 'pending',
                attempts: 0
            });
            return rawLog._id.toString();
        } catch (err) {
            console.error('[WebhookIngestion] Write-ahead logging failed:', err.message);
            return null;
        }
    }

    /**
     * Atomically claims and processes a single webhook job
     * @param {string} rawLogId - ID of the WebhookRawLog document
     * @returns {Promise<boolean>} Success status
     */
    async processWebhookJob(rawLogId) {
        if (!rawLogId) return false;

        const log = await WebhookRawLog.findOneAndUpdate(
            { _id: rawLogId, status: { $in: ['pending', 'failed'] } },
            { $set: { status: 'processing' }, $inc: { attempts: 1 } },
            { returnDocument: 'after' }
        );

        if (!log) return false;

        try {
            if (typeof this.routerHandler === 'function') {
                await this.routerHandler(log.payload, this.context);
            } else {
                throw new Error('No routerHandler registered in WebhookIngestionService');
            }

            await WebhookRawLog.updateOne(
                { _id: rawLogId },
                { $set: { status: 'processed', error: null } }
            );
            return true;
        } catch (err) {
            console.error(`[WebhookIngestion] Job ${rawLogId} processing failed:`, err.message);
            await WebhookRawLog.updateOne(
                { _id: rawLogId },
                { $set: { status: 'failed', error: err.message } }
            );
            return false;
        }
    }

    /**
     * Crash Recovery Sweep:
     * Drains any pending, stuck, or failed webhook logs left from server restarts or temporary errors.
     * @param {number} [limit=200] - Max logs to recover per batch
     * @returns {Promise<number>} Count of recovered jobs
     */
    async recoverPendingWebhookLogs(limit = 200) {
        try {
            const stuckLogs = await WebhookRawLog.find({
                status: { $in: ['pending', 'processing', 'failed'] },
                attempts: { $lt: 5 }
            })
            .sort({ createdAt: 1 })
            .limit(limit);

            if (stuckLogs.length > 0) {
                console.log(`[WebhookRecovery] Recovering ${stuckLogs.length} pending/stalled webhook payload(s)...`);
                const recoveryPromises = stuckLogs.map(log => 
                    this.processWebhookJob(log._id.toString()).catch((err) => {
                        console.error(`[WebhookRecovery] Error reprocessing job ${log._id}:`, err.message);
                    })
                );
                await Promise.all(recoveryPromises);
            }
            return stuckLogs.length;
        } catch (err) {
            console.error('[WebhookRecovery] Error during recovery check:', err.message);
            return 0;
        }
    }
}

// Export singleton instance
const webhookIngestionService = new WebhookIngestionService();
module.exports = webhookIngestionService;
