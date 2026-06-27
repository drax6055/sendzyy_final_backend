/**
 * SocketEmitter Service
 *
 * Singleton helper that wraps the Socket.io `io` instance and exposes
 * typed emit methods for phase and campaign lifecycle events.
 *
 * Usage:
 *   // In server.js, after io is created:
 *   const SocketEmitter = require('./services/SocketEmitter');
 *   SocketEmitter.setIo(io);
 *
 *   // In services:
 *   const SocketEmitter = require('./SocketEmitter');
 *   SocketEmitter.emitPhaseStarted(tenantId, campaignId, phaseNumber);
 *
 * All emit methods are no-ops when io has not been set (e.g. in unit tests).
 *
 * Requirements: Real-time UI updates (Task 14.1)
 */

let _io = null;

const SocketEmitter = {
    /**
     * Inject the Socket.io server instance.
     * Must be called once in server.js after `io` is created.
     *
     * @param {import('socket.io').Server} io
     */
    setIo(io) {
        _io = io;
    },

    /**
     * Emit `phase:started` to the tenant's room when a retry phase begins.
     *
     * Payload: { campaignId, phaseNumber, startedAt }
     *
     * @param {string} tenantId
     * @param {string} campaignId
     * @param {number} phaseNumber
     */
    emitPhaseStarted(tenantId, campaignId, phaseNumber) {
        if (!_io) return;
        if (!tenantId || !campaignId) return;

        const payload = {
            campaignId,
            phaseNumber,
            startedAt: new Date().toISOString()
        };

        _io.to(tenantId).emit('phase:started', payload);

        console.log(JSON.stringify({
            service: 'SocketEmitter',
            event: 'phase:started',
            tenantId,
            campaignId,
            phaseNumber,
            timestamp: payload.startedAt
        }));
    },

    /**
     * Emit `phase:completed` to the tenant's room when a retry phase finishes.
     *
     * Payload: { campaignId, phaseNumber, stats: { successCount, failureCount, executedAt, completedAt } }
     *
     * @param {string} tenantId
     * @param {string} campaignId
     * @param {number} phaseNumber
     * @param {{ successCount: number, failureCount: number, executedAt: Date, completedAt: Date }} stats
     */
    emitPhaseCompleted(tenantId, campaignId, phaseNumber, stats) {
        if (!_io) return;
        if (!tenantId || !campaignId) return;

        const payload = {
            campaignId,
            phaseNumber,
            stats: {
                successCount: stats.successCount,
                failureCount: stats.failureCount,
                executedAt: stats.executedAt instanceof Date
                    ? stats.executedAt.toISOString()
                    : stats.executedAt,
                completedAt: stats.completedAt instanceof Date
                    ? stats.completedAt.toISOString()
                    : stats.completedAt
            }
        };

        _io.to(tenantId).emit('phase:completed', payload);

        console.log(JSON.stringify({
            service: 'SocketEmitter',
            event: 'phase:completed',
            tenantId,
            campaignId,
            phaseNumber,
            successCount: stats.successCount,
            failureCount: stats.failureCount,
            timestamp: new Date().toISOString()
        }));
    },

    /**
     * Emit `campaign:progress` to the tenant's room after each message is sent.
     *
     * Payload: { campaignId, sentCount, totalCount, successCount, failureCount }
     *
     * @param {string} tenantId
     * @param {string} campaignId
     * @param {{ sentCount: number, totalCount: number, successCount: number, failureCount: number }} progress
     */
    emitCampaignProgress(tenantId, campaignId, { sentCount, totalCount, successCount, failureCount }) {
        if (!_io) return;
        if (!tenantId || !campaignId) return;

        const payload = {
            campaignId,
            sentCount,
            totalCount,
            successCount,
            failureCount
        };

        _io.to(tenantId).emit('campaign:progress', payload);

        console.log(JSON.stringify({
            service: 'SocketEmitter',
            event: 'campaign:progress',
            tenantId,
            campaignId,
            sentCount,
            totalCount,
            successCount,
            failureCount,
            timestamp: new Date().toISOString()
        }));
    },

    /**
     * Emit `campaign:error` to the tenant's room when a campaign fails.
     *
     * Payload: { campaignId, error, timestamp }
     *
     * @param {string} tenantId
     * @param {string} campaignId
     * @param {string} errorMessage
     */
    emitCampaignError(tenantId, campaignId, errorMessage) {
        if (!_io) return;
        if (!tenantId || !campaignId) return;

        const payload = {
            campaignId,
            error: errorMessage,
            timestamp: new Date().toISOString()
        };

        _io.to(tenantId).emit('campaign:error', payload);

        console.log(JSON.stringify({
            service: 'SocketEmitter',
            event: 'campaign:error',
            tenantId,
            campaignId,
            error: errorMessage,
            timestamp: payload.timestamp
        }));
    },

    /**
     * Emit `campaign:completed` to the tenant's room when a campaign finishes.
     *
     * Payload: { campaignId, status, finalStats: { totalSuccess, totalFailure, overallSuccessRate, phases } }
     *
     * @param {string} tenantId
     * @param {string} campaignId
     * @param {{ totalSuccess: number, totalFailure: number, overallSuccessRate: number, phases?: Array }} finalStats
     */
    emitCampaignCompleted(tenantId, campaignId, finalStats) {
        if (!_io) return;
        if (!tenantId || !campaignId) return;

        const payload = {
            campaignId,
            status: 'completed',
            finalStats: {
                totalSuccess: finalStats.totalSuccess,
                totalFailure: finalStats.totalFailure,
                overallSuccessRate: finalStats.overallSuccessRate,
                phases: finalStats.phases || []
            }
        };

        _io.to(tenantId).emit('campaign:completed', payload);

        console.log(JSON.stringify({
            service: 'SocketEmitter',
            event: 'campaign:completed',
            tenantId,
            campaignId,
            totalSuccess: finalStats.totalSuccess,
            totalFailure: finalStats.totalFailure,
            overallSuccessRate: finalStats.overallSuccessRate,
            timestamp: new Date().toISOString()
        }));
    }
};

module.exports = SocketEmitter;
