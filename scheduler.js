/**
 * Scheduler Module
 *
 * Sets up the cron job for processing due retry phases and implements
 * startup resilience to recover phases that were interrupted by a server restart.
 *
 * Requirements: 3.3, 3.5
 */
const cron = require('node-cron');

/**
 * Recover any ScheduledRetryPhase documents that were left in 'executing' status
 * from a previous server run (e.g., due to a crash or restart).
 *
 * - If scheduledAt is within the last 1 hour → reset to 'pending' so the cron
 *   job picks it up on the next tick.
 * - If scheduledAt is older than 1 hour → mark as 'failed' with an explanatory
 *   error message.
 *
 * @param {mongoose.Model} ScheduledRetryPhase - Mongoose model for scheduled phases
 * @returns {Promise<void>}
 *
 * Requirements: 3.5
 */
async function recoverExecutingPhases(ScheduledRetryPhase) {
    try {
        const executingPhases = await ScheduledRetryPhase.find({ status: 'executing' });

        if (executingPhases.length === 0) {
            console.log(JSON.stringify({
                service: 'Scheduler',
                event: 'startup_recovery_check',
                message: 'No executing phases found on startup',
                timestamp: new Date().toISOString()
            }));
            return;
        }

        console.log(JSON.stringify({
            service: 'Scheduler',
            event: 'startup_recovery_start',
            executingCount: executingPhases.length,
            timestamp: new Date().toISOString()
        }));

        const now = new Date();
        const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);

        for (const phase of executingPhases) {
            const isRecent = phase.scheduledAt >= oneHourAgo;

            if (isRecent) {
                // Within 1 hour — reset to pending so the cron job retries it
                await ScheduledRetryPhase.findByIdAndUpdate(phase._id, {
                    $set: { status: 'pending' }
                });

                console.log(JSON.stringify({
                    service: 'Scheduler',
                    event: 'startup_recovery_reset',
                    campaignId: phase.campaignId,
                    phaseNumber: phase.phaseNumber,
                    scheduledAt: phase.scheduledAt.toISOString(),
                    message: 'Reset to pending — scheduledAt within 1 hour',
                    timestamp: now.toISOString()
                }));
            } else {
                // Older than 1 hour — mark as failed
                await ScheduledRetryPhase.findByIdAndUpdate(phase._id, {
                    $set: {
                        status: 'failed',
                        errorMessage: `Phase abandoned on server restart. Originally scheduled at ${phase.scheduledAt.toISOString()}, which is more than 1 hour ago.`
                    }
                });

                console.log(JSON.stringify({
                    service: 'Scheduler',
                    event: 'startup_recovery_failed',
                    campaignId: phase.campaignId,
                    phaseNumber: phase.phaseNumber,
                    scheduledAt: phase.scheduledAt.toISOString(),
                    message: 'Marked as failed — scheduledAt older than 1 hour',
                    timestamp: now.toISOString()
                }));
            }
        }

        console.log(JSON.stringify({
            service: 'Scheduler',
            event: 'startup_recovery_complete',
            executingCount: executingPhases.length,
            timestamp: now.toISOString()
        }));
    } catch (err) {
        console.error(JSON.stringify({
            service: 'Scheduler',
            event: 'startup_recovery_error',
            error: err.message,
            stack: err.stack,
            timestamp: new Date().toISOString()
        }));
    }
}

/**
 * Start the retry phase cron job.
 *
 * Runs every minute. On each tick, calls retryScheduler.processDuePhases()
 * to execute any phases whose scheduledAt time has passed. Errors are caught
 * and logged so the cron job never crashes the process.
 *
 * @param {RetryScheduler} retryScheduler - RetryScheduler service instance
 * @returns {cron.ScheduledTask} The running cron task (can be stopped if needed)
 *
 * Requirements: 3.3
 */
function startRetrySchedulerCron(retryScheduler) {
    const task = cron.schedule('* * * * *', async () => {
        const startTime = new Date();
        console.log(JSON.stringify({
            service: 'Scheduler',
            event: 'cron_tick_start',
            timestamp: startTime.toISOString()
        }));

        try {
            await retryScheduler.processDuePhases();

            console.log(JSON.stringify({
                service: 'Scheduler',
                event: 'cron_tick_complete',
                durationMs: Date.now() - startTime.getTime(),
                timestamp: new Date().toISOString()
            }));
        } catch (err) {
            console.error(JSON.stringify({
                service: 'Scheduler',
                event: 'cron_tick_error',
                error: err.message,
                stack: err.stack,
                durationMs: Date.now() - startTime.getTime(),
                timestamp: new Date().toISOString()
            }));
        }
    });

    console.log(JSON.stringify({
        service: 'Scheduler',
        event: 'cron_started',
        schedule: '* * * * * (every minute)',
        timestamp: new Date().toISOString()
    }));

    return task;
}

/**
 * Start the daily cleanup cron job for orphaned phases.
 *
 * Runs once per day at midnight (00:00). On each tick, calls
 * retryScheduler.cleanupOrphanedPhases() to cancel any pending phases that
 * are older than 7 days and whose campaign no longer exists or is already
 * completed. Errors are caught and logged so the cron job never crashes the
 * process.
 *
 * @param {RetryScheduler} retryScheduler - RetryScheduler service instance
 * @returns {cron.ScheduledTask} The running cron task (can be stopped if needed)
 *
 * Requirements: System maintenance (Task 16.1)
 */
function startCleanupCron(retryScheduler) {
    const task = cron.schedule('0 0 * * *', async () => {
        const startTime = new Date();
        console.log(JSON.stringify({
            service: 'Scheduler',
            event: 'cleanup_cron_tick_start',
            timestamp: startTime.toISOString()
        }));

        try {
            const cancelledCount = await retryScheduler.cleanupOrphanedPhases();

            console.log(JSON.stringify({
                service: 'Scheduler',
                event: 'cleanup_cron_tick_complete',
                cancelledCount,
                durationMs: Date.now() - startTime.getTime(),
                timestamp: new Date().toISOString()
            }));
        } catch (err) {
            console.error(JSON.stringify({
                service: 'Scheduler',
                event: 'cleanup_cron_tick_error',
                error: err.message,
                stack: err.stack,
                durationMs: Date.now() - startTime.getTime(),
                timestamp: new Date().toISOString()
            }));
        }
    });

    console.log(JSON.stringify({
        service: 'Scheduler',
        event: 'cleanup_cron_started',
        schedule: '0 0 * * * (daily at midnight)',
        timestamp: new Date().toISOString()
    }));

    return task;
}

/**
 * Initialize the scheduler: run startup recovery then start the cron job.
 *
 * @param {RetryScheduler} retryScheduler - RetryScheduler service instance
 * @param {mongoose.Model} ScheduledRetryPhase - Mongoose model for scheduled phases
 * @returns {Promise<cron.ScheduledTask>} The running cron task
 *
 * Requirements: 3.3, 3.5
 */
async function initScheduler(retryScheduler, ScheduledRetryPhase) {
    // Task 13.2: Recover phases interrupted by a previous server restart
    await recoverExecutingPhases(ScheduledRetryPhase);

    // Task 13.1: Start the every-minute cron job
    const task = startRetrySchedulerCron(retryScheduler);

    // Task 16.1: Start the daily cleanup cron job for orphaned phases
    startCleanupCron(retryScheduler);

    return task;
}

module.exports = { initScheduler, recoverExecutingPhases, startRetrySchedulerCron, startCleanupCron };
