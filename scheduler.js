/**
 * Scheduler Module
 *
 * Sets up cron jobs for:
 *   1. Processing due retry phases (every minute)
 *   2. Cleaning up orphaned phases (daily at midnight)
 *   3. Proactive token expiry monitoring (daily at 9:00 AM)
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

// ─────────────────────────────────────────────────────────────────────────────
//  Token Expiry Watcher Cron
//
//  Runs daily at 9:00 AM. Scans all tenants with a non-null tokenExpiry field:
//
//    tokenExpiry <= now            → mark tokenStatus = 'expired'
//    tokenExpiry <= now + 7 days   → mark tokenStatus = 'expiring_soon'
//                                    send warning email
//    tokenExpiry is null           → system user token (never expires), skip
//
//  HOW TOKEN RENEWAL WORKS:
//  ─────────────────────────
//  Meta tokens CANNOT be renewed server-side without user interaction.
//  There are two scenarios:
//
//  A) User Token (tokenType = 'user', expires ~60 days):
//     → You CANNOT auto-renew. The customer must click "Connect WhatsApp"
//       and re-run Embedded Signup to get a new code → new token.
//     → This cron warns them 7 days before expiry so they have time to act.
//
//  B) System User Token (tokenType = 'system_user', tokenExpiry = null):
//     → Created via processOnboarding() Step 5.
//     → If created with "no expiry" in Meta Business Suite, it NEVER expires.
//     → This is the recommended production setup. No renewal needed.
//     → If it was created WITH an expiry, this cron still warns the tenant.
//
//  @param {mongoose.Model} Tenant       - Tenant Mongoose model
//  @param {Object}         transporter  - Nodemailer transporter
//  @param {Object}         io           - Socket.IO server instance
// ─────────────────────────────────────────────────────────────────────────────
function startTokenExpiryWatcherCron(Tenant, transporter, io) {
    const WARN_DAYS_BEFORE = 7;

    const task = cron.schedule('0 9 * * *', async () => {
        const now = new Date();
        const warnThreshold = new Date(now.getTime() + WARN_DAYS_BEFORE * 24 * 60 * 60 * 1000);

        console.log(JSON.stringify({
            service: 'TokenExpiryWatcher',
            event: 'cron_tick_start',
            warnThreshold: warnThreshold.toISOString(),
            timestamp: now.toISOString()
        }));

        try {
            // Find all tenants with a set tokenExpiry (system user tokens have null — skip them)
            const tenantsToCheck = await Tenant.find({
                'whatsappConfig.tokenExpiry': { $ne: null },
                'whatsappConfig.verified': true,
            }).select('name email whatsappConfig').lean();

            let expiredCount = 0;
            let expiringSoonCount = 0;

            for (const tenant of tenantsToCheck) {
                const tokenExpiry = tenant.whatsappConfig?.tokenExpiry;
                if (!tokenExpiry) continue;

                const expiryDate = new Date(tokenExpiry);
                const tenantId = tenant._id.toString();

                if (expiryDate <= now) {
                    // ── Already expired ──────────────────────────────────────
                    await Tenant.findByIdAndUpdate(tenantId, {
                        'whatsappConfig.tokenStatus': 'expired',
                        'whatsappConfig.verified': false,
                    });

                    // Emit socket event if tenant is online
                    if (io) {
                        io.to(tenantId).emit('token_expired', {
                            tenantId,
                            message: 'Your WhatsApp access token has expired. Please reconnect your account.',
                            action: 'reconnect'
                        });
                    }

                    // Send expiry email
                    if (tenant.email && transporter) {
                        const daysAgo = Math.floor((now - expiryDate) / (1000 * 60 * 60 * 24));
                        transporter.sendMail({
                            from: `"Sendzyy" <${process.env.EMAIL_USER}>`,
                            to: tenant.email,
                            subject: `🚨 WhatsApp Token Expired ${daysAgo > 0 ? `${daysAgo} day${daysAgo > 1 ? 's' : ''} ago` : 'Today'} — Reconnect Now | Sendzyy`,
                            html: buildTokenExpiredEmail(tenant.name, expiryDate, false),
                        }).catch(e => console.error(`[TokenExpiryWatcher] Email failed for ${tenant.email}:`, e.message));
                    }

                    expiredCount++;
                    console.log(JSON.stringify({
                        service: 'TokenExpiryWatcher',
                        event: 'token_marked_expired',
                        tenantId,
                        tokenExpiry: expiryDate.toISOString(),
                        timestamp: now.toISOString()
                    }));

                } else if (expiryDate <= warnThreshold) {
                    // ── Expiring within 7 days ───────────────────────────────
                    const daysLeft = Math.ceil((expiryDate - now) / (1000 * 60 * 60 * 24));

                    await Tenant.findByIdAndUpdate(tenantId, {
                        'whatsappConfig.tokenStatus': 'expiring_soon',
                    });

                    if (io) {
                        io.to(tenantId).emit('token_expiring_soon', {
                            tenantId,
                            daysLeft,
                            expiresAt: expiryDate.toISOString(),
                            message: `Your WhatsApp token expires in ${daysLeft} day${daysLeft !== 1 ? 's' : ''}. Please reconnect soon.`,
                        });
                    }

                    // Send warning email
                    if (tenant.email && transporter) {
                        transporter.sendMail({
                            from: `"Sendzyy" <${process.env.EMAIL_USER}>`,
                            to: tenant.email,
                            subject: `⏰ WhatsApp Token Expires in ${daysLeft} Day${daysLeft !== 1 ? 's' : ''} — Action Required | Sendzyy`,
                            html: buildTokenExpiringEmail(tenant.name, expiryDate, daysLeft),
                        }).catch(e => console.error(`[TokenExpiryWatcher] Warning email failed for ${tenant.email}:`, e.message));
                    }

                    expiringSoonCount++;
                    console.log(JSON.stringify({
                        service: 'TokenExpiryWatcher',
                        event: 'token_expiring_soon_warned',
                        tenantId,
                        daysLeft,
                        tokenExpiry: expiryDate.toISOString(),
                        timestamp: now.toISOString()
                    }));
                }
                // else: token is still valid and more than 7 days away — do nothing
            }

            console.log(JSON.stringify({
                service: 'TokenExpiryWatcher',
                event: 'cron_tick_complete',
                tenantsChecked: tenantsToCheck.length,
                expiredCount,
                expiringSoonCount,
                timestamp: new Date().toISOString()
            }));
        } catch (err) {
            console.error(JSON.stringify({
                service: 'TokenExpiryWatcher',
                event: 'cron_tick_error',
                error: err.message,
                stack: err.stack,
                timestamp: new Date().toISOString()
            }));
        }
    });

    console.log(JSON.stringify({
        service: 'TokenExpiryWatcher',
        event: 'cron_started',
        schedule: '0 9 * * * (daily at 9:00 AM)',
        warnDaysBefore: WARN_DAYS_BEFORE,
        timestamp: new Date().toISOString()
    }));

    return task;
}

/**
 * Build the "token expired" email HTML body.
 */
function buildTokenExpiredEmail(name, expiryDate, isSoon) {
    return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#fff8f0;font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#fff8f0;padding:40px 0;">
<tr><td align="center">
<table width="560" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
  <tr>
    <td style="background:linear-gradient(135deg,#B71C1C 0%,#E53935 60%,#EF5350 100%);padding:40px 48px;text-align:center;">
      <span style="font-size:48px;">🚨</span>
      <h1 style="margin:16px 0 8px;color:#fff;font-size:26px;font-weight:700;">WhatsApp Token Expired</h1>
      <p style="margin:0;color:rgba(255,255,255,0.85);font-size:14px;">Expired on: ${expiryDate.toDateString()}</p>
    </td>
  </tr>
  <tr>
    <td style="padding:36px 48px;">
      <p style="margin:0 0 20px;color:#555;font-size:15px;line-height:1.7;">Hello <strong>${name}</strong>,</p>
      <p style="margin:0 0 20px;color:#555;font-size:15px;line-height:1.7;">
        Your WhatsApp Business access token <strong>has expired</strong>. 
        Messages are not being sent or received right now.
      </p>
      <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
        <tr><td style="background:#ffebee;border-left:4px solid #E53935;border-radius:0 8px 8px 0;padding:14px 18px;">
          <p style="margin:0;color:#c62828;font-size:14px;font-weight:600;">
            📵 All outgoing messages are paused. Chatbots have been deactivated.
          </p>
        </td></tr>
      </table>
      <p style="margin:0 0 24px;color:#555;font-size:15px;line-height:1.7;">
        Go to your dashboard and click <strong>"Connect WhatsApp"</strong> to reconnect. 
        You will be asked to log in with Facebook and re-authorize Sendzyy — it takes under 2 minutes.
      </p>
      <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;">
        <tr><td align="center">
          <a href="https://app.sendzyy.com" target="_blank"
             style="display:inline-block;background:linear-gradient(135deg,#B71C1C,#E53935);color:#fff;text-decoration:none;font-size:16px;font-weight:700;padding:16px 48px;border-radius:50px;box-shadow:0 4px 14px rgba(183,28,28,0.4);">
            🔗 &nbsp; Reconnect WhatsApp Now
          </a>
        </td></tr>
      </table>
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr><td style="background:#f9f9f9;border-radius:10px;padding:14px 18px;">
          <p style="margin:0;color:#888;font-size:13px;line-height:1.6;">
            💡 <strong>Avoid this in the future:</strong> Ask Sendzyy support to set up a 
            permanent System User Token for your account. It never expires.
          </p>
        </td></tr>
      </table>
    </td>
  </tr>
  <tr>
    <td style="background:#f5f5f5;border-top:1px solid #eee;padding:24px 48px;text-align:center;">
      <p style="margin:0;color:#1B5E20;font-size:14px;font-weight:700;">Sendzyy</p>
      <p style="margin:4px 0 0;color:#aaa;font-size:12px;">© ${new Date().getFullYear()} Sendzyy · <a href="https://app.sendzyy.com" style="color:#4CAF50;text-decoration:none;">app.sendzyy.com</a></p>
    </td>
  </tr>
</table>
</td></tr>
</table>
</body>
</html>`;
}

/**
 * Build the "token expiring soon" warning email HTML body.
 */
function buildTokenExpiringEmail(name, expiryDate, daysLeft) {
    const urgencyColor = daysLeft <= 2 ? '#E65100' : '#F57F17';
    const urgencyBg = daysLeft <= 2 ? '#fff3e0' : '#fffde7';
    return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#fffde7;font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#fffde7;padding:40px 0;">
<tr><td align="center">
<table width="560" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
  <tr>
    <td style="background:linear-gradient(135deg,#F57F17 0%,#F9A825 60%,#FDD835 100%);padding:40px 48px;text-align:center;">
      <span style="font-size:48px;">⏰</span>
      <h1 style="margin:16px 0 8px;color:#fff;font-size:26px;font-weight:700;">Token Expires in ${daysLeft} Day${daysLeft !== 1 ? 's' : ''}</h1>
      <p style="margin:0;color:rgba(255,255,255,0.9);font-size:14px;">Expiry date: ${expiryDate.toDateString()}</p>
    </td>
  </tr>
  <tr>
    <td style="padding:36px 48px;">
      <p style="margin:0 0 20px;color:#555;font-size:15px;line-height:1.7;">Hello <strong>${name}</strong>,</p>
      <p style="margin:0 0 20px;color:#555;font-size:15px;line-height:1.7;">
        Your WhatsApp Business access token will expire in <strong>${daysLeft} day${daysLeft !== 1 ? 's' : ''}</strong> 
        (on ${expiryDate.toDateString()}). After that, messages and campaigns will stop working.
      </p>
      <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">
        <tr><td style="background:${urgencyBg};border-left:4px solid ${urgencyColor};border-radius:0 8px 8px 0;padding:12px 16px;color:#555;font-size:14px;">
          ⚡ <strong>Reconnect now</strong> to avoid any service interruption.
        </td></tr>
      </table>
      <p style="margin:0 0 24px;color:#555;font-size:15px;line-height:1.7;">
        Click the button below to go to your Sendzyy dashboard and click 
        <strong>"Connect WhatsApp"</strong>. The reconnection takes less than 2 minutes.
      </p>
      <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;">
        <tr><td align="center">
          <a href="https://app.sendzyy.com" target="_blank"
             style="display:inline-block;background:linear-gradient(135deg,#F57F17,#FDD835);color:#fff;text-decoration:none;font-size:16px;font-weight:700;padding:16px 48px;border-radius:50px;box-shadow:0 4px 14px rgba(245,127,23,0.4);">
            🔗 &nbsp; Reconnect Before It Expires
          </a>
        </td></tr>
      </table>
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr><td style="background:#f9f9f9;border-radius:10px;padding:14px 18px;">
          <p style="margin:0;color:#888;font-size:13px;line-height:1.6;">
            💡 <strong>Tip:</strong> Ask Sendzyy support about upgrading to a permanent 
            System User Token — it never expires so you'll never see this email again.
          </p>
        </td></tr>
      </table>
    </td>
  </tr>
  <tr>
    <td style="background:#f5f5f5;border-top:1px solid #eee;padding:24px 48px;text-align:center;">
      <p style="margin:0;color:#1B5E20;font-size:14px;font-weight:700;">Sendzyy</p>
      <p style="margin:4px 0 0;color:#aaa;font-size:12px;">© ${new Date().getFullYear()} Sendzyy · <a href="https://app.sendzyy.com" style="color:#4CAF50;text-decoration:none;">app.sendzyy.com</a></p>
    </td>
  </tr>
</table>
</td></tr>
</table>
</body>
</html>`;
}

/**
 * Initialize the scheduler: run startup recovery then start the cron jobs.
 *
 * @param {RetryScheduler}  retryScheduler      - RetryScheduler service instance
 * @param {mongoose.Model}  ScheduledRetryPhase - Mongoose model for scheduled phases
 * @param {mongoose.Model}  [Tenant]            - Optional: Tenant model for token watcher
 * @param {Object}          [transporter]       - Optional: Nodemailer transporter for token watcher
 * @param {Object}          [io]                - Optional: Socket.IO server for token watcher
 * @returns {Promise<cron.ScheduledTask>} The running retry cron task
 *
 * Requirements: 3.3, 3.5
 */
async function initScheduler(retryScheduler, ScheduledRetryPhase, Tenant, transporter, io) {
    // Task 13.2: Recover phases interrupted by a previous server restart
    await recoverExecutingPhases(ScheduledRetryPhase);

    // Task 13.1: Start the every-minute cron job
    const task = startRetrySchedulerCron(retryScheduler);

    // Task 16.1: Start the daily cleanup cron job for orphaned phases
    startCleanupCron(retryScheduler);

    // Token expiry watcher: daily at 9:00 AM
    if (Tenant && transporter) {
        startTokenExpiryWatcherCron(Tenant, transporter, io || null);
    }

    return task;
}

module.exports = {
    initScheduler,
    recoverExecutingPhases,
    startRetrySchedulerCron,
    startCleanupCron,
    startTokenExpiryWatcherCron,
};


