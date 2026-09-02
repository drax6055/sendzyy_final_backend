const axios = require('axios');

/**
 * Process incoming Instagram / Page DM Webhooks
 * 100% identical logic extracted from server.js
 */
async function handleInstagramWebhook(req, res, body, Tenant, InstagramAutomation, InstagramAutomationSession) {
    res.sendStatus(200); // Respond immediately to Meta
    console.log('[IG WEBHOOK] Received Instagram/Page webhook. object:', body.object);
    console.log('[IG WEBHOOK] Full body:', JSON.stringify(body, null, 2));

    for (const entry of body.entry || []) {
        const entryId = entry.id;
        const messagingEvents = entry.messaging || entry.standby || [];
        for (const messaging of messagingEvents) {
            const senderId = messaging.sender?.id;
            const recipientId = messaging.recipient?.id;

            console.log('[IG WEBHOOK] sender:', senderId, '| recipient:', recipientId, '| entryId:', entryId);

            if (!senderId || !recipientId) continue;

            // Find tenant by matching recipientId, senderId, entryId, or igUserId
            let resolvedTenant = await Tenant.findOne({
                $or: [
                    { 'instagramConfig.instagramAccountId': recipientId },
                    { 'instagramConfig.instagramAccountId': senderId },
                    { 'instagramConfig.instagramAccountId': entryId },
                    { 'instagramConfig.igUserId': recipientId },
                    { 'instagramConfig.igUserId': entryId }
                ],
                'instagramConfig.connected': true
            });

            if (!resolvedTenant) {
                // Fallback: match connected tenant in system and register igUserId
                const connectedTenants = await Tenant.find({ 'instagramConfig.connected': true });
                if (connectedTenants.length > 0) {
                    resolvedTenant = connectedTenants[0];
                    console.log(`[IG WEBHOOK] 💡 Auto-matching incoming IG webhook (recipient: ${recipientId}, entry: ${entryId}) to tenant: ${resolvedTenant._id}`);
                    resolvedTenant.instagramConfig.igUserId = recipientId;
                    await resolvedTenant.save();
                }
            }

            if (!resolvedTenant) {
                console.warn('[IG WEBHOOK] No tenant found for recipientId:', recipientId, '| senderId:', senderId, '| entryId:', entryId);
                const allTenants = await Tenant.find({ 'instagramConfig.connected': true }, { 'instagramConfig.instagramAccountId': 1 });
                console.warn('[IG WEBHOOK] Connected accounts:', allTenants.map(t => t.instagramConfig.instagramAccountId));
                continue;
            }

            const tenantId = resolvedTenant._id.toString();
            const accessToken = resolvedTenant.instagramConfig.accessToken;
            const igUserId = recipientId || resolvedTenant.instagramConfig.igUserId || resolvedTenant.instagramConfig.instagramAccountId;

            // Determine actual IGSID of the user sending the DM
            const actualSenderId = (senderId === igUserId || senderId === resolvedTenant.instagramConfig.instagramAccountId || senderId === entryId)
                ? recipientId
                : senderId;

            console.log('[IG WEBHOOK] Tenant found:', tenantId, '| igUserId:', igUserId, '| actualSenderId:', actualSenderId);

            // Helper: send message via Instagram Graph API
            const sendInstagramMessage = async (payload) => {
                try {
                    const apiVer = (process.env.INSTA_META_API_VERSION || process.env.META_API_VERSION || 'v26.0').toLowerCase();
                    console.log('[IG WEBHOOK] Sending message payload:', JSON.stringify(payload));
                    const response = await axios.post(
                        `https://graph.instagram.com/${apiVer}/${igUserId}/messages`,
                        payload,
                        { headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' } }
                    );
                    console.log('[IG WEBHOOK] ✅ Message sent. Response:', response.data);
                } catch (err) {
                    console.error('[IG WEBHOOK] ❌ Send message error:', err.response?.data || err.message);
                }
            };

            // ── Case 1: User clicked a Quick Reply button (postback) ──
            if (messaging.message?.quick_reply) {
                const payload = messaging.message.quick_reply.payload;
                console.log('[IG WEBHOOK] Quick reply payload:', payload, '| from IGSID:', actualSenderId);

                const automations = await InstagramAutomation.find({ tenantId, isActive: true });
                let responded = false;

                for (const automation of automations) {
                    const responseText = automation.payloadResponses?.get
                        ? automation.payloadResponses.get(payload)
                        : automation.payloadResponses?.[payload];

                    if (responseText) {
                        await sendInstagramMessage({
                            recipient: { id: actualSenderId },
                            messaging_type: 'RESPONSE',
                            message: { text: responseText }
                        });
                        responded = true;
                        break;
                    }
                }

                if (!responded) {
                    console.log('[IG WEBHOOK] No payloadResponse found for payload:', payload);
                }
                continue;
            }

            // ── Case 2: Regular incoming DM ──
            if (messaging.message?.text) {
                const incomingText = messaging.message.text.trim().toLowerCase();
                console.log('[IG WEBHOOK] Incoming DM text:', incomingText, '| from IGSID:', actualSenderId);

                const automations = await InstagramAutomation.find({ tenantId, isActive: true }).sort({ createdAt: 1 });
                console.log('[IG WEBHOOK] Active automations count:', automations.length);

                if (!automations.length) {
                    console.log('[IG WEBHOOK] No active automations found for tenant:', tenantId);
                    continue;
                }

                let matched = null;

                for (const automation of automations) {
                    if (automation.triggerType === 'any_dm') {
                        matched = automation;
                        break;
                    }
                    if (automation.triggerType === 'keyword') {
                        const keywords = automation.triggerKeywords.map(k => k.toLowerCase());
                        console.log('[IG WEBHOOK] Checking keywords:', keywords, 'against:', incomingText);
                        if (keywords.some(k => incomingText.includes(k))) {
                            matched = automation;
                            break;
                        }
                    }
                }

                if (!matched) {
                    console.log('[IG WEBHOOK] No matching automation for text:', incomingText);
                    continue;
                }

                console.log('[IG WEBHOOK] Matched automation:', matched.name);

                // 24h spam prevention (only for 'any_dm' triggers, keyword triggers always respond)
                if (matched.triggerType === 'any_dm') {
                    const sessionKey = { tenantId, igsid: actualSenderId };
                    const existingSession = await InstagramAutomationSession.findOne(sessionKey);
                    if (existingSession) {
                        const hoursSince = (Date.now() - existingSession.lastTriggerAt.getTime()) / (1000 * 60 * 60);
                        if (hoursSince < 24) {
                            console.log('[IG WEBHOOK] Any DM trigger already sent within 24h, skipping. Hours since:', hoursSince.toFixed(1));
                            continue;
                        }
                        existingSession.automationId = matched._id.toString();
                        existingSession.lastTriggerAt = new Date();
                        await existingSession.save();
                    } else {
                        await InstagramAutomationSession.create({
                            tenantId,
                            igsid: actualSenderId,
                            automationId: matched._id.toString(),
                            lastTriggerAt: new Date()
                        });
                    }
                }

                // Build quick reply payload
                const messagePayload = {
                    recipient: { id: actualSenderId },
                    messaging_type: 'RESPONSE',
                    message: {
                        text: matched.replyMessage,
                        ...(matched.quickReplies.length > 0 && {
                            quick_replies: matched.quickReplies.map(qr => ({
                                content_type: 'text',
                                title: qr.title,
                                payload: qr.payload
                            }))
                        })
                    }
                };

                await sendInstagramMessage(messagePayload);
            }
        }
    }
}

module.exports = {
    handleInstagramWebhook
};
