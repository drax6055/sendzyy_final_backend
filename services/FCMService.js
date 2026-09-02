const { initializeApp, getApps, cert } = require('firebase-admin/app');
const { getMessaging } = require('firebase-admin/messaging');
const path = require('path');
const fs = require('fs');

let messagingInstance = null;

function getMessagingClient() {
    if (messagingInstance) return messagingInstance;

    if (!getApps().length) {
        const serviceAccountPath = path.join(__dirname, '..', 'serviceAccountKey.json');
        if (!fs.existsSync(serviceAccountPath)) {
            console.warn('[FCMService] serviceAccountKey.json not found. FCM push notifications disabled.');
            return null;
        }

        try {
            const serviceAccount = require(serviceAccountPath);
            initializeApp({
                credential: cert(serviceAccount)
            });
            console.log('[FCMService] Firebase Admin initialized successfully.');
        } catch (err) {
            console.error('[FCMService] Failed to initialize Firebase Admin:', err.message);
            return null;
        }
    }

    try {
        messagingInstance = getMessaging();
        return messagingInstance;
    } catch (err) {
        console.error('[FCMService] getMessaging failed:', err.message);
        return null;
    }
}

function buildPayload({ title, body, data = {}, imageUrl = null }) {
    const stringData = {
        title: String(title || ''),
        body: String(body || ''),
        click_action: 'FLUTTER_NOTIFICATION_CLICK'
    };
    for (const key of Object.keys(data)) {
        stringData[key] = String(data[key] ?? '');
    }

    return {
        notification: {
            title,
            body,
            ...(imageUrl ? { imageUrl } : {})
        },
        data: stringData,
        android: {
            priority: 'high',
            notification: {
                title,
                body,
                sound: 'default',
                channelId: 'sendzyy_notifications',
                priority: 'high',
                defaultSound: true,
                defaultVibrateTimings: true,
                visibility: 'public',
                clickAction: 'FLUTTER_NOTIFICATION_CLICK',
                ...(imageUrl ? { imageUrl } : {})
            }
        },
        apns: {
            headers: { 'apns-priority': '10' },
            payload: {
                aps: {
                    alert: { title, body },
                    sound: 'default',
                    badge: 1
                }
            }
        },
        webpush: {
            headers: { Urgency: 'high' },
            notification: {
                title,
                body,
                icon: '/icons/icon-192.png',
                ...(imageUrl ? { image: imageUrl } : {})
            }
        }
    };
}

const FCMService = {
    /**
     * Send push notification to a single FCM device token
     */
    async sendToDevice(token, { title, body, data = {}, imageUrl = null }) {
        const messaging = getMessagingClient();
        if (!messaging) return { success: false, reason: 'FCM not initialized' };

        const basePayload = buildPayload({ title, body, data, imageUrl });
        const message = {
            token,
            ...basePayload
        };

        try {
            const response = await messaging.send(message);
            return { success: true, messageId: response };
        } catch (error) {
            if (error.code === 'messaging/registration-token-not-registered') {
                return { success: false, invalidToken: true, error: error.message };
            }
            console.error('[FCMService] Send to device error:', error.message);
            return { success: false, error: error.message };
        }
    },

    /**
     * Send push notification to multiple device tokens directly
     */
    async sendMulticast(tokens, { title, body, data = {}, imageUrl = null }) {
        const messaging = getMessagingClient();
        if (!messaging) return { success: false, reason: 'FCM not initialized' };
        if (!tokens || !tokens.length) return { success: true, sentCount: 0 };

        const basePayload = buildPayload({ title, body, data, imageUrl });
        const message = {
            tokens,
            ...basePayload
        };

        try {
            const response = await messaging.sendEachForMulticast(message);
            console.log(`[FCMService] Multicast result: ${response.successCount} success, ${response.failureCount} failure out of ${tokens.length} tokens`);

            // Clean up invalid/expired tokens asynchronously
            if (response.failureCount > 0) {
                const mongoose = require('mongoose');
                const FCMToken = mongoose.models.FCMToken;
                if (FCMToken) {
                    const invalidTokens = [];
                    response.responses.forEach((resp, idx) => {
                        if (!resp.success && resp.error) {
                            const code = resp.error.code;
                            if (code === 'messaging/registration-token-not-registered' || code === 'messaging/invalid-registration-token') {
                                invalidTokens.push(tokens[idx]);
                            }
                        }
                    });
                    if (invalidTokens.length > 0) {
                        FCMToken.deleteMany({ token: { $in: invalidTokens } }).catch(err => {
                            console.warn('[FCMService] Error cleaning up stale tokens:', err.message);
                        });
                    }
                }
            }

            return {
                success: response.successCount > 0,
                successCount: response.successCount,
                failureCount: response.failureCount,
                responses: response.responses
            };
        } catch (error) {
            console.error('[FCMService] Send multicast error:', error.message);
            return { success: false, error: error.message };
        }
    },

    /**
     * Send push notification to all devices subscribed to a tenant topic
     */
    async sendToTenant(tenantId, { title, body, data = {}, imageUrl = null }) {
        const messaging = getMessagingClient();
        if (!messaging) return { success: false, reason: 'FCM not initialized' };

        const topic = `tenant_${tenantId}`;
        const basePayload = buildPayload({ title, body, data, imageUrl });
        const message = {
            topic,
            ...basePayload
        };

        try {
            const response = await messaging.send(message);
            return { success: true, messageId: response };
        } catch (error) {
            console.error(`[FCMService] Send to topic ${topic} error:`, error.message);
            return { success: false, error: error.message };
        }
    },

    /**
     * Subscribe an FCM token to a tenant topic
     */
    async subscribeToTenantTopic(tokens, tenantId) {
        const messaging = getMessagingClient();
        if (!messaging) return;
        const topic = `tenant_${tenantId}`;
        const tokenList = Array.isArray(tokens) ? tokens : [tokens];
        try {
            await messaging.subscribeToTopic(tokenList, topic);
            console.log(`[FCMService] Subscribed ${tokenList.length} tokens to topic ${topic}`);
        } catch (err) {
            console.error(`[FCMService] Error subscribing to topic ${topic}:`, err.message);
        }
    },

    /**
     * Unsubscribe an FCM token from a tenant topic
     */
    async unsubscribeFromTenantTopic(tokens, tenantId) {
        const messaging = getMessagingClient();
        if (!messaging) return;
        const topic = `tenant_${tenantId}`;
        const tokenList = Array.isArray(tokens) ? tokens : [tokens];
        try {
            await messaging.unsubscribeFromTopic(tokenList, topic);
            console.log(`[FCMService] Unsubscribed ${tokenList.length} tokens from topic ${topic}`);
        } catch (err) {
            console.error(`[FCMService] Error unsubscribing from topic ${topic}:`, err.message);
        }
    }
};

module.exports = FCMService;

