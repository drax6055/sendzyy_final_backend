/**
 * FCMService
 *
 * Helper service that wraps firebase-admin SDK to send push notifications
 * to single devices, tenant topics, or multicast token groups.
 */

const admin = require('firebase-admin');
const path = require('path');
const fs = require('fs');

let messagingInstance = null;

function getMessaging() {
    if (messagingInstance) return messagingInstance;

    if (!admin.apps.length) {
        const serviceAccountPath = path.join(__dirname, '..', 'serviceAccountKey.json');
        if (!fs.existsSync(serviceAccountPath)) {
            console.warn('[FCMService] serviceAccountKey.json not found. FCM push notifications disabled.');
            return null;
        }

        try {
            const serviceAccount = require(serviceAccountPath);
            admin.initializeApp({
                credential: admin.credential.cert(serviceAccount)
            });
            console.log('[FCMService] Firebase Admin initialized successfully.');
        } catch (err) {
            console.error('[FCMService] Failed to initialize Firebase Admin:', err.message);
            return null;
        }
    }

    messagingInstance = admin.messaging();
    return messagingInstance;
}

const FCMService = {
    /**
     * Send push notification to a single FCM device token
     */
    async sendToDevice(token, { title, body, data = {}, imageUrl = null }) {
        const messaging = getMessaging();
        if (!messaging) return { success: false, reason: 'FCM not initialized' };

        const stringData = {};
        for (const key of Object.keys(data)) {
            stringData[key] = String(data[key] ?? '');
        }

        const message = {
            token,
            notification: {
                title,
                body,
                ...(imageUrl ? { imageUrl } : {})
            },
            data: stringData,
            android: {
                priority: 'high',
                notification: {
                    sound: 'default',
                    channelId: 'sendzyy_notifications',
                    ...(imageUrl ? { imageUrl } : {})
                }
            },
            apns: {
                headers: { 'apns-priority': '10' },
                payload: {
                    aps: {
                        sound: 'default',
                        badge: 1,
                        contentAvailable: true
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
     * Send push notification to all devices subscribed to a tenant topic
     */
    async sendToTenant(tenantId, { title, body, data = {}, imageUrl = null }) {
        const messaging = getMessaging();
        if (!messaging) return { success: false, reason: 'FCM not initialized' };

        const topic = `tenant_${tenantId}`;
        const stringData = {};
        for (const key of Object.keys(data)) {
            stringData[key] = String(data[key] ?? '');
        }

        const message = {
            topic,
            notification: {
                title,
                body,
                ...(imageUrl ? { imageUrl } : {})
            },
            data: stringData,
            android: {
                priority: 'high',
                notification: { sound: 'default', channelId: 'sendzyy_notifications' }
            },
            apns: {
                headers: { 'apns-priority': '10' },
                payload: { aps: { sound: 'default', contentAvailable: true } }
            },
            webpush: {
                headers: { Urgency: 'high' },
                notification: { title, body, icon: '/icons/icon-192.png' }
            }
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
        const messaging = getMessaging();
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
        const messaging = getMessaging();
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
