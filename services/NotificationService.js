/**
 * NotificationService
 *
 * Handles creation, retrieval, read state updating, and real-time/FCM dispatch
 * of notifications for Sendzyy.
 */

const FCMService = require('./FCMService');
const SocketEmitter = require('./SocketEmitter');
const mongoose = require('mongoose');

const NotificationService = {
    /**
     * Create a notification in MongoDB, emit real-time Socket event, and dispatch FCM push
     */
    async create({ tenantId, title, body, type, category, actionData = {}, imageUrl = null }) {
        if (!tenantId || !title || !body) {
            console.warn('[NotificationService] Missing required parameters for notification creation');
            return null;
        }

        const Notification = mongoose.model('Notification');
        const FCMToken = mongoose.model('FCMToken');

        try {
            // 1. Save to MongoDB
            const notification = new Notification({
                tenantId,
                title,
                body,
                type: type || 'system',
                category: category || 'system',
                actionData,
                imageUrl
            });
            await notification.save();

            // 2. Fetch unread count for badge
            const unreadCount = await Notification.countDocuments({
                tenantId,
                isRead: false,
                isDeleted: false
            });

            // 3. Emit real-time Socket.io event to tenant room
            try {
                // If SocketEmitter has io, emit 'notification:new' and 'notification:count'
                const notificationData = {
                    notification: notification.toObject(),
                    unreadCount
                };

                // Emit via SocketEmitter custom method or generic io if set
                if (SocketEmitter._io) {
                    SocketEmitter._io.to(tenantId).emit('notification:new', notificationData);
                    SocketEmitter._io.to(tenantId).emit('notification:count', { unreadCount });
                }
            } catch (sockErr) {
                console.warn('[NotificationService] Socket emit error:', sockErr.message);
            }

            // 4. Dispatch FCM Push Notification to tenant devices
            try {
                const activeTokens = await FCMToken.find({ tenantId, isActive: true }).select('token');
                const tokenList = [...new Set(activeTokens.map(t => t.token).filter(Boolean))];

                const pushPayload = {
                    title,
                    body,
                    imageUrl,
                    data: {
                        notificationId: notification._id.toString(),
                        type: type || 'system',
                        category: category || 'system',
                        unreadCount: String(unreadCount),
                        ...actionData
                    }
                };

                let fcmResult = { success: false };
                // Send direct multicast to all active tokens for instant high-priority delivery
                if (tokenList.length > 0) {
                    fcmResult = await FCMService.sendMulticast(tokenList, pushPayload);
                } else {
                    // Fallback to topic broadcast only if no individual device tokens are registered
                    fcmResult = await FCMService.sendToTenant(tenantId, pushPayload);
                }

                if (fcmResult.success) {
                    notification.pushSent = true;
                    notification.deliveredAt = new Date();
                    notification.fcmMessageId = fcmResult.messageId;
                    await notification.save();
                }
            } catch (fcmErr) {
                console.warn('[NotificationService] FCM send error:', fcmErr.message);
            }

            return notification;
        } catch (err) {
            console.error('[NotificationService] Error creating notification:', err.message);
            return null;
        }
    },

    /**
     * Get paginated notifications for a tenant
     */
    async getForTenant(tenantId, { page = 1, limit = 20, category = null, unreadOnly = false }) {
        const Notification = mongoose.model('Notification');

        const query = { tenantId, isDeleted: false };
        if (category && category !== 'all') {
            query.category = category;
        }
        if (unreadOnly) {
            query.isRead = false;
        }

        const numericPage = Math.max(1, parseInt(page, 10) || 1);
        const numericLimit = Math.min(100, Math.max(1, parseInt(limit, 10) || 20));

        const [notifications, total, unreadCount] = await Promise.all([
            Notification.find(query)
                .sort({ createdAt: -1 })
                .skip((numericPage - 1) * numericLimit)
                .limit(numericLimit)
                .lean(),
            Notification.countDocuments(query),
            Notification.countDocuments({ tenantId, isRead: false, isDeleted: false })
        ]);

        return {
            notifications,
            total,
            unreadCount,
            page: numericPage,
            pages: Math.ceil(total / numericLimit) || 1
        };
    },

    /**
     * Get unread count for badge display
     */
    async getUnreadCount(tenantId) {
        const Notification = mongoose.model('Notification');
        return Notification.countDocuments({ tenantId, isRead: false, isDeleted: false });
    },

    /**
     * Mark a single notification as read
     */
    async markRead(notificationId, tenantId) {
        const Notification = mongoose.model('Notification');
        const notification = await Notification.findOneAndUpdate(
            { _id: notificationId, tenantId },
            { isRead: true, readAt: new Date() },
            { new: true }
        );

        const unreadCount = await Notification.countDocuments({ tenantId, isRead: false, isDeleted: false });

        if (SocketEmitter._io) {
            SocketEmitter._io.to(tenantId).emit('notification:count', { unreadCount });
        }

        return { notification, unreadCount };
    },

    /**
     * Mark all notifications as read for a tenant
     */
    async markAllRead(tenantId) {
        const Notification = mongoose.model('Notification');
        await Notification.updateMany(
            { tenantId, isRead: false, isDeleted: false },
            { isRead: true, readAt: new Date() }
        );

        if (SocketEmitter._io) {
            SocketEmitter._io.to(tenantId).emit('notification:count', { unreadCount: 0 });
        }

        return { success: true, unreadCount: 0 };
    },

    /**
     * Delete notification (soft delete)
     */
    async delete(notificationId, tenantId) {
        const Notification = mongoose.model('Notification');
        const notification = await Notification.findOneAndUpdate(
            { _id: notificationId, tenantId },
            { isDeleted: true },
            { new: true }
        );

        const unreadCount = await Notification.countDocuments({ tenantId, isRead: false, isDeleted: false });
        if (SocketEmitter._io) {
            SocketEmitter._io.to(tenantId).emit('notification:count', { unreadCount });
        }

        return { notification, unreadCount };
    }
};

module.exports = NotificationService;
