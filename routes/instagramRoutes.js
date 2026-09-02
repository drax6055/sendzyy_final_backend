const express = require('express');
const router = express.Router();
const axios = require('axios');
const jwt = require('jsonwebtoken');

const InstagramAutomation = require('../models/InstagramAutomation');
const InstagramAutomationSession = require('../models/InstagramAutomationSession');

// Helper to access Tenant model from req.app or mongoose
const getTenantModel = (req) => req.app.get('TenantModel') || require('mongoose').model('Tenant');

// Auth Middleware for protected Instagram endpoints
const authenticate = (req, res, next) => {
    const token = req.headers['authorization']?.split(' ')[1] || req.query.token;
    if (!token) return res.sendStatus(401);
    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) return res.sendStatus(403);
        if (!user.tenantId) return res.status(401).json({ error: 'Invalid session' });
        req.user = user;
        next();
    });
};

// 1. Initiate Instagram OAuth Flow (Public)
router.get('/auth', (req, res) => {
    try {
        const token = req.headers['authorization']?.split(' ')[1] || req.query.token;
        const clientId = process.env.INSTAGRAM_CLIENT_ID;
        const redirectUri = 'https://appapi.sendzyy.com/api/instagram/callback';
        const scope = 'instagram_business_basic,instagram_business_manage_messages,instagram_business_manage_comments,instagram_business_content_publish,instagram_business_manage_insights';
        const authUrl =
            `https://www.instagram.com/oauth/authorize` +
            `?force_reauth=true` +
            `&client_id=${clientId}` +
            `&redirect_uri=${redirectUri}` +
            `&response_type=code` +
            `&scope=${encodeURIComponent(scope)}` +
            `&state=${encodeURIComponent(token)}`;

        return res.redirect(authUrl);
    } catch (error) {
        console.error('[INSTAGRAM AUTH] ❌ ERROR:', error.message);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
});

// 2. OAuth Callback Handler (Public)
router.get('/callback', async (req, res) => {
    const { code, state } = req.query;
    const targetFrontend = 'https://app.sendzyy.com';
    const Tenant = getTenantModel(req);

    if (req.query.error) {
        console.error('[INSTAGRAM CALLBACK] ❌ Instagram returned an OAuth error:', req.query.error_description || req.query.error);
        return res.redirect(`${targetFrontend}/?error=${encodeURIComponent(req.query.error_description || req.query.error || 'Instagram authorization failed')}`);
    }

    if (!code || !state) {
        console.error('[INSTAGRAM CALLBACK] ❌ Missing code or state');
        return res.redirect(`${targetFrontend}/?error=${encodeURIComponent('Missing code or state')}`);
    }

    jwt.verify(state, process.env.JWT_SECRET, async (err, user) => {
        if (err || !user?.tenantId) {
            console.error('[INSTAGRAM CALLBACK] ❌ JWT verification failed or tenantId missing');
            return res.redirect(`${targetFrontend}/?error=${encodeURIComponent('Invalid state token or Tenant ID missing')}`);
        }

        const tenantId = user.tenantId;

        try {
            const clientId = process.env.INSTAGRAM_CLIENT_ID;
            const clientSecret = process.env.INSTAGRAM_CLIENT_SECRET || process.env.META_APP_SECRET;

            if (!clientId || !clientSecret) {
                throw new Error('INSTAGRAM_CLIENT_ID or INSTAGRAM_CLIENT_SECRET / META_APP_SECRET missing');
            }

            const redirectUri = 'https://appapi.sendzyy.com/api/instagram/callback';
            const tokenParams = new URLSearchParams();
            tokenParams.append('client_id', clientId);
            tokenParams.append('client_secret', clientSecret);
            tokenParams.append('grant_type', 'authorization_code');
            tokenParams.append('redirect_uri', redirectUri);
            tokenParams.append('code', code);

            const tokenResponse = await axios.post(
                'https://api.instagram.com/oauth/access_token',
                tokenParams,
                { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
            );

            const { access_token: shortLivedToken } = tokenResponse.data;
            if (!shortLivedToken) throw new Error('Instagram did not return short-lived access token');

            const longLivedResponse = await axios.get('https://graph.instagram.com/access_token', {
                params: {
                    grant_type: 'ig_exchange_token',
                    client_secret: clientSecret,
                    access_token: shortLivedToken
                }
            });

            const { access_token: longLivedToken, expires_in } = longLivedResponse.data;
            if (!longLivedToken) throw new Error('Instagram did not return long-lived access token');

            const profileResponse = await axios.get('https://graph.instagram.com/me', {
                params: { fields: 'id,username,name', access_token: longLivedToken }
            });

            const { id: instagramAccountId, username, name } = profileResponse.data;
            const tokenExpiry = expires_in ? new Date(Date.now() + expires_in * 1000) : null;

            const tenant = await Tenant.findById(tenantId);
            if (!tenant) {
                return res.redirect(`${targetFrontend}/?error=${encodeURIComponent('Tenant not found')}`);
            }

            tenant.instagramConfig = {
                instagramAccountId,
                igUserId: instagramAccountId,
                username,
                name: name || username,
                accessToken: longLivedToken,
                tokenExpiry,
                connected: true
            };

            await tenant.save();

            // Auto-subscribe Instagram Account to app webhooks
            try {
                const apiVer = (process.env.INSTA_META_API_VERSION || process.env.META_API_VERSION || 'v26.0').toLowerCase();
                console.log(`[INSTAGRAM SUBSCRIBED_APPS] Subscribing IG Account ${instagramAccountId} to app webhooks using version ${apiVer}...`);
                try {
                    await axios.post(
                        `https://graph.instagram.com/${apiVer}/${instagramAccountId}/subscribed_apps`,
                        { subscribed_fields: ['messages', 'messaging_postbacks', 'message_reactions', 'comments'] },
                        { headers: { Authorization: `Bearer ${longLivedToken}` } }
                    );
                } catch (igErr) {
                    await axios.post(
                        `https://graph.facebook.com/${apiVer}/${instagramAccountId}/subscribed_apps`,
                        { subscribed_fields: ['messages', 'messaging_postbacks', 'message_reactions', 'comments'] },
                        { headers: { Authorization: `Bearer ${longLivedToken}` } }
                    );
                }
            } catch (subErr) {
                console.error('[INSTAGRAM SUBSCRIBED_APPS] ⚠️ Subscription notice:', subErr.response?.data || subErr.message);
            }

            return res.redirect(`${targetFrontend}/?instagram_connected=true`);

        } catch (error) {
            console.error('[INSTAGRAM CALLBACK] ❌ ERROR DURING OAUTH FLOW:', error.message);
            const errMsg = error.response?.data?.error_message || error.response?.data?.error?.message || error.message || 'Instagram connection failed';
            return res.redirect(`${targetFrontend}/?error=${encodeURIComponent(errMsg)}`);
        }
    });
});

// 3. Get Instagram Profile Summary (Protected)
router.get('/profile', authenticate, async (req, res) => {
    try {
        const Tenant = getTenantModel(req);
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });

        const config = tenant.instagramConfig || {};
        const safeConfig = {
            instagramAccountId: config.instagramAccountId,
            igUserId: config.igUserId,
            username: config.username,
            name: config.name,
            tokenExpiry: config.tokenExpiry,
            connected: config.connected
        };

        return res.json(safeConfig);
    } catch (error) {
        console.error('[INSTAGRAM PROFILE API] ❌ ERROR:', error.message);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
});

// 4. Get Detailed Profile from Graph API (Protected)
router.get('/detailed-profile', authenticate, async (req, res) => {
    try {
        const Tenant = getTenantModel(req);
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });

        const config = tenant.instagramConfig;
        if (!config?.connected || !config?.accessToken || !config?.instagramAccountId) {
            return res.status(400).json({ error: 'Instagram not connected' });
        }

        const apiVer = (process.env.INSTA_META_API_VERSION || process.env.META_API_VERSION || 'v26.0').toLowerCase();
        const graphResponse = await axios.get(
            `https://graph.instagram.com/${apiVer}/${config.instagramAccountId}`,
            {
                params: {
                    fields: 'name,username,profile_picture_url,followers_count',
                    access_token: config.accessToken
                }
            }
        );

        return res.json(graphResponse.data);
    } catch (error) {
        console.error('[INSTAGRAM DETAILED PROFILE] ❌ Error:', error.response?.data || error.message);
        const errMsg = error.response?.data?.error?.message || error.message || 'Failed to fetch detailed profile';
        return res.status(500).json({ error: errMsg });
    }
});

// 5. Subscribe Webhooks (Protected)
router.post('/subscribe-webhooks', authenticate, async (req, res) => {
    try {
        const Tenant = getTenantModel(req);
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant || !tenant.instagramConfig?.connected) {
            return res.status(400).json({ error: 'Instagram account not connected' });
        }
        const { instagramAccountId, accessToken } = tenant.instagramConfig;
        const apiVer = (process.env.INSTA_META_API_VERSION || process.env.META_API_VERSION || 'v26.0').toLowerCase();

        let result = null;
        try {
            const response = await axios.post(
                `https://graph.facebook.com/${apiVer}/${instagramAccountId}/subscribed_apps`,
                { subscribed_fields: ['messages', 'messaging_postbacks', 'message_reactions', 'comments'] },
                { headers: { Authorization: `Bearer ${accessToken}` } }
            );
            result = response.data;
        } catch (err1) {
            const response = await axios.post(
                `https://graph.instagram.com/${apiVer}/${instagramAccountId}/subscribed_apps`,
                { subscribed_fields: ['messages', 'messaging_postbacks', 'message_reactions', 'comments'] },
                { headers: { Authorization: `Bearer ${accessToken}` } }
            );
            result = response.data;
        }
        return res.json({ success: true, result });
    } catch (error) {
        console.error('[INSTAGRAM SUBSCRIBE ROUTE] ❌ Error:', error.response?.data || error.message);
        return res.status(500).json({ error: error.response?.data || error.message });
    }
});

// 6. Get Insights Profile (Protected)
router.get('/insights-profile', authenticate, async (req, res) => {
    try {
        const Tenant = getTenantModel(req);
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant || !tenant.instagramConfig?.connected) {
            return res.status(400).json({ error: 'Instagram not connected' });
        }
        const { accessToken, instagramAccountId } = tenant.instagramConfig;
        if (!accessToken) return res.status(400).json({ error: 'Access token missing' });

        const apiVer = (process.env.INSTA_META_API_VERSION || process.env.META_API_VERSION || 'v26.0').toLowerCase();
        let profileData = null;
        try {
            const resMe = await axios.get(`https://graph.instagram.com/${apiVer}/me`, {
                params: {
                    fields: 'id,username,name,profile_picture_url,followers_count,follows_count,media_count',
                    access_token: accessToken
                }
            });
            profileData = resMe.data;
        } catch (meErr) {
            const resAccount = await axios.get(`https://graph.instagram.com/${apiVer}/${instagramAccountId}`, {
                params: {
                    fields: 'id,username,name,profile_picture_url,followers_count,follows_count,media_count',
                    access_token: accessToken
                }
            });
            profileData = resAccount.data;
        }
        return res.json(profileData);
    } catch (error) {
        console.error('[INSTAGRAM INSIGHTS PROFILE] ❌ Error:', error.response?.data || error.message);
        return res.status(500).json({ error: error.response?.data?.error?.message || error.message || 'Failed to fetch insights profile' });
    }
});

// 7. Get Media Feed (Protected)
router.get('/media', authenticate, async (req, res) => {
    try {
        const Tenant = getTenantModel(req);
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant || !tenant.instagramConfig?.connected) {
            return res.status(400).json({ error: 'Instagram not connected' });
        }
        const { accessToken, instagramAccountId } = tenant.instagramConfig;
        if (!accessToken) return res.status(400).json({ error: 'Access token missing' });

        const apiVer = (process.env.INSTA_META_API_VERSION || process.env.META_API_VERSION || 'v26.0').toLowerCase();
        let mediaData = null;
        try {
            const resMedia = await axios.get(`https://graph.instagram.com/${apiVer}/me/media`, {
                params: {
                    fields: 'id,caption,media_type,media_product_type,media_url,thumbnail_url,permalink,timestamp',
                    access_token: accessToken
                }
            });
            mediaData = resMedia.data;
        } catch (meMediaErr) {
            const resMediaAccount = await axios.get(`https://graph.instagram.com/${apiVer}/${instagramAccountId}/media`, {
                params: {
                    fields: 'id,caption,media_type,media_product_type,media_url,thumbnail_url,permalink,timestamp',
                    access_token: accessToken
                }
            });
            mediaData = resMediaAccount.data;
        }
        return res.json(mediaData);
    } catch (error) {
        console.error('[INSTAGRAM MEDIA API] ❌ Error:', error.response?.data || error.message);
        return res.status(500).json({ error: error.response?.data?.error?.message || error.message || 'Failed to fetch Instagram media' });
    }
});

// 8. Disconnect Profile (Protected)
router.post('/disconnect', authenticate, async (req, res) => {
    try {
        const Tenant = getTenantModel(req);
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });

        tenant.instagramConfig = {
            instagramAccountId: '',
            username: '',
            name: '',
            accessToken: '',
            tokenExpiry: null,
            connected: false
        };

        await tenant.save();
        return res.json({ success: true, message: 'Instagram profile disconnected' });
    } catch (error) {
        console.error('[INSTAGRAM DISCONNECT] ❌ ERROR:', error.message);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
});

// ── Instagram Automations CRUD (Protected) ───────────────────────────────────

// Create automation
router.post('/automations', authenticate, async (req, res) => {
    try {
        const { name, triggerType, triggerKeywords, replyMessage, quickReplies, payloadResponses } = req.body;
        if (!name || !replyMessage) {
            return res.status(400).json({ error: 'name and replyMessage are required' });
        }
        if (quickReplies && quickReplies.length > 13) {
            return res.status(400).json({ error: 'Maximum 13 quick reply buttons allowed' });
        }

        const automation = await InstagramAutomation.create({
            tenantId: req.user.tenantId,
            name,
            triggerType: triggerType || 'keyword',
            triggerKeywords: triggerKeywords || [],
            replyMessage,
            quickReplies: quickReplies || [],
            payloadResponses: payloadResponses || {},
            isActive: true,
        });

        return res.status(201).json(automation);
    } catch (error) {
        console.error('[INSTAGRAM AUTOMATION] Create error:', error.message);
        return res.status(500).json({ error: 'Failed to create automation' });
    }
});

// List all automations for tenant
router.get('/automations', authenticate, async (req, res) => {
    try {
        const automations = await InstagramAutomation.find({ tenantId: req.user.tenantId }).sort({ createdAt: -1 });
        return res.json(automations);
    } catch (error) {
        console.error('[INSTAGRAM AUTOMATION] List error:', error.message);
        return res.status(500).json({ error: 'Failed to fetch automations' });
    }
});

// Get single automation
router.get('/automations/:id', authenticate, async (req, res) => {
    try {
        const automation = await InstagramAutomation.findOne({ _id: req.params.id, tenantId: req.user.tenantId });
        if (!automation) return res.status(404).json({ error: 'Automation not found' });
        return res.json(automation);
    } catch (error) {
        console.error('[INSTAGRAM AUTOMATION] Get error:', error.message);
        return res.status(500).json({ error: 'Failed to fetch automation' });
    }
});

// Update automation
router.put('/automations/:id', authenticate, async (req, res) => {
    try {
        const { name, triggerType, triggerKeywords, replyMessage, quickReplies, payloadResponses, isActive } = req.body;
        if (quickReplies && quickReplies.length > 13) {
            return res.status(400).json({ error: 'Maximum 13 quick reply buttons allowed' });
        }

        const automation = await InstagramAutomation.findOneAndUpdate(
            { _id: req.params.id, tenantId: req.user.tenantId },
            { name, triggerType, triggerKeywords, replyMessage, quickReplies, payloadResponses, isActive },
            { new: true, runValidators: true }
        );

        if (!automation) return res.status(404).json({ error: 'Automation not found' });
        return res.json(automation);
    } catch (error) {
        console.error('[INSTAGRAM AUTOMATION] Update error:', error.message);
        return res.status(500).json({ error: 'Failed to update automation' });
    }
});

// Toggle active/inactive
router.patch('/automations/:id/toggle', authenticate, async (req, res) => {
    try {
        const automation = await InstagramAutomation.findOne({ _id: req.params.id, tenantId: req.user.tenantId });
        if (!automation) return res.status(404).json({ error: 'Automation not found' });

        automation.isActive = !automation.isActive;
        await automation.save();
        return res.json({ id: automation._id, isActive: automation.isActive });
    } catch (error) {
        console.error('[INSTAGRAM AUTOMATION] Toggle error:', error.message);
        return res.status(500).json({ error: 'Failed to toggle automation' });
    }
});

// Delete automation
router.delete('/automations/:id', authenticate, async (req, res) => {
    try {
        const result = await InstagramAutomation.findOneAndDelete({ _id: req.params.id, tenantId: req.user.tenantId });
        if (!result) return res.status(404).json({ error: 'Automation not found' });

        await InstagramAutomationSession.deleteMany({ automationId: req.params.id });
        return res.json({ success: true });
    } catch (error) {
        console.error('[INSTAGRAM AUTOMATION] Delete error:', error.message);
        return res.status(500).json({ error: 'Failed to delete automation' });
    }
});

module.exports = router;
