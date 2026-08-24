/**
 * CampaignExecutor Service
 *
 * Orchestrates the initial bulk send loop for a campaign. Responsible for:
 *   - Iterating over each recipient and sending a WhatsApp template message
 *   - Creating Recipient documents and StatusMapping records in the database
 *   - Emitting real-time progress events via SocketEmitter
 *   - Updating Campaign success/failure counts after the loop completes
 *
 * This service is fire-and-forget from the HTTP layer. All errors are caught and
 * logged internally; fatal errors mark the campaign status as 'error'.
 *
 * Dependencies are injected via the constructor so the class is fully unit-testable.
 */

const WHATSAPP_API_URL = 'https://graph.facebook.com/v25.0';

class CampaignExecutor {
    /**
     * @param {Object} deps
     * @param {import('mongoose').Model} deps.Campaign        - Mongoose Campaign model
     * @param {import('mongoose').Model} deps.Recipient       - Mongoose Recipient model
     * @param {import('mongoose').Model} deps.StatusMapping   - Mongoose StatusMapping model
     * @param {import('mongoose').Model} deps.Tenant          - Mongoose Tenant model
     * @param {Object}                  deps.axios            - Axios instance for HTTP requests
     * @param {Object}                  deps.socketEmitter    - SocketEmitter singleton
     * @param {Object}                  deps.campaignLifecycleManager - CampaignLifecycleManager instance
     */
    constructor({ Campaign, Recipient, StatusMapping, Tenant, axios, socketEmitter, campaignLifecycleManager } = {}) {
        if (!Campaign) throw new Error('Campaign model is required');
        if (!Recipient) throw new Error('Recipient model is required');
        if (!StatusMapping) throw new Error('StatusMapping model is required');
        if (!Tenant) throw new Error('Tenant model is required');
        if (!axios) throw new Error('axios is required');
        if (!socketEmitter) throw new Error('socketEmitter is required');

        this.Campaign = Campaign;
        this.Recipient = Recipient;
        this.StatusMapping = StatusMapping;
        this.Tenant = Tenant;
        this.axios = axios;
        this.socketEmitter = socketEmitter;
        this.campaignLifecycleManager = campaignLifecycleManager || null;
    }

    /**
     * Execute the initial bulk send for a campaign.
     *
     * Iterates over each recipient, sends a WhatsApp template message, persists
     * Recipient and StatusMapping documents, and emits real-time progress events.
     * After the loop, updates the Campaign document with final success/failure counts.
     *
     * This method is designed to be called fire-and-forget from the HTTP layer.
     *
     * @param {string}   campaignId  - Unique campaign identifier
     * @param {string}   tenantId    - Tenant identifier
     * @param {Array}    recipients  - Array of recipient objects { mobileNumber, variables?, headerVariables? }
     * @param {string}   template    - WhatsApp template name
     * @param {string}   language    - Template language code (e.g. 'en_US')
     * @param {string}   [mediaId]   - Optional media ID for header component
     * @param {string}   [mediaType] - Optional media type ('image'|'video'|'document')
     * @returns {Promise<Object>}    Summary: { campaignId, successCount, failureCount }
     */
    async executeCampaign(campaignId, tenantId, recipients, template, language, mediaId, mediaType) {
        if (!campaignId) throw new Error('campaignId is required');
        if (!tenantId) throw new Error('tenantId is required');
        if (!recipients || !Array.isArray(recipients)) throw new Error('recipients must be an array');
        if (!template) throw new Error('template is required');
        if (!language) throw new Error('language is required');

        console.log(JSON.stringify({
            service: 'CampaignExecutor',
            event: 'campaign_execution_start',
            campaignId,
            tenantId,
            totalCount: recipients.length,
            timestamp: new Date().toISOString()
        }));

        // Fetch tenant WhatsApp config once for the entire campaign
        let tenant;
        try {
            tenant = await this.Tenant.findById(tenantId);
        } catch (err) {
            console.error(JSON.stringify({
                service: 'CampaignExecutor',
                event: 'tenant_lookup_error',
                campaignId,
                tenantId,
                error: err.message,
                timestamp: new Date().toISOString()
            }));
            await this._markCampaignError(campaignId, `Tenant lookup failed: ${err.message}`);
            throw err;
        }

        if (!tenant) {
            const msg = `Tenant not found: ${tenantId}`;
            await this._markCampaignError(campaignId, msg);
            throw new Error(msg);
        }

        const config = tenant.whatsappConfig;
        if (!config?.accessToken || !config?.phoneNumberId) {
            const msg = 'WhatsApp not configured for tenant';
            await this._markCampaignError(campaignId, msg);
            throw new Error(msg);
        }

        const totalCount = recipients.length;
        let successCount = 0;
        let failureCount = 0;
        let sentCount = 0;

        const BATCH_SIZE = 10;
        for (let i = 0; i < recipients.length; i += BATCH_SIZE) {
            const chunk = recipients.slice(i, i + BATCH_SIZE);
            const recipientDocs = [];
            const statusMappingOps = [];

            await Promise.all(chunk.map(async (recipient) => {
                const to = recipient.mobileNumber || recipient.to;
                if (!to) {
                    console.warn(JSON.stringify({
                        service: 'CampaignExecutor',
                        event: 'recipient_skip_missing_number',
                        campaignId,
                        timestamp: new Date().toISOString()
                    }));
                    failureCount++;
                    sentCount++;
                    return;
                }

                try {
                    // Build the WhatsApp API payload
                    const payload = this._buildPayload(to, template, language, recipient.variables, mediaId, mediaType, recipient.headerVariables);

                    // Send via WhatsApp Cloud API
                    const response = await this.axios.post(
                        `${WHATSAPP_API_URL}/${config.phoneNumberId}/messages`,
                        payload,
                        { headers: { Authorization: `Bearer ${config.accessToken}` } }
                    );

                    const wamid = response.data?.messages?.[0]?.id;
                    if (!wamid) throw new Error('No wamid returned from WhatsApp API');

                    const sentAt = new Date().toISOString();

                    // Queue Recipient document for bulk insert
                    recipientDocs.push({
                        tenantId,
                        campaignId,
                        wamid,
                        to,
                        status: 'sent',
                        sentAt,
                        phaseNumber: null,
                        retryHistory: []
                    });

                    // Queue StatusMapping for bulk write
                    statusMappingOps.push({
                        updateOne: {
                            filter: { wamid },
                            update: { $set: { wamid, tenantId, campaignId, to } },
                            upsert: true
                        }
                    });

                    successCount++;
                    sentCount++;

                    console.log(JSON.stringify({
                        service: 'CampaignExecutor',
                        event: 'message_sent',
                        campaignId,
                        to,
                        wamid,
                        timestamp: new Date().toISOString()
                    }));
                } catch (sendErr) {
                    failureCount++;
                    sentCount++;

                    console.error(JSON.stringify({
                        service: 'CampaignExecutor',
                        event: 'message_send_error',
                        campaignId,
                        to,
                        error: sendErr.message,
                        responseData: sendErr.response?.data || null,
                        timestamp: new Date().toISOString()
                    }));

                    // Queue failed Recipient document for bulk insert
                    recipientDocs.push({
                        tenantId,
                        campaignId,
                        wamid: `failed_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
                        to,
                        status: 'failed',
                        failedAt: new Date().toISOString(),
                        phaseNumber: null,
                        retryHistory: []
                    });
                }
            }));

            // Execute batched DB writes for maximum throughput
            const dbPromises = [];
            if (recipientDocs.length > 0) {
                dbPromises.push(this.Recipient.insertMany(recipientDocs, { ordered: false }).catch(err => console.error('[CampaignExecutor] Bulk Recipient insert error:', err.message)));
            }
            if (statusMappingOps.length > 0) {
                dbPromises.push(this.StatusMapping.bulkWrite(statusMappingOps, { ordered: false }).catch(err => console.error('[CampaignExecutor] Bulk StatusMapping write error:', err.message)));
            }
            await Promise.all(dbPromises);

            // Emit progress after each batch
            this.socketEmitter.emitCampaignProgress(tenantId, campaignId, {
                sentCount,
                totalCount,
                successCount,
                failureCount
            });
        }

        // Update Campaign document with final counts
        try {
            await this.Campaign.updateOne(
                { id: campaignId },
                {
                    $set: {
                        successCount,
                        failureCount,
                        totalCount
                    }
                }
            );
        } catch (dbErr) {
            console.error(JSON.stringify({
                service: 'CampaignExecutor',
                event: 'campaign_update_error',
                campaignId,
                error: dbErr.message,
                timestamp: new Date().toISOString()
            }));
        }

        console.log(JSON.stringify({
            service: 'CampaignExecutor',
            event: 'campaign_execution_complete',
            campaignId,
            tenantId,
            totalCount,
            successCount,
            failureCount,
            timestamp: new Date().toISOString()
        }));

        return { campaignId, successCount, failureCount };
    }

    /**
     * Build the WhatsApp Cloud API message payload for a template send.
     *
     * Supports optional header media (image/video/document) and template
     * variable components.
     *
     * @param {string}        to          - Recipient phone number (E.164 without '+')
     * @param {string}        template    - Template name
     * @param {string}        language    - Language code
     * @param {Array|Object}  [variables] - Template variables (positional array or object)
     * @param {string}        [mediaId]   - WhatsApp media ID for header
     * @param {string}        [mediaType] - 'image' | 'video' | 'document'
     * @returns {Object} WhatsApp API payload
     * @private
     */
    _buildPayload(to, template, language, variables, mediaId, mediaType, headerVariables) {
        const components = [];

        // Header component — media takes priority over text variables
        if (mediaId && mediaType) {
            const supportedTypes = ['image', 'video', 'document'];
            const type = supportedTypes.includes(mediaType) ? mediaType : 'image';
            components.push({
                type: 'header',
                parameters: [
                    { type, [type]: { id: mediaId } }
                ]
            });
        } else if (headerVariables && typeof headerVariables === 'object') {
            // TEXT header with {{n}} variables
            const headerVars = Array.isArray(headerVariables)
                ? headerVariables
                : Object.entries(headerVariables)
                    .sort(([a], [b]) => Number(a) - Number(b))
                    .map(([, v]) => v);
            if (headerVars.length > 0) {
                components.push({
                    type: 'header',
                    parameters: headerVars.map(v => ({ type: 'text', text: String(v) }))
                });
            }
        }

        // Body component with text variables (if provided)
        if (variables) {
            const vars = Array.isArray(variables) ? variables : Object.values(variables);
            if (vars.length > 0) {
                components.push({
                    type: 'body',
                    parameters: vars.map(v => ({ type: 'text', text: String(v) }))
                });
            }
        }

        const payload = {
            messaging_product: 'whatsapp',
            to,
            type: 'template',
            template: {
                name: template,
                language: { code: language || 'en_US' }
            }
        };

        if (components.length > 0) {
            payload.template.components = components;
        }

        return payload;
    }

    /**
     * Mark a campaign as 'error' status to prevent further automatic retries.
     *
     * @param {string} campaignId    - Campaign identifier
     * @param {string} errorMessage  - Error description
     * @returns {Promise<void>}
     * @private
     */
    async _markCampaignError(campaignId, errorMessage) {
        try {
            await this.Campaign.updateOne(
                { id: campaignId },
                { $set: { status: 'error' } }
            );
            console.error(
                `[CampaignExecutor] Campaign ${campaignId} marked as error: ${errorMessage}`
            );
        } catch (err) {
            console.error(
                `[CampaignExecutor] Failed to mark campaign ${campaignId} as error: ${err.message}`
            );
        }
    }
}

module.exports = CampaignExecutor;
