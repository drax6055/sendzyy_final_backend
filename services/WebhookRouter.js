/**
 * WebhookRouter Service
 * Unpacks Meta WhatsApp Cloud API webhooks with full batch traversal.
 * Handles:
 *   - Account Updates (PARTNER_ADDED)
 *   - Template Status Updates (APPROVED / REJECTED)
 *   - Phone Number Name Updates
 *   - Status Updates (sent / delivered / read / failed) via MessageTracker.processStatusUpdateAtomic
 *   - Incoming Messages via Chatbot / Live-Chat handlers
 */

/**
 * Route and process an incoming raw webhook payload.
 * 
 * @param {Object} body - Parsed JSON body from Meta
 * @param {Object} handlers - Service handlers and model dependencies
 * @param {Function} handlers.processStatusUpdateAtomic - Atomic status processor
 * @param {Function} handlers.handlePartnerAdded - Handler for PARTNER_ADDED account update
 * @param {Function} handlers.handleTemplateStatusUpdate - Handler for template status changes
 * @param {Function} handlers.handlePhoneNumberNameUpdate - Handler for phone number verified name approval
 * @param {Function} handlers.processIncomingMessage - Handler for user inbound messages
 * @param {Object} handlers.context - Database models and broadcasters
 */
async function processIncomingWebhookPayload(body, handlers = {}) {
    if (!body || body.object !== 'whatsapp_business_account') {
        return { success: false, reason: 'ignored_non_waba_object' };
    }

    const {
        processStatusUpdateAtomic,
        handlePartnerAdded,
        handleTemplateStatusUpdate,
        handlePhoneNumberNameUpdate,
        processIncomingMessage,
        context = {}
    } = handlers;

    const results = {
        statusesProcessed: 0,
        messagesProcessed: 0,
        eventsProcessed: 0
    };

    for (const entry of body.entry || []) {
        const entryId = entry.id; // WABA ID

        for (const change of entry.changes || []) {
            const val = change.value;
            if (!val) continue;

            // 1. Account Updates (PARTNER_ADDED)
            if (change.field === 'account_update' && val.event === 'PARTNER_ADDED') {
                if (typeof handlePartnerAdded === 'function') {
                    await handlePartnerAdded(entryId, val, context);
                    results.eventsProcessed++;
                }
                continue;
            }

            // 2. Message Template Status Updates
            if (change.field === 'message_template_status_update') {
                if (typeof handleTemplateStatusUpdate === 'function') {
                    await handleTemplateStatusUpdate(entryId, val, context);
                    results.eventsProcessed++;
                }
                continue;
            }

            // 3. Phone Number Name Approval Updates
            if (change.field === 'phone_number_name_update') {
                if (typeof handlePhoneNumberNameUpdate === 'function') {
                    await handlePhoneNumberNameUpdate(val, context);
                    results.eventsProcessed++;
                }
                continue;
            }

            // 4. Batch Status Updates (Iterates all items - fixing batch truncation bug)
            if (Array.isArray(val.statuses)) {
                for (const statusUpdate of val.statuses) {
                    if (typeof processStatusUpdateAtomic === 'function') {
                        await processStatusUpdateAtomic(statusUpdate, context);
                        results.statusesProcessed++;
                    }
                }
            }

            // 5. Batch Incoming Messages (Iterates all items)
            if (Array.isArray(val.messages)) {
                const receiverPhoneNumberId = val.metadata?.phone_number_id;
                for (const message of val.messages) {
                    if (typeof processIncomingMessage === 'function') {
                        await processIncomingMessage(message, val.contacts, receiverPhoneNumberId, context);
                        results.messagesProcessed++;
                    }
                }
            }
        }
    }

    return { success: true, ...results };
}

module.exports = {
    processIncomingWebhookPayload
};
