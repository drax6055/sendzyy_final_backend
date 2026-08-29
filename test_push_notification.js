#!/usr/bin/env node

/**
 * Sendzyy Background Push Notification Test Script
 *
 * Usage:
 *   1. Send to a specific FCM device token:
 *      node test_push_notification.js --token <FCM_DEVICE_TOKEN>
 *
 *   2. Send to a tenant topic (all devices registered under tenant):
 *      node test_push_notification.js --tenant <TENANT_ID>
 *
 *   3. Custom title/body:
 *      node test_push_notification.js --tenant <TENANT_ID> --title "New Lead" --body "Maulik sent a WhatsApp message"
 */

const FCMService = require('./services/FCMService');

const args = process.argv.slice(2);
const params = {};

for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith('--')) {
        const key = args[i].substring(2);
        const value = args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : true;
        params[key] = value;
    }
}

async function main() {
    console.log('========================================================');
    console.log('🚀 Sendzyy Firebase Background Push Notification Tester');
    console.log('========================================================\n');

    const title = params.title || '🔔 Sendzyy Background Test Notification';
    const body = params.body || 'This notification was received while the app is in the background or closed!';
    const tenantId = params.tenant;
    const token = params.token;

    if (!tenantId && !token) {
        console.log('❌ Error: Please provide either --tenant <tenantId> or --token <deviceToken>\n');
        console.log('Examples:');
        console.log('  node test_push_notification.js --tenant 67b6f721e8d1a10012345678');
        console.log('  node test_push_notification.js --token dXz...YourFcmDeviceToken...');
        console.log('  node test_push_notification.js --tenant 67b6f721e8d1a10012345678 --title "Alert" --body "Campaign Finished"');
        process.exit(1);
    }

    const payloadData = {
        type: 'test_notification',
        category: 'system',
        timestamp: new Date().toISOString(),
        click_action: 'FLUTTER_NOTIFICATION_CLICK'
    };

    console.log(`📌 Title:   "${title}"`);
    console.log(`📌 Body:    "${body}"`);
    console.log(`📌 Data:    ${JSON.stringify(payloadData)}\n`);

    if (token) {
        console.log(`📤 Sending direct push notification to token: ${token.substring(0, 20)}...`);
        const result = await FCMService.sendToDevice(token, {
            title,
            body,
            data: payloadData
        });

        if (result.success) {
            console.log('\n✅ Push notification successfully sent!');
            console.log(`   Message ID: ${result.messageId}`);
            console.log('   Check your phone / browser notification tray now.');
        } else {
            console.log('\n❌ Push notification failed.');
            console.log(`   Error: ${result.error || result.reason}`);
            if (result.invalidToken) {
                console.log('   ⚠️ The provided token is invalid or expired.');
            }
        }
    } else if (tenantId) {
        console.log(`📤 Sending broadcast push notification to topic: "tenant_${tenantId}"...`);
        const result = await FCMService.sendToTenant(tenantId, {
            title,
            body,
            data: payloadData
        });

        if (result.success) {
            console.log('\n✅ Topic push notification successfully broadcasted!');
            console.log(`   Message ID: ${result.messageId}`);
            console.log(`   All devices subscribed to topic "tenant_${tenantId}" will receive this notification in background/closed state.`);
        } else {
            console.log('\n❌ Broadcast push notification failed.');
            console.log(`   Error: ${result.error || result.reason}`);
        }
    }
    console.log('\n========================================================\n');
}

main().catch(err => {
    console.error('Fatal error running test:', err);
    process.exit(1);
});
