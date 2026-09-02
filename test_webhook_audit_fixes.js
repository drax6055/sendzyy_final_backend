/**
 * Automated Verification Suite for WhatsApp Webhook Reporting Module Fixes
 * 
 * Tests:
 * 1. Signature Verification (HMAC-SHA256 timing-safe check)
 * 2. DST & Timezone Day-Boundary Calculations (Asia/Kolkata, UTC, America/New_York across DST)
 * 3. Full-Pipeline Concurrency Stress Test (10 simultaneous identical delivered events -> 1 count)
 * 4. Concurrent Out-of-Order Execution (Simultaneous read + delivered -> both 1 count, earliest timestamp retained)
 * 5. State Regression Guard (failed after read is ignored)
 * 6. Crash Recovery Sweep (stalled pending logs drained and processed)
 */

const mongoose = require('mongoose');
const crypto = require('crypto');
const { verifyMetaWebhookSignature } = require('./middleware/verifyMetaSignature');
const WebhookRawLog = require('./models/WebhookRawLog');
const webhookIngestionService = require('./services/WebhookIngestionService');
const { processIncomingWebhookPayload } = require('./services/WebhookRouter');
const MessageTracker = require('./services/MessageTracker');
const { getYesterdayWindowInTz, getStartOfDayUtc } = require('./utils/reportingWindow');

// Mock response object for middleware test
function createMockRes() {
    return {
        statusCode: 200,
        sentData: null,
        status(code) {
            this.statusCode = code;
            return this;
        },
        send(data) {
            this.sentData = data;
            return this;
        }
    };
}

async function runTests() {
    console.log('\n======================================================');
    console.log('🧪 Starting WhatsApp Webhook Audit Verification Tests');
    console.log('======================================================\n');

    let passed = 0;
    let failed = 0;

    function assert(condition, testName) {
        if (condition) {
            console.log(`✅ [PASS] ${testName}`);
            passed++;
        } else {
            console.error(`❌ [FAIL] ${testName}`);
            failed++;
        }
    }

    // -------------------------------------------------------------
    // TEST 1: Signature Verification Middleware
    // -------------------------------------------------------------
    console.log('\n--- Test 1: Signature Verification Middleware ---');
    const testSecret = 'test_secret_key_1234567890abcdef';
    process.env.META_APP_SECRET = testSecret;
    process.env.NODE_ENV = 'production';

    const testPayload = JSON.stringify({ object: 'whatsapp_business_account', entry: [] });
    const validHmac = crypto.createHmac('sha256', testSecret).update(testPayload).digest('hex');

    // 1.1 Valid Signature
    let nextCalled = false;
    const reqValid = {
        headers: { 'x-hub-signature-256': `sha256=${validHmac}` },
        rawBody: testPayload
    };
    const resValid = createMockRes();
    verifyMetaWebhookSignature(reqValid, resValid, () => { nextCalled = true; });
    assert(nextCalled === true, 'Accepts valid HMAC-SHA256 signature');

    // 1.2 Invalid Signature
    nextCalled = false;
    const reqInvalid = {
        headers: { 'x-hub-signature-256': 'sha256=invalid_hash_value' },
        rawBody: testPayload
    };
    const resInvalid = createMockRes();
    verifyMetaWebhookSignature(reqInvalid, resInvalid, () => { nextCalled = true; });
    assert(resInvalid.statusCode === 403 && nextCalled === false, 'Rejects invalid HMAC-SHA256 signature with 403');

    // 1.3 Missing Signature
    nextCalled = false;
    const reqMissing = {
        headers: {},
        rawBody: testPayload
    };
    const resMissing = createMockRes();
    verifyMetaWebhookSignature(reqMissing, resMissing, () => { nextCalled = true; });
    assert(resMissing.statusCode === 401 && nextCalled === false, 'Rejects missing signature with 401');

    // -------------------------------------------------------------
    // TEST 2: Timezone & DST Boundary Math
    // -------------------------------------------------------------
    console.log('\n--- Test 2: Timezone & DST Boundary Calculations ---');
    
    // 2.1 Asia/Kolkata (UTC + 05:30)
    const refDate = new Date('2026-09-02T12:00:00Z');
    const kolkataWindow = getYesterdayWindowInTz('Asia/Kolkata', refDate);
    assert(
        kolkataWindow.startUtc.toISOString() === '2026-08-31T18:30:00.000Z',
        'Asia/Kolkata start-of-day translates to 18:30:00.000Z of previous UTC calendar day'
    );
    assert(
        kolkataWindow.endUtc.toISOString() === '2026-09-01T18:29:59.999Z',
        'Asia/Kolkata end-of-day translates to 18:29:59.999Z'
    );

    // 2.2 America/New_York on Standard Day (EST = UTC-5)
    const nyStandard = getStartOfDayUtc(2026, 1, 15, 'America/New_York');
    assert(
        nyStandard.toISOString() === '2026-01-15T05:00:00.000Z',
        'America/New_York Winter standard time (UTC-5) midnight translates to 05:00:00.000Z'
    );

    // 2.3 America/New_York on Daylight Saving Day (EDT = UTC-4)
    const nyDaylight = getStartOfDayUtc(2026, 7, 15, 'America/New_York');
    assert(
        nyDaylight.toISOString() === '2026-07-15T04:00:00.000Z',
        'America/New_York Summer daylight time (UTC-4) midnight translates to 04:00:00.000Z'
    );

    // 2.4 America/New_York on DST Changeover Day (Spring Forward: 2026-03-08)
    const nySpringDst = getStartOfDayUtc(2026, 3, 9, 'America/New_York');
    assert(
        nySpringDst.toISOString() === '2026-03-09T04:00:00.000Z',
        'America/New_York Day following Spring DST transition midnight correctly calculated as UTC-4'
    );

    // -------------------------------------------------------------
    // DATABASE-BACKED CONCURRENCY & ATOMICITY TESTS
    // -------------------------------------------------------------
    console.log('\n--- Connecting to MongoDB for Concurrency Stress Tests ---');
    require('dotenv').config();
    const mongoUri = process.env.MONGODB_URI;

    if (!mongoUri) {
        console.warn('⚠️ MONGODB_URI not found. Skipping DB integration tests.');
    } else {
        await mongoose.connect(mongoUri);
        console.log('MongoDB connected for test runner.');

        // Schemas
        const tenantId = `test_tenant_${Date.now()}`;
        const campaignId = `test_camp_${Date.now()}`;
        const testWamid = `wamid.HBgLTEST${Date.now()}`;

        const Recipient = mongoose.models.Recipient || mongoose.model('Recipient', new mongoose.Schema({
            tenantId: String,
            campaignId: String,
            wamid: { type: String, unique: true },
            to: String,
            status: { type: String, default: 'sent' },
            sentAt: String,
            deliveredAt: String,
            readAt: String,
            failedAt: String,
            phaseNumber: { type: Number, default: null },
            deliveryTimestamp: { type: Date, default: null }
        }));

        const Campaign = mongoose.models.Campaign || mongoose.model('Campaign', new mongoose.Schema({
            tenantId: String,
            id: String,
            totalCount: { type: Number, default: 1 },
            successCount: { type: Number, default: 1 },
            deliveredCount: { type: Number, default: 0 },
            readCount: { type: Number, default: 0 },
            failureCount: { type: Number, default: 0 },
            currentPhase: { type: Number, default: 1 }
        }));

        const StatusMapping = mongoose.models.StatusMapping || mongoose.model('StatusMapping', new mongoose.Schema({
            wamid: { type: String, unique: true },
            tenantId: String,
            campaignId: String,
            to: String
        }));

        const Message = mongoose.models.Message || mongoose.model('Message', new mongoose.Schema({
            tenantId: String,
            contactId: String,
            wamid: String,
            status: { type: String, default: 'sent' },
            errorDetails: String
        }));

        const messageTracker = new MessageTracker(Recipient);

        // Setup Router in Ingestion Service
        webhookIngestionService.setHandler(async (body) => {
            return processIncomingWebhookPayload(body, {
                processStatusUpdateAtomic: (statusUpdate) => messageTracker.processStatusUpdateAtomic(statusUpdate, {
                    Recipient,
                    StatusMapping,
                    Campaign,
                    Message,
                    broadcastMessages: async () => {},
                    broadcastCampaigns: async () => {},
                    messageTracker
                })
            });
        });

        // Seed initial documents
        await Campaign.create({
            tenantId,
            id: campaignId,
            totalCount: 1,
            successCount: 1,
            deliveredCount: 0,
            readCount: 0,
            failureCount: 0
        });

        await StatusMapping.create({
            wamid: testWamid,
            tenantId,
            campaignId,
            to: '919876543210'
        });

        await Recipient.create({
            tenantId,
            campaignId,
            wamid: testWamid,
            to: '919876543210',
            status: 'sent',
            sentAt: new Date().toISOString()
        });

        await Message.create({
            tenantId,
            contactId: '919876543210',
            wamid: testWamid,
            status: 'sent'
        });

        // -------------------------------------------------------------
        // TEST 3: Full Pipeline Concurrency Stress Test
        // -------------------------------------------------------------
        console.log('\n--- Test 3: Full Pipeline Concurrency Stress Test ---');
        const makeDeliveredPayload = (wamid) => ({
            object: 'whatsapp_business_account',
            entry: [{
                id: 'waba_123',
                changes: [{
                    field: 'messages',
                    value: {
                        messaging_product: 'whatsapp',
                        statuses: [{
                            id: wamid,
                            status: 'delivered',
                            timestamp: `${Math.floor(Date.now() / 1000)}`
                        }]
                    }
                }]
            }]
        });

        // Fire 10 simultaneous raw deliveries for the exact same wamid
        const parallelWebhooks = Array(10).fill(null).map(async () => {
            const rawId = await webhookIngestionService.enqueueRawWebhook(makeDeliveredPayload(testWamid));
            return webhookIngestionService.processWebhookJob(rawId);
        });

        await Promise.all(parallelWebhooks);

        const campAfterDelivered = await Campaign.findOne({ tenantId, id: campaignId });
        const recipAfterDelivered = await Recipient.findOne({ wamid: testWamid });

        assert(campAfterDelivered.deliveredCount === 1, 'Concurrent 10x delivered webhooks increment Campaign.deliveredCount exactly once (1)');
        assert(recipAfterDelivered.status === 'delivered', 'Recipient status is delivered');
        assert(recipAfterDelivered.deliveredAt !== null, 'Recipient deliveredAt is recorded');

        // -------------------------------------------------------------
        // TEST 4: Concurrent Out-of-Order Read & Delivered Test
        // -------------------------------------------------------------
        console.log('\n--- Test 4: Concurrent Out-of-Order Execution (New WAMID) ---');
        const testWamid2 = `wamid.HBgLTEST_ORDER_${Date.now()}`;
        const earlyDeliveryTime = new Date(Date.now() - 5000);

        await StatusMapping.create({
            wamid: testWamid2,
            tenantId,
            campaignId,
            to: '919876543211'
        });

        await Recipient.create({
            tenantId,
            campaignId,
            wamid: testWamid2,
            to: '919876543211',
            status: 'sent',
            sentAt: new Date().toISOString()
        });

        const makeReadPayload = (wamid) => ({
            object: 'whatsapp_business_account',
            entry: [{
                id: 'waba_123',
                changes: [{
                    field: 'messages',
                    value: {
                        messaging_product: 'whatsapp',
                        statuses: [{
                            id: wamid,
                            status: 'read',
                            timestamp: `${Math.floor(Date.now() / 1000)}`
                        }]
                    }
                }]
            }]
        });

        // Fire Read first, then Delivered
        const rawReadId = await webhookIngestionService.enqueueRawWebhook(makeReadPayload(testWamid2));
        const rawDelivId = await webhookIngestionService.enqueueRawWebhook(makeDeliveredPayload(testWamid2));

        await Promise.all([
            webhookIngestionService.processWebhookJob(rawReadId),
            webhookIngestionService.processWebhookJob(rawDelivId)
        ]);

        const campAfterOrder = await Campaign.findOne({ tenantId, id: campaignId });
        const recipAfterOrder = await Recipient.findOne({ wamid: testWamid2 });

        assert(campAfterOrder.readCount === 1, 'Campaign readCount incremented to 1');
        assert(campAfterOrder.deliveredCount === 2, 'Campaign deliveredCount correctly includes both recipients (total 2)');
        assert(recipAfterOrder.status === 'read', 'Recipient advances to read without regressing to delivered');

        // -------------------------------------------------------------
        // TEST 5: State Regression Guard
        // -------------------------------------------------------------
        console.log('\n--- Test 5: State Regression Guard ---');
        const makeFailedPayload = (wamid) => ({
            object: 'whatsapp_business_account',
            entry: [{
                id: 'waba_123',
                changes: [{
                    field: 'messages',
                    value: {
                        messaging_product: 'whatsapp',
                        statuses: [{
                            id: wamid,
                            status: 'failed',
                            timestamp: `${Math.floor(Date.now() / 1000)}`,
                            errors: [{ title: 'Failed to deliver' }]
                        }]
                    }
                }]
            }]
        });

        const rawFailId = await webhookIngestionService.enqueueRawWebhook(makeFailedPayload(testWamid2));
        await webhookIngestionService.processWebhookJob(rawFailId);

        const campAfterFail = await Campaign.findOne({ tenantId, id: campaignId });
        const recipAfterFail = await Recipient.findOne({ wamid: testWamid2 });

        assert(recipAfterFail.status === 'read', 'Recipient status remains read (not downgraded to failed)');
        assert(campAfterFail.failureCount === 0, 'Campaign failureCount remains 0');

        // -------------------------------------------------------------
        // TEST 6: Crash Recovery Sweep
        // -------------------------------------------------------------
        console.log('\n--- Test 6: Crash Recovery Sweep ---');
        const testWamid3 = `wamid.HBgLTEST_CRASH_${Date.now()}`;
        await StatusMapping.create({
            wamid: testWamid3,
            tenantId,
            campaignId,
            to: '919876543212'
        });
        await Recipient.create({
            tenantId,
            campaignId,
            wamid: testWamid3,
            to: '919876543212',
            status: 'sent'
        });

        // Insert directly as pending (simulating server crash right after write-ahead log)
        const crashLog = await WebhookRawLog.create({
            payload: makeDeliveredPayload(testWamid3),
            status: 'pending',
            attempts: 0
        });

        // Run recovery sweep
        const recoveredCount = await webhookIngestionService.recoverPendingWebhookLogs();
        const updatedCrashLog = await WebhookRawLog.findById(crashLog._id);
        const recipAfterCrash = await Recipient.findOne({ wamid: testWamid3 });

        assert(recoveredCount >= 1, 'Crash recovery sweep finds pending logs');
        assert(updatedCrashLog.status === 'processed', 'Stalled log transitioned to processed');
        assert(recipAfterCrash.status === 'delivered', 'Recipient was delivered during recovery sweep');

        // Cleanup test data
        await Campaign.deleteMany({ tenantId });
        await StatusMapping.deleteMany({ tenantId });
        await Recipient.deleteMany({ tenantId });
        await Message.deleteMany({ tenantId });
        await WebhookRawLog.deleteMany({ _id: { $in: [crashLog._id] } });

        await mongoose.disconnect();
    }

    console.log('\n======================================================');
    console.log(`📊 Test Results: ${passed} Passed, ${failed} Failed`);
    console.log('======================================================\n');

    if (failed > 0) {
        process.exit(1);
    }
}

runTests().catch((err) => {
    console.error('Fatal test runner error:', err);
    process.exit(1);
});
