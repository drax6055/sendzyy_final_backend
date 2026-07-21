/**
 * campaign_counts.test.js
 *
 * Cross-check tests for the two bug fixes:
 *
 * Fix 1 (server.js GET /campaigns):
 *   Campaign document failureCount can be stale (e.g. only 2 API-rejection
 *   failures recorded) while the Recipient collection has many more docs
 *   with status='failed'. The endpoint must now aggregate from Recipient
 *   and override the campaign-doc counters before returning.
 *
 * Fix 2 (campaign_report_dialog.dart logic mirrored here in JS):
 *   The summary chips must count from the recipients list, NOT from the
 *   campaign document fields.
 *
 * These tests use an in-memory MongoDB instance (mongodb-memory-server) so
 * no real database connection is needed.
 */

const mongoose = require('mongoose');
const { MongoMemoryServer } = require('mongodb-memory-server');

// ─── Minimal Mongoose schemas (matching server.js) ────────────────────────────

const campaignSchema = new mongoose.Schema({
    id:            { type: String, required: true, unique: true },
    tenantId:      { type: String, required: true },
    template:      { type: String },
    timestamp:     { type: Date, default: Date.now },
    totalCount:    { type: Number, default: 0 },
    successCount:  { type: Number, default: 0 },
    failureCount:  { type: Number, default: 0 },
    deliveredCount:{ type: Number, default: 0 },
    readCount:     { type: Number, default: 0 },
    status:        { type: String, default: 'initial' },
});

const recipientSchema = new mongoose.Schema({
    tenantId:   { type: String, required: true },
    campaignId: { type: String, required: true },
    to:         { type: String },
    status:     { type: String, default: 'sent' },
    wamid:      { type: String },
    sentAt:     { type: String },
    deliveredAt:{ type: String },
    readAt:     { type: String },
    failedAt:   { type: String },
    phaseNumber:{ type: Number, default: null },
});

let Campaign;
let Recipient;
let mongod;

// ─── Helper: compute live counts (mirrors GET /campaigns logic) ───────────────

async function computeLiveCounts(tenantId, campaignIds) {
    const recipientAgg = await Recipient.aggregate([
        { $match: { tenantId, campaignId: { $in: campaignIds } } },
        {
            $group: {
                _id: { campaignId: '$campaignId', status: '$status' },
                count: { $sum: 1 }
            }
        }
    ]);

    const recipientCounts = {};
    for (const row of recipientAgg) {
        const cid = row._id.campaignId;
        const status = row._id.status;
        if (!recipientCounts[cid]) {
            recipientCounts[cid] = { sent: 0, delivered: 0, read: 0, failed: 0, total: 0 };
        }
        recipientCounts[cid].total += row.count;
        if (status === 'sent')       recipientCounts[cid].sent      += row.count;
        if (status === 'delivered')  recipientCounts[cid].delivered += row.count;
        if (status === 'read')       recipientCounts[cid].read      += row.count;
        if (status === 'failed')     recipientCounts[cid].failed    += row.count;
    }
    return recipientCounts;
}

async function enrichCampaign(campaign, recipientCounts) {
    const rc = recipientCounts[campaign.id];
    const base = campaign.toObject ? campaign.toObject() : { ...campaign };
    if (rc && rc.total > 0) {
        base.totalCount      = rc.total;
        base.successCount    = rc.total - rc.failed;
        base.failureCount    = rc.failed;
        base.deliveredCount  = rc.delivered + rc.read;
        base.readCount       = rc.read;
    }
    return base;
}

// ─── Helper: compute counts from a recipients array (mirrors dialog chip logic) ─

function computeChipCounts(recipients) {
    if (!recipients || recipients.length === 0) return null;
    const sentCount      = recipients.length;
    const deliveredCount = recipients.filter(r => r.status === 'delivered' || r.status === 'read').length;
    const readCount      = recipients.filter(r => r.status === 'read').length;
    const failedCount    = recipients.filter(r => r.status === 'failed').length;
    return { sentCount, deliveredCount, readCount, failedCount };
}

// ─── Setup / Teardown ─────────────────────────────────────────────────────────

beforeAll(async () => {
    mongod = await MongoMemoryServer.create();
    const uri = mongod.getUri();
    await mongoose.connect(uri);
    Campaign  = mongoose.model('Campaign',  campaignSchema);
    Recipient = mongoose.model('Recipient', recipientSchema);
});

afterAll(async () => {
    await mongoose.disconnect();
    await mongod.stop();
});

afterEach(async () => {
    await Campaign.deleteMany({});
    await Recipient.deleteMany({});
});

// ─── Test Suite 1: GET /campaigns live-count enrichment ──────────────────────

describe('GET /campaigns — live count enrichment from Recipient collection', () => {

    test('Fix 1a: failureCount must reflect all recipient failures, not just API-send failures', async () => {
        const tenantId  = 'tenant_001';
        const campaignId = 'camp_001';

        // Campaign doc records only 2 API-rejection failures
        await Campaign.create({
            id:           campaignId,
            tenantId,
            template:     'sendzybysharad',
            totalCount:   453,
            successCount: 451,
            failureCount: 2,          // stale: only API send failures
            deliveredCount: 219,
            readCount:    0,
            status:       'initial',
        });

        // Recipient collection has many more failures
        const recipientsToCreate = [
            { tenantId, campaignId, to: '91111', status: 'failed',    wamid: 'failed_1', phaseNumber: null },
            { tenantId, campaignId, to: '91112', status: 'failed',    wamid: 'failed_2', phaseNumber: null },
            ...Array.from({ length: 230 }, (_, i) => ({
                tenantId, campaignId, to: `919${i}`, status: 'failed', wamid: `pf_${i}`, phaseNumber: null
            })),
            ...Array.from({ length: 219 }, (_, i) => ({
                tenantId, campaignId, to: `918${i}`, status: 'delivered', wamid: `d_${i}`, phaseNumber: 1
            })),
            { tenantId, campaignId, to: '917001', status: 'sent', wamid: 'w1', phaseNumber: null },
            { tenantId, campaignId, to: '917002', status: 'sent', wamid: 'w2', phaseNumber: null },
        ];
        await Recipient.insertMany(recipientsToCreate);

        const campaigns = await Campaign.find({ tenantId });
        const campaignIds = campaigns.map(c => c.id);
        const rc = await computeLiveCounts(tenantId, campaignIds);
        const enriched = await enrichCampaign(campaigns[0], rc);

        expect(enriched.failureCount).toBe(232);
        expect(enriched.totalCount).toBe(453);
        expect(enriched.deliveredCount).toBe(219);
        expect(enriched.readCount).toBe(0);
        expect(enriched.successCount).toBe(453 - 232);
    });

    test('Fix 1b: deliveredCount must include messages that progressed to read', async () => {
        const tenantId  = 'tenant_002';
        const campaignId = 'camp_002';

        await Campaign.create({
            id: campaignId, tenantId, template: 'test',
            totalCount: 10, successCount: 10, failureCount: 0,
            deliveredCount: 4, readCount: 3, status: 'completed',
        });

        await Recipient.insertMany([
            { tenantId, campaignId, to: '1', status: 'read',      wamid: 'r1' },
            { tenantId, campaignId, to: '2', status: 'read',      wamid: 'r2' },
            { tenantId, campaignId, to: '3', status: 'read',      wamid: 'r3' },
            { tenantId, campaignId, to: '4', status: 'delivered', wamid: 'd1' },
            { tenantId, campaignId, to: '5', status: 'delivered', wamid: 'd2' },
            { tenantId, campaignId, to: '6', status: 'delivered', wamid: 'd3' },
            { tenantId, campaignId, to: '7', status: 'delivered', wamid: 'd4' },
            { tenantId, campaignId, to: '8', status: 'failed',    wamid: 'f1' },
            { tenantId, campaignId, to: '9', status: 'sent',      wamid: 's1' },
            { tenantId, campaignId, to: '10',status: 'sent',      wamid: 's2' },
        ]);

        const campaigns  = await Campaign.find({ tenantId });
        const rc         = await computeLiveCounts(tenantId, campaigns.map(c => c.id));
        const enriched   = await enrichCampaign(campaigns[0], rc);

        expect(enriched.totalCount).toBe(10);
        expect(enriched.readCount).toBe(3);
        expect(enriched.deliveredCount).toBe(7);   // delivered(4) + read(3)
        expect(enriched.failureCount).toBe(1);
        expect(enriched.successCount).toBe(9);
    });

    test('Fix 1c: campaign with no recipient docs yet must keep campaign-doc values', async () => {
        const tenantId  = 'tenant_003';
        const campaignId = 'camp_003';

        await Campaign.create({
            id: campaignId, tenantId, template: 'test',
            totalCount: 500, successCount: 0, failureCount: 0,
            deliveredCount: 0, readCount: 0, status: 'initial',
        });
        // No Recipient docs yet

        const campaigns = await Campaign.find({ tenantId });
        const rc        = await computeLiveCounts(tenantId, campaigns.map(c => c.id));
        const enriched  = await enrichCampaign(campaigns[0], rc);

        expect(enriched.totalCount).toBe(500);
        expect(enriched.successCount).toBe(0);
        expect(enriched.failureCount).toBe(0);
    });

    test('Fix 1d: multiple campaigns each get their own accurate counts', async () => {
        const tenantId = 'tenant_004';

        await Campaign.insertMany([
            { id: 'c1', tenantId, template: 'A', failureCount: 0, deliveredCount: 0, readCount: 0, totalCount: 3, successCount: 3, status: 'completed' },
            { id: 'c2', tenantId, template: 'B', failureCount: 0, deliveredCount: 0, readCount: 0, totalCount: 2, successCount: 2, status: 'completed' },
        ]);

        await Recipient.insertMany([
            { tenantId, campaignId: 'c1', to: '1', status: 'failed',    wamid: 'f1' },
            { tenantId, campaignId: 'c1', to: '2', status: 'delivered', wamid: 'd1' },
            { tenantId, campaignId: 'c1', to: '3', status: 'read',      wamid: 'r1' },
            { tenantId, campaignId: 'c2', to: '4', status: 'delivered', wamid: 'd2' },
            { tenantId, campaignId: 'c2', to: '5', status: 'delivered', wamid: 'd3' },
        ]);

        const campaigns = await Campaign.find({ tenantId });
        const rc        = await computeLiveCounts(tenantId, campaigns.map(c => c.id));

        const e1 = await enrichCampaign(campaigns.find(c => c.id === 'c1'), rc);
        const e2 = await enrichCampaign(campaigns.find(c => c.id === 'c2'), rc);

        expect(e1.failureCount).toBe(1);
        expect(e1.deliveredCount).toBe(2);   // delivered(1) + read(1)
        expect(e1.readCount).toBe(1);
        expect(e1.successCount).toBe(2);

        expect(e2.failureCount).toBe(0);
        expect(e2.deliveredCount).toBe(2);
        expect(e2.readCount).toBe(0);
        expect(e2.successCount).toBe(2);
    });
});

// ─── Test Suite 2: Campaign Report Dialog chip counts (mirrored logic) ────────

describe('Campaign Report Dialog — chip counts from recipients list', () => {

    test('Fix 2a: chips show 0 failed when all recipients are delivered', () => {
        const recipients = [
            { status: 'delivered' },
            { status: 'delivered' },
            { status: 'read' },
        ];
        const chips = computeChipCounts(recipients);
        expect(chips.sentCount).toBe(3);
        expect(chips.deliveredCount).toBe(3); // delivered(2) + read(1)
        expect(chips.readCount).toBe(1);
        expect(chips.failedCount).toBe(0);
    });

    test('Fix 2b: failed chip matches actual failed recipients (not stale campaign doc)', () => {
        // The bug: campaign doc said failureCount=2, list had many more
        const recipients = [
            { status: 'failed' },
            { status: 'failed' },
            { status: 'failed' },
            { status: 'failed' },
            { status: 'sent' },
            { status: 'delivered' },
            { status: 'read' },
        ];
        const chips = computeChipCounts(recipients);
        expect(chips.sentCount).toBe(7);
        expect(chips.failedCount).toBe(4);    // matches list, not stale campaign doc
        expect(chips.deliveredCount).toBe(2); // delivered(1) + read(1)
        expect(chips.readCount).toBe(1);
    });

    test('Fix 2c: sent chip = total recipients count', () => {
        const recipients = Array.from({ length: 451 }, (_, i) => ({
            status: i < 219 ? 'delivered' : i < 221 ? 'failed' : 'sent'
        }));
        const chips = computeChipCounts(recipients);
        expect(chips.sentCount).toBe(451);
        expect(chips.failedCount).toBe(2);
        expect(chips.deliveredCount).toBe(219);
    });

    test('Fix 2d: read recipients are included in deliveredCount', () => {
        const recipients = [
            { status: 'read' },
            { status: 'read' },
            { status: 'delivered' },
            { status: 'failed' },
        ];
        const chips = computeChipCounts(recipients);
        expect(chips.readCount).toBe(2);
        expect(chips.deliveredCount).toBe(3);  // read(2) + delivered(1)
        expect(chips.failedCount).toBe(1);
        expect(chips.sentCount).toBe(4);
    });

    test('Fix 2e: empty recipient list returns null (fallback to campaign-doc fields)', () => {
        const chips = computeChipCounts([]);
        expect(chips).toBeNull();
    });
});

// ─── Test Suite 3: Regression ─────────────────────────────────────────────────

describe('Regression: _completeCampaign stale failureCount is corrected by live aggregation', () => {

    test('Fix 3: after live enrichment, failureCount matches actual failed recipient count', async () => {
        const tenantId  = 'tenant_005';
        const campaignId = 'camp_005';

        // Simulate post-completion: campaign doc failureCount=2 (only API-send failures)
        // but Recipient has 232 failed docs (2 + 230 marked by lifecycle manager)
        await Campaign.create({
            id: campaignId, tenantId, template: 'test',
            totalCount: 453, successCount: 451,
            failureCount: 2,   // stale
            deliveredCount: 219, readCount: 0, status: 'completed',
        });

        await Recipient.insertMany(
            Array.from({ length: 232 }, (_, i) => ({
                tenantId, campaignId, to: `9${i}`,
                status: 'failed', wamid: `f_${i}`, phaseNumber: null
            }))
        );

        const failedInDB = await Recipient.countDocuments({ campaignId, status: 'failed' });
        const campaign   = await Campaign.findOne({ id: campaignId });

        // Original bug: db count > campaign doc count
        expect(failedInDB).toBeGreaterThan(campaign.failureCount);

        // After fix — live aggregation corrects the count
        const rc = await computeLiveCounts(tenantId, [campaignId]);
        const enriched = await enrichCampaign(campaign, rc);

        expect(enriched.failureCount).toBe(failedInDB);
        expect(enriched.failureCount).toBe(232);
    });
});
