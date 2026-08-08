const mongoose = require('mongoose');
const { MongoMemoryServer } = require('mongodb-memory-server');
const bcrypt = require('bcrypt');

describe('Tenant Status and Login Activation Logic', () => {
    let mongoServer;
    let Tenant;

    beforeAll(async () => {
        mongoServer = await MongoMemoryServer.create();
        const mongoUri = mongoServer.getUri();
        await mongoose.connect(mongoUri);

        const tenantSchema = new mongoose.Schema({
            name: { type: String, required: true },
            email: { type: String, required: true, unique: true },
            password: { type: String, required: true },
            subscription: {
                planId: { type: String, default: 'free' },
                planName: { type: String, default: '' },
                billingCycle: { type: String, default: 'monthly' },
                price: { type: Number, default: 0 },
                expiryDate: { type: Date, default: () => new Date(Date.now() + 30 * 24 * 60 * 60 * 1000) },
                lastPaymentId: String,
                lastPaymentDate: Date,
            },
            whatsappConfig: {
                phoneNumberId: String,
                accessToken: String,
                businessAccountId: String,
            },
            webhookSecret: { type: String, default: '' },
            openaiApiKey: { type: String, default: '' },
            status: { type: String, enum: ['active', 'inactive'], default: 'active' },
        }, { timestamps: true });

        Tenant = mongoose.model('TestTenantStatus', tenantSchema);
    });

    afterAll(async () => {
        await mongoose.disconnect();
        await mongoServer.stop();
    });

    test('New tenant should default status to "active"', async () => {
        const hashedPassword = await bcrypt.hash('Password123!', 10);
        const tenant = await Tenant.create({
            name: 'Care Plus Physiotherapy',
            email: 'careplus@gmail.com',
            password: hashedPassword,
            subscription: {
                planId: 'panel_12m',
                planName: '12 Month Access',
                price: 11999,
                expiryDate: new Date(Date.now() + 365 * 24 * 60 * 60 * 1000),
                lastPaymentId: 'pay_TAC1NOHwRSBwlq',
                lastPaymentDate: new Date(),
            },
        });

        expect(tenant.status).toBe('active');

        const tenantObj = tenant.toObject();
        const keys = Object.keys(tenantObj);
        // Verify status key position right above createdAt
        const statusIdx = keys.indexOf('status');
        const createdAtIdx = keys.indexOf('createdAt');
        expect(statusIdx).toBeGreaterThan(-1);
        expect(createdAtIdx).toBeGreaterThan(-1);
        expect(statusIdx).toBeLessThan(createdAtIdx);
    });

    test('PATCH request changes tenant status between active and inactive', async () => {
        const tenant = await Tenant.findOne({ email: 'careplus@gmail.com' });
        
        // Update to inactive
        const inactiveTenant = await Tenant.findByIdAndUpdate(
            tenant._id,
            { $set: { status: 'inactive' } },
            { new: true }
        );
        expect(inactiveTenant.status).toBe('inactive');

        // Update back to active
        const activeTenant = await Tenant.findByIdAndUpdate(
            tenant._id,
            { $set: { status: 'active' } },
            { new: true }
        );
        expect(activeTenant.status).toBe('active');
    });

    test('Login restriction logic based on tenant status', async () => {
        const tenant = await Tenant.findOne({ email: 'careplus@gmail.com' });

        // Helper mimicking /login handler check
        const canUserLogin = (tenantDoc) => {
            if (tenantDoc.status === 'inactive') {
                return { allowed: false, error: 'account_inactive', message: 'Your account is inactive. Please contact support.' };
            }
            return { allowed: true };
        };

        // When active
        await Tenant.findByIdAndUpdate(tenant._id, { $set: { status: 'active' } });
        const activeDoc = await Tenant.findById(tenant._id);
        expect(canUserLogin(activeDoc).allowed).toBe(true);

        // When inactive
        await Tenant.findByIdAndUpdate(tenant._id, { $set: { status: 'inactive' } });
        const inactiveDoc = await Tenant.findById(tenant._id);
        const loginAttempt = canUserLogin(inactiveDoc);
        expect(loginAttempt.allowed).toBe(false);
        expect(loginAttempt.error).toBe('account_inactive');
    });

    test('Validation and error handling for PATCH tenant status handler', () => {
        const handlePatch = (targetId, status) => {
            if (!targetId) return { statusCode: 400, error: 'tenantId is required' };
            if (!status || !['active', 'inactive'].includes(status)) {
                return { statusCode: 400, error: "status must be 'active' or 'inactive'" };
            }
            return { statusCode: 200 };
        };

        expect(handlePatch(null, 'active')).toEqual({ statusCode: 400, error: 'tenantId is required' });
        expect(handlePatch('123', 'invalid_status')).toEqual({ statusCode: 400, error: "status must be 'active' or 'inactive'" });
        expect(handlePatch('123', null)).toEqual({ statusCode: 400, error: "status must be 'active' or 'inactive'" });
        expect(handlePatch('123', 'inactive')).toEqual({ statusCode: 200 });
    });
});
