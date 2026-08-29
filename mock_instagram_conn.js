const mongoose = require('mongoose');

// MONGODB_URI
const mongoURI = "mongodb://localhost:27017/sendzyy";

// Define schema matching the server's tenantSchema
const tenantSchema = new mongoose.Schema({
    email: String,
    instagramConfig: {
        instagramAccountId: { type: String, default: '' },
        username: { type: String, default: '' },
        name: { type: String, default: '' },
        accessToken: { type: String, default: '' },
        tokenExpiry: { type: Date, default: null },
        connected: { type: Boolean, default: false }
    }
});

const Tenant = mongoose.model('Tenant', tenantSchema);

async function run() {
    try {
        await mongoose.connect(mongoURI);
        console.log("✅ Connected to local MongoDB successfully.");

        // Find all tenants
        const tenants = await Tenant.find();
        if (tenants.length === 0) {
            console.log("❌ No tenant found in the database. Please create/register a tenant first.");
            return;
        }

        console.log(`Found ${tenants.length} tenant(s). Updating all to connected status for testing...`);
        
        for (const tenant of tenants) {
            tenant.instagramConfig = {
                instagramAccountId: "mock_insta_acc_id_12345",
                username: "sendzyy_test_user",
                name: "Sendzyy Mock Test Account",
                accessToken: "mock_long_lived_access_token_xyz987abc123_this_is_a_mock_token_for_local_testing",
                tokenExpiry: new Date(Date.now() + 60 * 24 * 60 * 60 * 1000), // 60 days from now
                connected: true
            };

            await tenant.save();
            console.log(`✅ Successfully mocked Instagram connection for tenant: ${tenant.email}`);
        }

    } catch (err) {
        console.error("❌ Error updating tenant:", err);
    } finally {
        await mongoose.disconnect();
        console.log("Disconnected from MongoDB.");
    }
}

run();
