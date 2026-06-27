const admin = require('firebase-admin');
const serviceAccount = require("./serviceAccountKey.json");

if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount)
    });
}

const db = admin.firestore();

async function checkData() {
    console.log("🔍 Checking Firestore Data...");
    
    const tenants = await db.collection('tenants').get();
    console.log(`Found ${tenants.size} tenants.`);
    
    for (const doc of tenants.docs) {
        const tenantId = doc.id;
        const tenantData = doc.data();
        console.log(`--- Tenant: ${tenantData.name} (${tenantId}) ---`);
        
        const campaigns = await db.collection('tenants').doc(tenantId)
            .collection('data').doc('reports')
            .collection('campaigns').get();
        console.log(`  Campaigns: ${campaigns.size}`);
        
        const convs = await db.collection('tenants').doc(tenantId)
            .collection('data').doc('chats')
            .collection('conversations').get();
        console.log(`  Conversations: ${convs.size}`);
    }
    process.exit(0);
}

checkData();
