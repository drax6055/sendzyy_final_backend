const admin = require('firebase-admin');
const serviceAccount = require("./serviceAccountKey.json");

if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount)
    });
}

const db = admin.firestore();

async function deepCheck() {
    console.log("🔍 Deep Firestore Scan...");
    
    // 1. Check all campaigns in any tenant
    const tenants = await db.collection('tenants').get();
    for (const tenantDoc of tenants.docs) {
        console.log(`Tenant: ${tenantDoc.id} (${tenantDoc.data().name})`);
        
        // Check subcollections
        const subs = await tenantDoc.ref.listCollections();
        for (const sub of subs) {
            console.log(`  Subcollection: ${sub.id}`);
            if (sub.id === 'data') {
                const dataDocs = await sub.get();
                for (const dDoc of dataDocs.docs) {
                    console.log(`    Data Doc: ${dDoc.id}`);
                    const dataSubs = await dDoc.ref.listCollections();
                    for (const ds of dataSubs) {
                        const items = await ds.get();
                        console.log(`      Data Subcoll: ${ds.id} (Items: ${items.size})`);
                        if (ds.id === 'campaigns' && items.size > 0) {
                            items.docs.forEach(i => console.log(`        - Campaign: ${i.id} (${i.data().template})`));
                        }
                    }
                }
            }
        }
    }

    // 2. Check global collections
    console.log("Global Collections:");
    const globals = await db.listCollections();
    for (const g of globals) {
        const items = await g.limit(5).get();
        console.log(`  Coll: ${g.id} (Sample: ${items.size} docs)`);
    }

    process.exit(0);
}

deepCheck();
