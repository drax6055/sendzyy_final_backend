const express = require('express');
const bodyParser = require('body-parser');
const admin = require('firebase-admin');

// 1. Initialize Firebase Admin
// You will need to download your serviceAccountKey.json from Firebase Console
// Project Settings > Service Accounts > Generate new private key
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();
const app = express();
app.use(bodyParser.json());

const PORT = 3000;
const VERIFY_TOKEN = "whatsapp_bulk_verify_token_123";

// 2. Webhook Verification (for Meta)
app.get('/webhook', (req, res) => {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];

    if (mode && token === VERIFY_TOKEN) {
        console.log("Webhook Verified!");
        res.status(200).send(challenge);
    } else {
        res.sendStatus(403);
    }
});

// 3. Receive Messages
app.post('/webhook', async (req, res) => {
    const body = req.body;

    if (body.object === 'whatsapp_business_account') {
        try {
            const entry = body.entry[0];
            const changes = entry.changes[0];
            const value = changes.value;

            if (value.messages) {
                const message = value.messages[0];
                const contact = value.contacts[0];

                const from = message.from;
                const text = message.text ? message.text.body : "Sent a media file";
                const timestamp = admin.firestore.FieldValue.serverTimestamp();

                console.log(`Received message from ${from}: ${text}`);

                // Save to Firestore
                const convRef = db.collection("conversations").doc(from);

                await convRef.set({
                    name: contact.profile.name || from,
                    lastMessage: text,
                    lastActive: timestamp,
                    id: from
                }, {
                    merge: true
                });

                await convRef.collection("messages").add({
                    text: text,
                    isMe: false,
                    time: new Date().toLocaleTimeString([], {
                        hour: '2-digit',
                        minute: '2-digit'
                    }),
                    timestamp: timestamp
                });
            }

            res.sendStatus(200);
        } catch (error) {
            console.error("Error processing webhook:", error);
            res.sendStatus(500);
        }
    } else {
        res.sendStatus(404);
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Webhook server is listening on port ${PORT}`);
    console.log(`🔗 Use ngrok to expose this port: ngrok http ${PORT}`);
});