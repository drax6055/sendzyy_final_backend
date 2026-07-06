const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
setInterval(() => { }, 100000);
const bodyParser = require('body-parser');
const cors = require('cors');
const axios = require('axios');
const nodemailer = require('nodemailer');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const Razorpay = require('razorpay');
const crypto = require('crypto');
const fs = require('fs');
const mongoose = require('mongoose');
require('dotenv').config();
const { generateAppSecretProof } = require('./cryptoUtils');

//  MongoDB Connection 
if (process.env.MONGODB_URI) {
    mongoose.connect(process.env.MONGODB_URI)
        .then(async () => {
            console.log(' MongoDB connected');
            // Drop the old unique compound index on {tenantId, version} if it exists.
            // We now use a single-field unique index on {tenantId} since one doc per tenant.
            try {
                const col = mongoose.connection.collection('retryconfigurations');
                await col.dropIndex('tenantId_1_version_1');
                console.log(' Dropped old retryconfigurations index tenantId_1_version_1');
            } catch (_) {
                // Index may not exist — that's fine
            }
        })
        .catch(err => console.error(' MongoDB connection error:', err));
} else {
    console.warn(' MONGODB_URI missing in .env');
}

//  Schemas 
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
        phoneNumberId: String,         // WABA phone number ID (Step 6)
        accessToken: String,            // Customer business token (Step 5)
        businessAccountId: String,      // WABA ID (Step 3)
        businessPortfolioId: String,    // Business Portfolio ID (Step 3 — required for Step 5)
        metaAppId: String,
        businessId: String,
        displayPhone: String,           // Human-readable phone e.g. +91 98765 43210 (Step 6)
        verifiedName: String,           // Business display name (Step 6)
        qualityRating: String,          // GREEN / YELLOW / RED (Step 6)
        throughputLevel: String,        // STANDARD / HIGH / NOT_APPLICABLE (Step 6)
        onboardedAt: Date,              // Timestamp when PARTNER_ADDED webhook fired (Step 3)
        verified: { type: Boolean, default: false },
        templateRejections: { type: Map, of: String, default: {} }, // Map of templateName -> rejectionReason
    },
    webhookSecret: { type: String, default: '' },  // AES-256 encrypted hex string
    openaiApiKey: { type: String, default: '' },
}, { timestamps: true });
const Tenant = mongoose.model('Tenant', tenantSchema);

const clientSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    name: { type: String, required: true },
    mobileNumber: { type: String, required: true },
    companyName: String,
    emailId: String,
    venue: { type: String, required: true },
    remark: { type: String },
}, { timestamps: true });

clientSchema.index({ tenantId: 1, name: 1 });
clientSchema.index({ tenantId: 1, mobileNumber: 1 });
clientSchema.index({ tenantId: 1, companyName: 1 });

const Client = mongoose.model('Client', clientSchema);

// Panel plans — base prices with 18% GST included in totalPrice
const PANEL_PLANS = [
    { id: 'panel_1m', name: '1 Month Access', description: '1 month full panel access', basePrice: Math.round(1499 / 1.18), gstPercent: 18, totalPrice: 1499, panelDays: 30 },
    { id: 'panel_3m', name: '3 Month Access', description: '3 months full panel access', basePrice: Math.round(3999 / 1.18), gstPercent: 18, totalPrice: 3999, panelDays: 90 },
    { id: 'panel_6m', name: '6 Month Access', description: '6 months full panel access', basePrice: Math.round(7499 / 1.18), gstPercent: 18, totalPrice: 7499, panelDays: 180 },
    { id: 'panel_12m', name: '12 Month Access', description: '12 months full panel access', basePrice: Math.round(11999 / 1.18), gstPercent: 18, totalPrice: 11999, panelDays: 365 },
];

function getPlanById(planId) {
    return PANEL_PLANS.find(p => p.id === planId) || PANEL_PLANS[3]; // default to 12m
}

// Keep PANEL_PLAN as alias for backward compat (defaults to 12m plan)
const PANEL_PLAN = PANEL_PLANS[3];

const conversationSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    contactId: { type: String, required: true },
    name: String,
    lastMessage: String,
    lastActive: { type: Date, default: Date.now },
    hasReply: { type: Boolean, default: false },
}, { timestamps: true });
conversationSchema.index({ tenantId: 1, contactId: 1 }, { unique: true });
const Conversation = mongoose.model('Conversation', conversationSchema);

const messageSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    contactId: { type: String, required: true },
    text: String,
    isMe: Boolean,
    time: String,
    timestamp: { type: Date, default: Date.now },
    // Structured message metadata (optional — legacy docs without these fields remain readable)
    messageType: { type: String },          // 'text'|'template'|'image'|'video'|'audio'|'voice'|'document'|'sticker'|'location'|'contacts'|'reaction'|'interactive'|'unsupported'
    mediaUrl: { type: String },          // Meta mediaId for media messages
    templateName: { type: String },          // template name for outbound templates
    templateBody: { type: String },          // resolved body text of the template
    interactivePayload: { type: mongoose.Schema.Types.Mixed }, // { type, title, id }
});
const Message = mongoose.model('Message', messageSchema);

const campaignSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    id: { type: String, required: true },
    template: String,
    timestamp: { type: Date, default: Date.now },
    dispatchedAt: { type: Date, default: null },
    totalCount: { type: Number, default: 0 },
    successCount: { type: Number, default: 0 },
    failureCount: { type: Number, default: 0 },
    deliveredCount: { type: Number, default: 0 },
    readCount: { type: Number, default: 0 },
    // Retry configuration snapshot
    retryConfig: {
        version: { type: Number, required: true, default: 0 },
        phases: [{
            phaseNumber: { type: Number, required: true },
            intervalHours: { type: Number, required: true }
        }]
    },
    // Campaign execution state
    status: {
        type: String,
        enum: ['initial', 'retrying', 'completed', 'error'],
        default: 'initial'
    },
    currentPhase: { type: Number, default: 1 },
    // Phase-wise statistics
    phaseStats: [{
        phaseNumber: { type: Number, required: true },
        successCount: { type: Number, default: 0 },
        failureCount: { type: Number, default: 0 },
        executedAt: { type: Date },
        completedAt: { type: Date }
    }]
}, { timestamps: true });
campaignSchema.index({ tenantId: 1, id: 1 }, { unique: true });
campaignSchema.index({ tenantId: 1, status: 1 });
const Campaign = mongoose.model('Campaign', campaignSchema);

const paymentRecordSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    paymentId: { type: String, required: true },
    orderId: String,
    category: { type: String, default: 'panel_renewal' }, // 'panel_renewal' | 'credit_purchase'
    description: String,
    amount: Number,
    credits: { type: Number, default: 0 },
    timestamp: { type: Date, default: Date.now },
}, { timestamps: true });
paymentRecordSchema.index({ tenantId: 1, timestamp: -1 });
const PaymentRecord = mongoose.model('PaymentRecord', paymentRecordSchema);

const recipientSchema = new mongoose.Schema({
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
    deliveryTimestamp: { type: Date, default: null },
    retryHistory: [{
        phaseNumber: Number,
        attemptedAt: Date,
        status: String
    }]
});
recipientSchema.index({ campaignId: 1, phaseNumber: 1 });
recipientSchema.index({ campaignId: 1, status: 1 });
const Recipient = mongoose.model('Recipient', recipientSchema);

const retryConfigurationSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    version: { type: Number, required: true },
    phases: [{
        phaseNumber: {
            type: Number,
            required: true,
            min: [1, 'Phase number must be at least 1'],
            max: [5, 'Phase number must be at most 5']
        },
        intervalHours: {
            type: Number,
            required: true,
            min: [1, 'Interval must be at least 1 hour'],
            max: [48, 'Interval must be at most 48 hours']
        }
    }],
    createdAt: { type: Date, default: Date.now },
    isActive: { type: Boolean, default: true }
}, { timestamps: true });

// Validation: 0-5 phases
retryConfigurationSchema.path('phases').validate(function (phases) {
    return phases.length >= 0 && phases.length <= 5;
}, 'Phase count must be between 0 and 5');

// Validation: Ascending order of intervals
retryConfigurationSchema.path('phases').validate(function (phases) {
    if (phases.length <= 1) return true;
    for (let i = 1; i < phases.length; i++) {
        if (phases[i].intervalHours <= phases[i - 1].intervalHours) {
            return false;
        }
    }
    return true;
}, 'Interval hours must be in ascending order');

// Validation: Sequential phase numbers
retryConfigurationSchema.path('phases').validate(function (phases) {
    for (let i = 0; i < phases.length; i++) {
        if (phases[i].phaseNumber !== i + 1) {
            return false;
        }
    }
    return true;
}, 'Phase numbers must be sequential starting from 1');

// Indexes — one config per tenant
retryConfigurationSchema.index({ tenantId: 1, isActive: 1 });
retryConfigurationSchema.index({ tenantId: 1 }, { unique: true });

const RetryConfiguration = mongoose.model('RetryConfiguration', retryConfigurationSchema);

const scheduledRetryPhaseSchema = new mongoose.Schema({
    campaignId: { type: String, required: true, index: true },
    tenantId: { type: String, required: true },
    phaseNumber: { type: Number, required: true },
    scheduledAt: { type: Date, required: true, index: true },
    status: {
        type: String,
        enum: ['pending', 'executing', 'completed', 'cancelled', 'failed'],
        default: 'pending'
    },
    executedAt: { type: Date },
    errorMessage: { type: String }
}, { timestamps: true });

// Indexes
scheduledRetryPhaseSchema.index({ status: 1, scheduledAt: 1 });
scheduledRetryPhaseSchema.index({ campaignId: 1, phaseNumber: 1 });

const ScheduledRetryPhase = mongoose.model('ScheduledRetryPhase', scheduledRetryPhaseSchema);

const scheduledCampaignSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    campaignName: { type: String, default: '' },
    template: { type: String, required: true },
    language: { type: String, default: 'en_US' },
    recipients: { type: Array, required: true }, // [{mobileNumber, variables}]
    mediaId: String,
    mediaType: String,
    scheduledAt: { type: Date, required: true },
    status: { type: String, default: 'pending' }, // pending | running | completed | failed | cancelled
    resultCampaignId: String,
    successCount: { type: Number, default: 0 },
    failureCount: { type: Number, default: 0 },
    errorMessage: String,
}, { timestamps: true });
const ScheduledCampaign = mongoose.model('ScheduledCampaign', scheduledCampaignSchema);

const statusMappingSchema = new mongoose.Schema({
    wamid: { type: String, unique: true },
    tenantId: String,
    campaignId: String,
    to: String,
    timestamp: { type: Date, default: Date.now },
});
const StatusMapping = mongoose.model('StatusMapping', statusMappingSchema);

const chatbotSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    name: { type: String, required: true },
    triggerKeywords: { type: [String], default: [] },
    flow: { type: mongoose.Schema.Types.Mixed, default: {} },
    isActive: { type: Boolean, default: false },
}, { timestamps: true });
const Chatbot = mongoose.model('Chatbot', chatbotSchema);

const chatbotSessionSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    contactId: { type: String, required: true },
    chatbotId: { type: String, required: true },
    currentNodeId: { type: String },
    lastReply: { type: String },
    waitingForReply: { type: Boolean, default: false },
}, { timestamps: true });
chatbotSessionSchema.index({ tenantId: 1, contactId: 1 }, { unique: true });
const ChatbotSession = mongoose.model('ChatbotSession', chatbotSessionSchema);

const chatbotAnalyticsSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    chatbotId: { type: String, required: true },
    date: { type: Date, required: true },
    totalSessions: { type: Number, default: 0 },
    completedSessions: { type: Number, default: 0 },
    droppedSessions: { type: Number, default: 0 },
    messagesSent: { type: Number, default: 0 },
});
chatbotAnalyticsSchema.index({ tenantId: 1, chatbotId: 1, date: 1 });
const ChatbotAnalytics = mongoose.model('ChatbotAnalytics', chatbotAnalyticsSchema);

const leadSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    name: { type: String, default: '' },
    mobileNumber: { type: String, required: true },   // normalised, e.g. "919876543210"
    email: { type: String, default: '' },
    companyName: { type: String, default: '' },
    source: { type: String, enum: ['shopify', 'wordpress'], required: true },
    formName: { type: String, default: '' },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
    status: { type: String, enum: ['new', 'contacted', 'converted', 'failed'], default: 'new' },
    clientId: { type: mongoose.Schema.Types.ObjectId, ref: 'Client' },
    isDuplicate: { type: Boolean, default: false },
}, { timestamps: true });
leadSchema.index({ tenantId: 1, createdAt: -1 });
leadSchema.index({ tenantId: 1, mobileNumber: 1 });
const Lead = mongoose.model('Lead', leadSchema);

const leadTriggerSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    source: { type: String, enum: ['shopify', 'wordpress', 'any'], default: 'any' },
    formName: { type: String, default: '' },
    action: { type: String, enum: ['send_template', 'start_chatbot'], required: true },
    templateName: { type: String, default: '' },
    templateLanguage: { type: String, default: 'en_US' },
    mediaId: { type: String, default: '' },
    mediaType: { type: String, default: '' },
    variableMapping: { type: mongoose.Schema.Types.Mixed, default: {} }, // {"1":"name","2":"mobileNumber"}
    chatbotId: { type: String, default: '' },
    isActive: { type: Boolean, default: true },
}, { timestamps: true });
const LeadTrigger = mongoose.model('LeadTrigger', leadTriggerSchema);

const clientTriggerSchema = new mongoose.Schema({
    tenantId: { type: String, required: true, unique: true },
    templateName: { type: String, required: true },
    templateLanguage: { type: String, default: 'en_US' },
    mediaId: { type: String, default: '' },
    mediaType: { type: String, default: '' },
    variableMapping: { type: mongoose.Schema.Types.Mixed, default: {} }, // {"1":"name","2":"mobileNumber"}
    isActive: { type: Boolean, default: true },
}, { timestamps: true });
const ClientTrigger = mongoose.model('ClientTrigger', clientTriggerSchema);

const webhookLogSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    sourceIp: String,
    httpStatus: Number,
    timestamp: { type: Date, default: Date.now, expires: 2592000 }, // TTL 30 days
});
const WebhookLog = mongoose.model('WebhookLog', webhookLogSchema);

const onboardingLogSchema = new mongoose.Schema({
    tenantId: { type: String, default: null },
    sessionId: { type: String, default: null },
    wabaId: { type: String, default: null },
    businessPortfolioId: { type: String, default: null },
    phoneNumberId: { type: String, default: null },
    step: { type: String, required: true },
    status: { type: String, enum: ['info', 'success', 'warning', 'error'], default: 'info' },
    message: { type: String, required: true },
    details: { type: mongoose.Schema.Types.Mixed, default: null },
    timestamp: { type: Date, default: Date.now, expires: 2592000 }, // TTL 30 days
});
const OnboardingLog = mongoose.model('OnboardingLog', onboardingLogSchema);

async function saveOnboardingLog({ tenantId, sessionId, wabaId, businessPortfolioId, phoneNumberId, step, status = 'info', message, details }) {
    try {
        const log = new OnboardingLog({
            tenantId,
            sessionId,
            wabaId,
            businessPortfolioId,
            phoneNumberId,
            step,
            status,
            message,
            details
        });

        // If tenantId is missing but we have wabaId, try to find the tenant to backfill tenantId
        if (!log.tenantId && wabaId) {
            const tenant = await Tenant.findOne({ 'whatsappConfig.businessAccountId': wabaId });
            if (tenant) {
                log.tenantId = tenant._id.toString();
            }
        }

        await log.save();

        const prefix = `[Onboarding][${status.toUpperCase()}][${step}]`;
        const detailsStr = details ? ` | Details: ${JSON.stringify(details)}` : '';
        console.log(`${prefix} ${message}${detailsStr}`);
    } catch (err) {
        console.error(`Failed to save onboarding log:`, err.message);
    }
}


const groupSchema = new mongoose.Schema({
    tenantId: { type: String, required: true },
    name: { type: String, required: true },
    clientIds: { type: [String], default: [] },
}, { timestamps: true });
groupSchema.index({ tenantId: 1 });
const Group = mongoose.model('Group', groupSchema);

// ── Retry / Campaign Lifecycle Services ──────────────────────────────────────
const ConfigurationManager = require('./services/ConfigurationManager');
const RetryScheduler = require('./services/RetryScheduler');
const PhaseExecutor = require('./services/PhaseExecutor');
const CampaignLifecycleManager = require('./services/CampaignLifecycleManager');
const CampaignExecutor = require('./services/CampaignExecutor');
const MessageTracker = require('./services/MessageTracker');
const ReportGenerator = require('./services/ReportGenerator');
const SocketEmitter = require('./services/SocketEmitter');

const configManager = new ConfigurationManager(RetryConfiguration);

// messageSender adapter: wraps the existing WhatsApp send logic so PhaseExecutor
// can delegate to it without knowing about Express or axios directly.
const messageSenderAdapter = {
    /**
     * Send a WhatsApp template message to a single recipient as part of a retry phase.
     * @param {Object} recipient  - Recipient document
     * @param {Object} campaign   - Campaign document
     * @param {number} phaseNumber - Current retry phase number
     */
    async sendMessage(recipient, campaign, phaseNumber) {
        const tenant = await Tenant.findById(campaign.tenantId);
        if (!tenant) throw new Error(`Tenant not found: ${campaign.tenantId}`);

        const config = tenant.whatsappConfig;
        if (!config?.accessToken || !config?.phoneNumberId) {
            throw new Error('WhatsApp not configured for tenant');
        }

        const payload = {
            messaging_product: 'whatsapp',
            to: recipient.to,
            type: 'template',
            template: {
                name: campaign.template,
                language: { code: 'en_US' }
            }
        };

        const response = await axios.post(
            `${WHATSAPP_API_URL}/${config.phoneNumberId}/messages`,
            payload,
            { headers: { Authorization: `Bearer ${config.accessToken}` } }
        );

        const wamid = response.data?.messages?.[0]?.id;
        if (!wamid) throw new Error('No wamid returned from WhatsApp API');

        // Track the new wamid → campaign mapping so delivery webhooks are handled
        await StatusMapping.findOneAndUpdate(
            { wamid },
            { wamid, tenantId: campaign.tenantId, campaignId: campaign.id, to: recipient.to },
            { upsert: true }
        );

        // Record the retry attempt in the recipient's history
        await Recipient.findOneAndUpdate(
            { _id: recipient._id },
            {
                $set: { wamid, status: 'sent', sentAt: new Date().toISOString() },
                $push: {
                    retryHistory: {
                        phaseNumber,
                        attemptedAt: new Date(),
                        status: 'sent'
                    }
                }
            }
        );

        console.log(
            `[messageSender] Sent retry phase ${phaseNumber} message to ${recipient.to} ` +
            `(campaign ${campaign.id}, wamid ${wamid})`
        );
    }
};

const retryScheduler = new RetryScheduler({ ScheduledRetryPhase, Campaign });
const phaseExecutor = new PhaseExecutor({
    Campaign,
    Recipient,
    messageSender: messageSenderAdapter,
    retryScheduler
});
retryScheduler.phaseExecutor = phaseExecutor;

const campaignLifecycleManager = new CampaignLifecycleManager({
    Campaign,
    Recipient,
    configurationManager: configManager,
    retryScheduler
});

// Wire campaignLifecycleManager into phaseExecutor after both are created
phaseExecutor.campaignLifecycleManager = campaignLifecycleManager;

const campaignExecutor = new CampaignExecutor({
    Campaign,
    Recipient,
    StatusMapping,
    Tenant,
    axios,
    socketEmitter: SocketEmitter,
    campaignLifecycleManager
});

const messageTracker = new MessageTracker(Recipient);
const reportGenerator = new ReportGenerator(Campaign, ScheduledRetryPhase, Recipient);

//  Logging 
const logFile = 'server.log';
function logToFile(msg) {
    const timestamp = new Date().toISOString();
    fs.appendFileSync(logFile, `[${timestamp}] ${msg}\n`);
}
const originalLog = console.log;
const originalError = console.error;
console.log = (...args) => { logToFile(args.join(' ')); originalLog(...args); };
console.error = (...args) => { logToFile(args.join(' ')); originalError(...args); };

process.on('unhandledRejection', (reason, promise) => console.error(' Unhandled Rejection:', reason));
process.on('uncaughtException', (err) => console.error(' Uncaught Exception:', err));

//  App Setup 
const app = express();
app.use(cors());
const httpServer = http.createServer(app);
const io = new Server(httpServer, { cors: { origin: '*' } });
SocketEmitter.setIo(io);
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.raw({ type: 'application/octet-stream', limit: '50mb' }));

const VERIFY_TOKEN = process.env.WHATSAPP_VERIFY_TOKEN || 'whatsapp_bulk_verify_token_123';
const WHATSAPP_API_URL = 'https://graph.facebook.com/v25.0';

console.log(` Server starting at: ${new Date().toLocaleString()}`);

//  Razorpay 
let razorpay;
if (process.env.RAZORPAY_KEY_ID && process.env.RAZORPAY_KEY_SECRET) {
    razorpay = new Razorpay({ key_id: process.env.RAZORPAY_KEY_ID, key_secret: process.env.RAZORPAY_KEY_SECRET });
    console.log(' Razorpay initialized');
} else {
    console.warn(' Razorpay keys missing. Payment features disabled.');
}

//  Email 
const transporter = nodemailer.createTransport({
    service: process.env.EMAIL_SERVICE,
    auth: { user: process.env.EMAIL_USER, pass: process.env.EMAIL_PASS }
});

//  Helpers 
async function logCreditTransaction(tenantId, type, credits, description) {
    // No-op: credit transaction logging removed
}

async function sendCredentialsEmail(email, password, businessName) {
    try {
        await transporter.sendMail({
            from: `"Sendzyy" <${process.env.EMAIL_USER}>`,
            to: email,
            subject: `🎉 Welcome to Sendzyy — Your Account is Ready!`,
            html: `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f0f7f0;font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f7f0;padding:40px 0;">
<tr><td align="center">
<table width="580" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:20px;overflow:hidden;box-shadow:0 8px 40px rgba(0,0,0,0.10);">

  <!-- Header -->
  <tr>
    <td style="background:linear-gradient(135deg,#1B5E20 0%,#2E7D32 55%,#43A047 100%);padding:50px 48px 38px;text-align:center;">
      <div style="background:rgba(255,255,255,0.15);border-radius:50%;width:80px;height:80px;margin:0 auto 20px;display:table-cell;vertical-align:middle;text-align:center;">
        <span style="font-size:40px;">🚀</span>
      </div>
      <h1 style="margin:0 0 8px;color:#fff;font-size:32px;font-weight:800;letter-spacing:0.5px;">Welcome to Sendzyy!</h1>
      <p style="margin:0;color:rgba(255,255,255,0.82);font-size:15px;">WhatsApp Business Messaging Platform</p>
    </td>
  </tr>

  <!-- Body -->
  <tr>
    <td style="padding:40px 48px 0;">
      <p style="margin:0 0 4px;color:#aaa;font-size:12px;text-transform:uppercase;letter-spacing:1.5px;font-weight:600;">Hello,</p>
      <h2 style="margin:0 0 16px;color:#1B5E20;font-size:24px;font-weight:700;">${businessName} 👋</h2>
      <p style="margin:0 0 28px;color:#555;font-size:15px;line-height:1.8;">
        Your account has been created successfully. Use the credentials below to log in and start sending bulk WhatsApp messages to your customers.
      </p>
    </td>
  </tr>

  <!-- Credentials Card -->
  <tr>
    <td style="padding:0 48px;">
      <table width="100%" cellpadding="0" cellspacing="0" style="background:linear-gradient(135deg,#f1fdf3,#e8f5e9);border:2px solid #a5d6a7;border-radius:16px;">
        <tr>
          <td style="padding:24px 28px 8px;">
            <p style="margin:0 0 20px;color:#2E7D32;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;">🔐 Your Login Credentials</p>
          </td>
        </tr>
        <tr>
          <td style="padding:0 28px 12px;border-bottom:1px solid #c8e6c9;">
            <p style="margin:0 0 4px;color:#888;font-size:12px;text-transform:uppercase;letter-spacing:1px;">Email Address</p>
            <p style="margin:0;color:#1B5E20;font-size:17px;font-weight:700;font-family:'Courier New',monospace;">${email}</p>
          </td>
        </tr>
        <tr>
          <td style="padding:12px 28px 24px;">
            <p style="margin:0 0 4px;color:#888;font-size:12px;text-transform:uppercase;letter-spacing:1px;">Password</p>
            <p style="margin:0;color:#1B5E20;font-size:26px;font-weight:800;letter-spacing:4px;font-family:'Courier New',monospace;">${password}</p>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- CTA Button -->
  <tr>
    <td style="padding:32px 48px 24px;text-align:center;">
      <a href="https://app.sendzyy.com" target="_blank"
         style="display:inline-block;background:linear-gradient(135deg,#2E7D32,#43A047);color:#fff;text-decoration:none;font-size:16px;font-weight:700;padding:16px 56px;border-radius:50px;box-shadow:0 6px 20px rgba(46,125,50,0.38);letter-spacing:0.5px;">
        ✨ &nbsp; Login to Dashboard
      </a>
    </td>
  </tr>

  <!-- Features -->
  <tr>
    <td style="padding:0 48px 28px;">
      <p style="margin:0 0 14px;color:#333;font-size:14px;font-weight:700;">What you can do with Sendzyy:</p>
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td width="50%" style="padding:5px 6px 5px 0;">
            <div style="background:#e8f5e9;border-radius:8px;padding:10px 14px;color:#2E7D32;font-size:13px;">📤 Bulk WhatsApp Messaging</div>
          </td>
          <td width="50%" style="padding:5px 0 5px 6px;">
            <div style="background:#e8f5e9;border-radius:8px;padding:10px 14px;color:#2E7D32;font-size:13px;">📋 Message Templates</div>
          </td>
        </tr>
        <tr>
          <td width="50%" style="padding:5px 6px 5px 0;">
            <div style="background:#e8f5e9;border-radius:8px;padding:10px 14px;color:#2E7D32;font-size:13px;">📊 Campaign Analytics</div>
          </td>
          <td width="50%" style="padding:5px 0 5px 6px;">
            <div style="background:#e8f5e9;border-radius:8px;padding:10px 14px;color:#2E7D32;font-size:13px;">💬 Live Customer Chat</div>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Warning -->
  <tr>
    <td style="padding:0 48px 36px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="background:#fff8e1;border-left:4px solid #FFC107;border-radius:0 10px 10px 0;padding:14px 18px;">
            <p style="margin:0;color:#795548;font-size:13px;line-height:1.7;">
              ⚠️ <strong>Security tip:</strong> Change your password after first login via <strong>Settings → Security & Password</strong>.
            </p>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Footer -->
  <tr>
    <td style="background:#f5f5f5;border-top:1px solid #e0e0e0;padding:28px 48px;text-align:center;">
      <p style="margin:0 0 4px;color:#1B5E20;font-size:15px;font-weight:700;">Sendzyy</p>
      <p style="margin:0 0 10px;color:#aaa;font-size:12px;">WhatsApp Business Messaging · iFlora Info Pvt. Ltd.</p>
      <p style="margin:0;color:#ccc;font-size:11px;">© ${new Date().getFullYear()} Sendzyy · <a href="https://app.sendzyy.com" style="color:#4CAF50;text-decoration:none;">app.sendzyy.com</a></p>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>`,
        });
        console.log(` Welcome email sent to ${email}`);
    } catch (error) { console.error(' Welcome email failed:', error); }
}

async function sendInvoiceEmail(email, businessName, paymentId, pkg, amount) {
    try {
        const now = new Date();
        const dateStr = now.toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });
        const invoiceNo = `INV-${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}-${paymentId.slice(-6).toUpperCase()}`;

        await transporter.sendMail({
            from: `"Sendzyy" <${process.env.EMAIL_USER}>`,
            to: email,
            subject: `✅ Payment Successful — ${pkg.name} Plan Activated | Sendzyy`,
            html: `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f0f7f0;font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f7f0;padding:40px 0;">
<tr><td align="center">
<table width="580" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:20px;overflow:hidden;box-shadow:0 8px 40px rgba(0,0,0,0.10);">

  <!-- Header -->
  <tr>
    <td style="background:linear-gradient(135deg,#1B5E20 0%,#2E7D32 55%,#43A047 100%);padding:50px 48px 38px;text-align:center;">
      <div style="background:rgba(255,255,255,0.15);border-radius:50%;width:80px;height:80px;margin:0 auto 20px;display:table-cell;vertical-align:middle;text-align:center;">
        <span style="font-size:44px;">✅</span>
      </div>
      <h1 style="margin:0 0 8px;color:#fff;font-size:32px;font-weight:800;">Payment Successful!</h1>
      <p style="margin:0;color:rgba(255,255,255,0.82);font-size:15px;">Your <strong>${pkg.name}</strong> plan is now active</p>
    </td>
  </tr>

  <!-- Greeting -->
  <tr>
    <td style="padding:40px 48px 0;">
      <p style="margin:0 0 4px;color:#aaa;font-size:12px;text-transform:uppercase;letter-spacing:1.5px;font-weight:600;">Hello,</p>
      <h2 style="margin:0 0 14px;color:#1B5E20;font-size:24px;font-weight:700;">${businessName} 👋</h2>
      <p style="margin:0 0 28px;color:#555;font-size:15px;line-height:1.8;">
        Thank you for your purchase! Your payment has been verified and ${pkg.credits > 0 ? `<strong>${pkg.credits.toLocaleString('en-IN')} credits</strong> have been added to your account.` : `your <strong>${pkg.name}</strong> plan is now active.`}
      </p>
    </td>
  </tr>

  <!-- Invoice Table -->
  <tr>
    <td style="padding:0 48px;">
      <table width="100%" cellpadding="0" cellspacing="0" style="border:2px solid #a5d6a7;border-radius:16px;overflow:hidden;">
        <!-- Invoice Header -->
        <tr>
          <td colspan="2" style="background:linear-gradient(135deg,#f1fdf3,#e8f5e9);padding:16px 24px;border-bottom:1px solid #c8e6c9;">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td>
                  <p style="margin:0;color:#2E7D32;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:1px;">🧾 Invoice Receipt</p>
                  <p style="margin:3px 0 0;color:#888;font-size:11px;font-family:'Courier New',monospace;">${invoiceNo}</p>
                </td>
                <td align="right">
                  <p style="margin:0;color:#888;font-size:12px;">${dateStr}</p>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        <!-- Rows -->
        <tr>
          <td style="padding:14px 24px;color:#777;font-size:13px;border-bottom:1px solid #f0f0f0;">Plan</td>
          <td style="padding:14px 24px;color:#1B5E20;font-size:14px;font-weight:700;text-align:right;border-bottom:1px solid #f0f0f0;">${pkg.name}</td>
        </tr>
        ${pkg.credits > 0 ? `<tr>
          <td style="padding:14px 24px;color:#777;font-size:13px;border-bottom:1px solid #f0f0f0;">Credits Added</td>
          <td style="padding:14px 24px;color:#333;font-size:14px;font-weight:600;text-align:right;border-bottom:1px solid #f0f0f0;">${pkg.credits.toLocaleString('en-IN')} credits</td>
        </tr>` : ''}
        <tr>
          <td style="padding:14px 24px;color:#777;font-size:13px;border-bottom:1px solid #f0f0f0;">Payment ID</td>
          <td style="padding:14px 24px;color:#555;font-size:12px;font-family:'Courier New',monospace;text-align:right;border-bottom:1px solid #f0f0f0;">${paymentId}</td>
        </tr>
        <!-- Total -->
        <tr>
          <td style="padding:18px 24px;background:#e8f5e9;">
            <span style="color:#2E7D32;font-size:15px;font-weight:700;">Total Paid</span>
          </td>
          <td style="padding:18px 24px;background:#e8f5e9;text-align:right;">
            <span style="color:#1B5E20;font-size:26px;font-weight:800;">₹${Number(amount).toLocaleString('en-IN')}</span>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- CTA -->
  <tr>
    <td style="padding:32px 48px 24px;text-align:center;">
      <a href="https://app.sendzyy.com" target="_blank"
         style="display:inline-block;background:linear-gradient(135deg,#2E7D32,#43A047);color:#fff;text-decoration:none;font-size:16px;font-weight:700;padding:16px 56px;border-radius:50px;box-shadow:0 6px 20px rgba(46,125,50,0.38);letter-spacing:0.5px;">
        🚀 &nbsp; Go to Dashboard
      </a>
    </td>
  </tr>

  <!-- Support Note -->
  <tr>
    <td style="padding:0 48px 36px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="background:#e3f2fd;border-left:4px solid #90CAF9;border-radius:0 10px 10px 0;padding:14px 18px;">
            <p style="margin:0;color:#1565C0;font-size:13px;line-height:1.7;">
              💬 <strong>Need help?</strong> Reply to this email or contact our support team. We're here to help you get the most out of Sendzyy.
            </p>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Footer -->
  <tr>
    <td style="background:#f5f5f5;border-top:1px solid #e0e0e0;padding:28px 48px;text-align:center;">
      <p style="margin:0 0 4px;color:#1B5E20;font-size:15px;font-weight:700;">Sendzyy</p>
      <p style="margin:0 0 10px;color:#aaa;font-size:12px;">WhatsApp Business Messaging · iFlora Info Pvt. Ltd.</p>
      <p style="margin:0;color:#ccc;font-size:11px;">© ${new Date().getFullYear()} Sendzyy · <a href="https://app.sendzyy.com" style="color:#4CAF50;text-decoration:none;">app.sendzyy.com</a></p>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>`,
        });
        console.log(` Invoice email sent to ${email}`);
    } catch (error) { console.error(' Invoice email failed:', error); }
}

//  Auth Middleware 
const authenticate = (req, res, next) => {
    // Accept token from Authorization header OR ?token= query param (for browser window.open requests)
    const token = req.headers['authorization']?.split(' ')[1] || req.query.token;
    if (!token) return res.sendStatus(401);
    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) return res.sendStatus(403);
        if (!user.tenantId) return res.status(401).json({ error: 'Invalid session' });
        req.user = user;
        next();
    });
};

//  System Update broadcast (triggered via Postman/admin)
app.post('/api/admin/system-update', (req, res) => {
    const { secret, message } = req.body;
    const adminSecret = process.env.ADMIN_UPDATE_SECRET || 'sendzyy-update-secret-9988';

    if (!secret || secret !== adminSecret) {
        return res.status(401).json({ error: 'Unauthorized: Invalid secret' });
    }

    if (!message || typeof message !== 'string') {
        return res.status(400).json({ error: 'Bad Request: Message is required and must be a string' });
    }

    console.log(`[System Update] Triggered update alert broadcast with message: "${message}"`);

    // Broadcast update alert to all connected sockets
    io.emit('system_update', { message });

    return res.json({ success: true, message: 'Broadcast sent to all clients.' });
});

//  Register — validates only, does NOT save to DB. Returns a short-lived registration token.
app.post('/register', async (req, res) => {
    const { name, email, password } = req.body;
    if (!name || !email || !password) return res.status(400).json({ error: 'Missing fields' });
    try {
        const existing = await Tenant.findOne({ email });
        if (existing) return res.status(400).json({ error: 'Email already registered' });

        const hashedPassword = await bcrypt.hash(password, 10);

        // Issue a short-lived registration token (15 min) — no DB write yet
        const regToken = jwt.sign(
            { type: 'registration', name, email, hashedPassword },
            process.env.JWT_SECRET,
            { expiresIn: '15m' }
        );

        res.status(201).json({ message: 'Validation successful. Complete payment to activate.', regToken });
    } catch (error) {
        console.error(' Registration Error:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// Create panel order for NEW registration (public - uses regToken)
app.post('/create-panel-order-register', async (req, res) => {
    const { regToken, planId } = req.body;
    if (!regToken) return res.status(400).json({ error: 'regToken required' });
    let regData;
    try {
        regData = jwt.verify(regToken, process.env.JWT_SECRET);
        if (regData.type !== 'registration') return res.status(400).json({ error: 'Invalid registration token' });
    } catch (e) {
        return res.status(401).json({ error: 'Registration token expired or invalid. Please register again.' });
    }
    if (!razorpay) return res.status(500).json({ error: 'Payment gateway not configured' });
    const plan = getPlanById(planId);
    try {
        const order = await razorpay.orders.create({
            amount: plan.totalPrice * 100,
            currency: 'INR',
            receipt: `reg_${Date.now()}`,
            notes: { email: regData.email, panelDays: plan.panelDays, planId: plan.id }
        });
        res.json({ ...order, panelDays: plan.panelDays, price: plan.totalPrice, planId: plan.id });
    } catch (error) {
        res.status(500).json({ error: 'Failed to create order', details: error });
    }
});

// Verify panel payment for NEW registration - creates tenant in DB only on success
app.post('/verify-panel-payment-register', async (req, res) => {
    const { regToken, planId, razorpay_order_id, razorpay_payment_id, razorpay_signature } = req.body;
    if (!regToken) return res.status(400).json({ error: 'regToken required' });
    let regData;
    try {
        regData = jwt.verify(regToken, process.env.JWT_SECRET);
        if (regData.type !== 'registration') return res.status(400).json({ error: 'Invalid registration token' });
    } catch (e) {
        return res.status(401).json({ error: 'Registration token expired. Please register again.' });
    }
    const plan = getPlanById(planId);
    try {
        const expectedSignature = crypto
            .createHmac('sha256', process.env.RAZORPAY_KEY_SECRET)
            .update(`${razorpay_order_id}|${razorpay_payment_id}`)
            .digest('hex');
        if (expectedSignature !== razorpay_signature)
            return res.status(400).json({ error: 'Invalid payment signature' });

        const existing = await Tenant.findOne({ email: regData.email });
        if (existing) return res.status(400).json({ error: 'Email already registered' });

        const now = new Date();
        const panelExpiresAt = new Date(now.getTime() + plan.panelDays * 24 * 60 * 60 * 1000);

        const tenant = await Tenant.create({
            name: regData.name,
            email: regData.email,
            password: regData.hashedPassword,
            subscription: {
                planId: plan.id,
                planName: plan.name,
                price: plan.totalPrice,
                expiryDate: panelExpiresAt,
                lastPaymentId: razorpay_payment_id,
                lastPaymentDate: now,
            }
        });

        sendCredentialsEmail(regData.email, '(your chosen password)', regData.name);
        try { sendInvoiceEmail(regData.email, regData.name, razorpay_payment_id, { name: plan.name, credits: 0, price: plan.totalPrice }, plan.totalPrice); } catch (_) { }

        // Save payment record
        await PaymentRecord.create({
            tenantId: tenant._id.toString(),
            paymentId: razorpay_payment_id,
            orderId: razorpay_order_id,
            category: 'panel_renewal',
            description: `${plan.name} — New Registration`,
            amount: plan.totalPrice,
            timestamp: now,
        });

        res.json({ success: true, message: 'Account created. Please log in.' });
    } catch (error) {
        console.error(' Registration payment verification error:', error);
        res.status(500).json({ error: 'Failed to complete registration', details: error.message });
    }
});

//  Login 
app.post('/login', async (req, res) => {
    const { email, password } = req.body;
    try {
        const tenant = await Tenant.findOne({ email });
        if (!tenant) return res.status(401).json({ error: 'Invalid credentials' });

        const isMatch = await bcrypt.compare(password, tenant.password);
        if (!isMatch) return res.status(401).json({ error: 'Invalid credentials' });

        // Block login if no paid plan has ever been activated
        const planId = tenant.subscription?.planId;
        if (!planId || planId === 'free') {
            return res.status(403).json({
                error: 'no_subscription',
                message: 'No active subscription found. Please complete your plan purchase to log in.',
            });
        }

        // Block login if panel subscription has expired
        if (tenant.subscription?.expiryDate) {
            const now = new Date();
            const expiryDate = new Date(tenant.subscription.expiryDate);
            if (now > expiryDate) {
                return res.status(403).json({
                    error: 'subscription_expired',
                    message: 'Your subscription has expired. Please renew your plan to continue.',
                    expiredAt: tenant.subscription.expiryDate,
                });
            }
        }

        const token = jwt.sign(
            { tenantId: tenant._id.toString(), email: tenant.email },
            process.env.JWT_SECRET,
            { expiresIn: '24h' }
        );

        res.json({
            token,
            tenant: {
                id: tenant._id.toString(),
                name: tenant.name,
                email: tenant.email,
                subscription: tenant.subscription,
                whatsappConfig: tenant.whatsappConfig,
            }
        });
    } catch (error) {
        console.error(' Login Error:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// Panel plans - returns all available plans
app.get('/panel-plans', (req, res) => {
    res.json(PANEL_PLANS.map(p => ({
        id: p.id,
        name: p.name,
        description: p.description,
        basePrice: p.basePrice,
        gstPercent: p.gstPercent,
        totalPrice: p.totalPrice,
        panelDays: p.panelDays,
    })));
});
// Create panel order for authenticated renewal
app.post('/create-panel-order', authenticate, async (req, res) => {
    if (!razorpay) return res.status(500).json({ error: 'Payment gateway not configured' });
    const plan = getPlanById(req.body.planId);
    try {
        const order = await razorpay.orders.create({
            amount: plan.totalPrice * 100,
            currency: 'INR',
            receipt: `panel_${Date.now()}`,
            notes: { tenantId: req.user.tenantId, panelDays: plan.panelDays, planId: plan.id }
        });
        res.json({ ...order, panelDays: plan.panelDays, price: plan.totalPrice, planId: plan.id });
    } catch (error) {
        res.status(500).json({ error: 'Failed to create panel order', details: error });
    }
});

// Create panel order for RENEWAL - public, authenticates via email+password
app.post('/create-panel-order-renew', async (req, res) => {
    const { email, password, planId } = req.body;
    if (!email || !password) return res.status(400).json({ error: 'email and password required' });
    const tenant = await Tenant.findOne({ email });
    if (!tenant) return res.status(401).json({ error: 'Invalid credentials' });
    const valid = await bcrypt.compare(password, tenant.password);
    if (!valid) return res.status(401).json({ error: 'Invalid credentials' });
    if (!razorpay) return res.status(500).json({ error: 'Payment gateway not configured' });
    const plan = getPlanById(planId);
    try {
        const order = await razorpay.orders.create({
            amount: plan.totalPrice * 100,
            currency: 'INR',
            receipt: `renew_${Date.now()}`,
            notes: { tenantId: tenant._id.toString(), panelDays: plan.panelDays, planId: plan.id }
        });
        res.json({ ...order, panelDays: plan.panelDays, price: plan.totalPrice, planId: plan.id });
    } catch (error) {
        res.status(500).json({ error: 'Failed to create panel order', details: error });
    }
});

// Verify panel payment for RENEWAL - public, authenticates via email+password
app.post('/verify-panel-payment-renew', async (req, res) => {
    const { email, password, planId, razorpay_order_id, razorpay_payment_id, razorpay_signature } = req.body;
    if (!email || !password) return res.status(400).json({ error: 'email and password required' });
    const tenant = await Tenant.findOne({ email });
    if (!tenant) return res.status(401).json({ error: 'Invalid credentials' });
    const valid = await bcrypt.compare(password, tenant.password);
    if (!valid) return res.status(401).json({ error: 'Invalid credentials' });
    const plan = getPlanById(planId);
    try {
        const expectedSignature = crypto
            .createHmac('sha256', process.env.RAZORPAY_KEY_SECRET)
            .update(`${razorpay_order_id}|${razorpay_payment_id}`)
            .digest('hex');
        if (expectedSignature !== razorpay_signature)
            return res.status(400).json({ error: 'Invalid payment signature' });
        const now = new Date();
        const panelExpiresAt = new Date(now.getTime() + plan.panelDays * 24 * 60 * 60 * 1000);
        await Tenant.findByIdAndUpdate(tenant._id, {
            'subscription.planId': plan.id,
            'subscription.planName': plan.name,
            'subscription.price': plan.totalPrice,
            'subscription.expiryDate': panelExpiresAt,
            'subscription.lastPaymentId': razorpay_payment_id,
            'subscription.lastPaymentDate': now,
        });
        try { sendInvoiceEmail(tenant.email, tenant.name, razorpay_payment_id, { name: plan.name, credits: 0, price: plan.totalPrice }, plan.totalPrice); } catch (_) { }
        await PaymentRecord.create({
            tenantId: tenant._id.toString(),
            paymentId: razorpay_payment_id,
            orderId: razorpay_order_id,
            category: 'panel_renewal',
            description: `${plan.name} — Renewal`,
            amount: plan.totalPrice,
            timestamp: now,
        });
        res.json({ success: true, panelExpiresAt });
    } catch (error) {
        res.status(500).json({ error: 'Panel payment verification failed', details: error.message });
    }
});

// Verify panel payment - authenticated renewal
app.post('/verify-panel-payment', authenticate, async (req, res) => {
    const { planId, razorpay_order_id, razorpay_payment_id, razorpay_signature } = req.body;
    const tenantId = req.user.tenantId;
    const plan = getPlanById(planId);
    try {
        const expectedSignature = crypto
            .createHmac('sha256', process.env.RAZORPAY_KEY_SECRET)
            .update(`${razorpay_order_id}|${razorpay_payment_id}`)
            .digest('hex');
        if (expectedSignature !== razorpay_signature)
            return res.status(400).json({ error: 'Invalid payment signature' });
        const now = new Date();
        const panelExpiresAt = new Date(now.getTime() + plan.panelDays * 24 * 60 * 60 * 1000);
        await Tenant.findByIdAndUpdate(tenantId, {
            'subscription.planId': plan.id,
            'subscription.planName': plan.name,
            'subscription.price': plan.totalPrice,
            'subscription.expiryDate': panelExpiresAt,
            'subscription.lastPaymentId': razorpay_payment_id,
            'subscription.lastPaymentDate': now,
        });
        const tenant = await Tenant.findById(tenantId);
        try { sendInvoiceEmail(tenant.email, tenant.name, razorpay_payment_id, { name: plan.name, credits: 0, price: plan.totalPrice }, plan.totalPrice); } catch (_) { }
        await PaymentRecord.create({
            tenantId,
            paymentId: razorpay_payment_id,
            orderId: razorpay_order_id,
            category: 'panel_renewal',
            description: `${plan.name} — Renewal`,
            amount: plan.totalPrice,
            timestamp: now,
        });
        res.json({ success: true, panelExpiresAt });
    } catch (error) {
        res.status(500).json({ error: 'Panel payment verification failed', details: error.message });
    }
});

app.get('/config', (req, res) => res.json({ razorpayKeyId: process.env.RAZORPAY_KEY_ID }));

// Payment history for authenticated tenant
app.get('/payment-history', authenticate, async (req, res) => {
    try {
        const records = await PaymentRecord.find({ tenantId: req.user.tenantId })
            .sort({ timestamp: -1 })
            .limit(100)
            .lean();
        res.json(records);
    } catch (error) {
        res.status(500).json({ error: 'Failed to fetch payment history' });
    }
});

//  Webhook Verification 
app.get('/webhook', (req, res) => {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];
    if (mode && token === VERIFY_TOKEN) {
        console.log(' Webhook Verified!');
        return res.status(200).send(challenge);
    }
    res.sendStatus(403);
});

//  Templates 
app.get('/fetch-templates', authenticate, async (req, res) => {
    try {
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
        const config = tenant.whatsappConfig;
        if (!config.accessToken || !config.businessAccountId)
            return res.status(400).json({ error: 'WhatsApp not configured' });
        const response = await axios.get(
            `${WHATSAPP_API_URL}/${config.businessAccountId}/message_templates`,
            { headers: { Authorization: `Bearer ${config.accessToken}` } }
        );
        res.json(response.data);
    } catch (error) {
        res.status(error.response?.status || 500).json({ error: 'Failed to fetch templates' });
    }
});

app.delete('/delete-template', authenticate, async (req, res) => {
    const { name } = req.query;
    if (!name) return res.status(400).json({ error: 'Template name is required' });
    try {
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
        const config = tenant.whatsappConfig;
        if (!config.accessToken || !config.businessAccountId)
            return res.status(400).json({ error: 'WhatsApp not configured' });
        const response = await axios.delete(
            `${WHATSAPP_API_URL}/${config.businessAccountId}/message_templates`,
            { params: { name }, headers: { Authorization: `Bearer ${config.accessToken}` } }
        );
        res.json(response.data);
    } catch (error) {
        res.status(error.response?.status || 500).json({ error: 'Failed to delete template', details: error.response?.data || error.message });
    }
});

app.post('/create-template', authenticate, async (req, res) => {
    try {
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
        const config = tenant.whatsappConfig;
        if (!config.accessToken || !config.businessAccountId)
            return res.status(400).json({ error: 'WhatsApp not configured' });
        const response = await axios.post(
            `${WHATSAPP_API_URL}/${config.businessAccountId}/message_templates`,
            req.body,
            { headers: { Authorization: `Bearer ${config.accessToken}` } }
        );
        res.json(response.data);
    } catch (error) {
        const errorData = error.response?.data || error.message;
        res.status(error.response?.status || 500).json({ error: 'Failed to create template', details: errorData });
    }
});

// In-memory OTP store: key = `${tenantId}:${phone}` → { otp, expiresAt }
const otpStore = new Map();
const OTP_TTL_MS = 10 * 60 * 1000; // 10 minutes

// Test Authentication Template — generate OTP and send via WhatsApp
app.post('/send-otp', authenticate, async (req, res) => {
    const { to, templateName, languageCode } = req.body;
    if (!to || !templateName) return res.status(400).json({ error: 'to and templateName are required' });

    try {
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
        const config = tenant.whatsappConfig;
        if (!config.accessToken || !config.phoneNumberId)
            return res.status(400).json({ error: 'WhatsApp not configured' });

        // Generate 6-digit OTP and store with TTL
        const otp = Math.floor(100000 + Math.random() * 900000).toString();
        const key = `${req.user.tenantId}:${to}`;
        otpStore.set(key, { otp, expiresAt: Date.now() + OTP_TTL_MS });

        const payload = {
            messaging_product: 'whatsapp',
            to,
            type: 'template',
            template: {
                name: templateName,
                language: { code: languageCode || 'en' },
                components: [
                    { type: 'body', parameters: [{ type: 'text', text: otp }] },
                    { type: 'button', sub_type: 'url', index: '0', parameters: [{ type: 'text', text: otp }] },
                ],
            },
        };

        const response = await axios.post(
            `${WHATSAPP_API_URL}/${config.phoneNumberId}/messages`,
            payload,
            { headers: { Authorization: `Bearer ${config.accessToken}` } }
        );

        res.json({ success: true, wamid: response.data.messages?.[0]?.id });
    } catch (error) {
        console.error(' Send OTP Error:', error.response?.data || error.message);
        res.status(error.response?.status || 500).json({
            error: 'Failed to send OTP',
            details: error.response?.data || error.message,
        });
    }
});

// Verify OTP
app.post('/verify-otp', authenticate, async (req, res) => {
    const { to, otp } = req.body;
    if (!to || !otp) return res.status(400).json({ error: 'to and otp are required' });

    const key = `${req.user.tenantId}:${to}`;
    const record = otpStore.get(key);

    if (!record) return res.status(400).json({ error: 'No OTP found for this number. Please request a new one.' });
    if (Date.now() > record.expiresAt) {
        otpStore.delete(key);
        return res.status(400).json({ error: 'OTP has expired. Please request a new one.' });
    }
    if (record.otp !== otp.trim()) {
        return res.status(400).json({ error: 'Incorrect OTP. Please try again.' });
    }

    otpStore.delete(key); // single-use
    res.json({ success: true, message: 'OTP verified successfully.' });
});

//  Profile & Config 
app.get('/me', authenticate, async (req, res) => {
    try {
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
        res.json({
            id: tenant._id.toString(),
            name: tenant.name,
            email: tenant.email,
            createdAt: tenant.createdAt,
            subscription: {
                ...tenant.subscription.toObject(),
                expiresAt: tenant.subscription.expiryDate,
            },
            whatsappConfig: tenant.whatsappConfig,
        });
    } catch (error) {
        res.status(500).json({ error: 'Failed to fetch profile' });
    }
});

app.post('/update-config', authenticate, async (req, res) => {
    const { phoneNumberId, accessToken, businessAccountId, metaAppId } = req.body;
    try {
        await Tenant.findByIdAndUpdate(req.user.tenantId, {
            'whatsappConfig.phoneNumberId': phoneNumberId,
            'whatsappConfig.accessToken': accessToken,
            'whatsappConfig.businessAccountId': businessAccountId,
            'whatsappConfig.metaAppId': metaAppId,
            'whatsappConfig.verified': true,
        });
        res.json({ success: true, message: 'Configuration updated' });
    } catch (error) {
        res.status(500).json({ error: 'Failed to update configuration' });
    }
});

// GET /onboarding-status — Checks verification status dynamically from Meta and templates setup
app.get('/onboarding-status', authenticate, async (req, res) => {
    try {
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
        
        const config = tenant.whatsappConfig || {};
        const wabaId = config.businessAccountId;
        const accessToken = config.accessToken;
        const phoneId = config.phoneNumberId;
        
        const status = {
            whatsappConnected: !!(wabaId && accessToken),
            phoneVerified: !!phoneId,
            metaBusinessVerified: 'NOT_VERIFIED', // VERIFIED | PENDING | NOT_VERIFIED
            hasApprovedTemplate: false
        };
        
        if (status.whatsappConnected) {
            try {
                const wabaRes = await axios.get(
                    `${WHATSAPP_API_URL}/${wabaId}`,
                    {
                        params: {
                            fields: 'business_verification_status',
                            access_token: accessToken
                        }
                    }
                );
                
                const metaStatus = wabaRes.data?.business_verification_status;
                if (metaStatus === 'verified') {
                    status.metaBusinessVerified = 'VERIFIED';
                } else if (['pending', 'pending_need_feedback', 'pending_rca', 'pending_submission', 'in_eligibility_review'].includes(metaStatus)) {
                    status.metaBusinessVerified = 'PENDING';
                } else {
                    status.metaBusinessVerified = 'NOT_VERIFIED';
                }
            } catch (err) {
                console.error('[OnboardingStatus] Failed to fetch business_verification_status:', err.response?.data || err.message);
            }
            
            try {
                const templatesRes = await axios.get(
                    `${WHATSAPP_API_URL}/${wabaId}/message_templates`,
                    {
                        params: {
                            access_token: accessToken,
                            limit: 100
                        }
                    }
                );
                const templates = templatesRes.data?.data || [];
                status.hasApprovedTemplate = templates.some(t => t.status === 'APPROVED');
            } catch (err) {
                console.error('[OnboardingStatus] Failed to fetch message templates:', err.response?.data || err.message);
            }
        }
        
        res.json(status);
    } catch (error) {
        console.error('[OnboardingStatus] Fatal error:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// GET /template-rejection-reason/:name — Fetches cached rejection reason for a template
app.get('/template-rejection-reason/:name', authenticate, async (req, res) => {
    try {
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
        
        const templateName = req.params.name;
        const savedReason = tenant.whatsappConfig?.templateRejections?.get(templateName) || null;
        
        res.json({ name: templateName, reason: savedReason });
    } catch (error) {
        console.error('[TemplateRejectionReason] Error:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// Refresh Meta Account — re-fetches WABA ID and Phone Number ID using the stored token.
// Called when embedded signup completes but IDs were not captured from the JS SDK.
// [Obsolete manual refresh-meta-account route removed to avoid conflicts. Real route is defined below in onboarding routes.]

app.post('/change-password', authenticate, async (req, res) => {
    const { currentPassword, newPassword } = req.body;
    if (!currentPassword || !newPassword)
        return res.status(400).json({ error: 'currentPassword and newPassword are required' });
    if (newPassword.length < 6)
        return res.status(400).json({ error: 'New password must be at least 6 characters' });
    try {
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'User not found' });
        const valid = await bcrypt.compare(currentPassword, tenant.password);
        if (!valid) return res.status(401).json({ error: 'Current password is incorrect' });
        const hashed = await bcrypt.hash(newPassword, 10);
        await Tenant.findByIdAndUpdate(req.user.tenantId, { password: hashed });
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: 'Failed to change password' });
    }
});

app.post('/forgot-password', async (req, res) => {
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'Email is required' });
    try {
        const tenant = await Tenant.findOne({ email });
        if (!tenant) return res.status(404).json({ error: 'No account found with this email' });

        // Generate a random 10-char password
        const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789@#$';
        let newPassword = '';
        for (let i = 0; i < 10; i++) newPassword += chars[Math.floor(Math.random() * chars.length)];

        const hashed = await bcrypt.hash(newPassword, 10);
        await Tenant.findByIdAndUpdate(tenant._id, { password: hashed });

        await transporter.sendMail({
            from: `"Sendzyy" <${process.env.EMAIL_USER}>`,
            to: email,
            subject: '🔐 Sendzyy — Your New Password',
            html: `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>Password Reset</title></head>
<body style="margin:0;padding:0;background-color:#f0f4f0;font-family:'Segoe UI',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f4f0;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">

        <!-- Header -->
        <tr>
          <td style="background:linear-gradient(135deg,#1B5E20 0%,#2E7D32 60%,#388E3C 100%);padding:40px 48px;text-align:center;">
            <div style="display:inline-block;background:rgba(255,255,255,0.15);border-radius:50%;padding:16px;margin-bottom:16px;">
              <span style="font-size:36px;">🔐</span>
            </div>
            <h1 style="margin:0;color:#ffffff;font-size:26px;font-weight:700;letter-spacing:0.5px;">Password Reset</h1>
            <p style="margin:8px 0 0;color:rgba(255,255,255,0.8);font-size:14px;">Sendzyy — WhatsApp Business Platform</p>
          </td>
        </tr>

        <!-- Body -->
        <tr>
          <td style="padding:40px 48px;">
            <p style="margin:0 0 8px;color:#555;font-size:14px;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Hello,</p>
            <h2 style="margin:0 0 20px;color:#1B5E20;font-size:22px;font-weight:700;">${tenant.name} 👋</h2>
            <p style="margin:0 0 24px;color:#444;font-size:15px;line-height:1.7;">
              We received a request to reset your <strong>Sendzyy</strong> account password. Here is your new temporary password — please keep it safe.
            </p>

            <!-- Password Box -->
            <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
              <tr>
                <td style="background:#f8fdf8;border:2px dashed #4CAF50;border-radius:12px;padding:24px;text-align:center;">
                  <p style="margin:0 0 8px;color:#888;font-size:12px;text-transform:uppercase;letter-spacing:1.5px;font-weight:600;">Your New Password</p>
                  <p style="margin:0;color:#1B5E20;font-size:28px;font-weight:800;letter-spacing:4px;font-family:'Courier New',monospace;">${newPassword}</p>
                </td>
              </tr>
            </table>

            <!-- Steps -->
            <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:32px;">
              <tr>
                <td style="background:#f9f9f9;border-radius:10px;padding:20px 24px;">
                  <p style="margin:0 0 12px;color:#333;font-size:14px;font-weight:700;">📋 Next Steps:</p>
                  <table cellpadding="0" cellspacing="0">
                    <tr>
                      <td style="padding:4px 0;color:#555;font-size:14px;">
                        <span style="color:#4CAF50;font-weight:700;margin-right:8px;">1.</span> Click the button below to go to the login page
                      </td>
                    </tr>
                    <tr>
                      <td style="padding:4px 0;color:#555;font-size:14px;">
                        <span style="color:#4CAF50;font-weight:700;margin-right:8px;">2.</span> Sign in using your email and the password above
                      </td>
                    </tr>
                    <tr>
                      <td style="padding:4px 0;color:#555;font-size:14px;">
                        <span style="color:#4CAF50;font-weight:700;margin-right:8px;">3.</span> Go to <strong>Settings → Security & Password</strong> to set a new one
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>

            <!-- CTA Button -->
            <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:32px;">
              <tr>
                <td align="center">
                  <a href="https://app.sendzyy.com" target="_blank"
                     style="display:inline-block;background:linear-gradient(135deg,#2E7D32,#43A047);color:#ffffff;text-decoration:none;font-size:16px;font-weight:700;padding:16px 48px;border-radius:50px;letter-spacing:0.5px;box-shadow:0 4px 14px rgba(46,125,50,0.4);">
                    🚀 &nbsp; Click to Login
                  </a>
                </td>
              </tr>
              <tr>
                <td align="center" style="padding-top:12px;">
                  <p style="margin:0;color:#aaa;font-size:12px;">Or copy this link: <a href="https://app.sendzyy.com" style="color:#2E7D32;">https://app.sendzyy.com</a></p>
                </td>
              </tr>
            </table>

            <!-- Warning -->
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td style="background:#fff8e1;border-left:4px solid #FFC107;border-radius:0 8px 8px 0;padding:14px 18px;">
                  <p style="margin:0;color:#795548;font-size:13px;line-height:1.6;">
                    ⚠️ <strong>Didn't request this?</strong> If you did not request a password reset, please ignore this email or contact our support team immediately. Your account may be at risk.
                  </p>
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="background:#f8f8f8;border-top:1px solid #eee;padding:28px 48px;text-align:center;">
            <p style="margin:0 0 6px;color:#1B5E20;font-size:15px;font-weight:700;">Sendzyy</p>
            <p style="margin:0 0 12px;color:#999;font-size:12px;">WhatsApp Business Messaging Platform</p>
            <p style="margin:0;color:#bbb;font-size:11px;">
              ©© ${new Date().getFullYear()} Sendzyy · All rights reserved<br/>
              <a href="https://app.sendzyy.com" style="color:#4CAF50;text-decoration:none;">app.sendzyy.com</a>
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>`,
        });

        res.json({ success: true });
    } catch (err) {
        console.error(' Forgot password error:', err.message);
        res.status(500).json({ error: 'Failed to send reset email' });
    }
});

//  Clients 
app.post('/api/clients', authenticate, async (req, res) => {
    try {
        const { name, mobileNumber, companyName, emailId, venue, remark } = req.body;
        if (!name || !mobileNumber) return res.status(400).json({ error: 'Name and Mobile Number are required' });
        const finalVenue = (venue && typeof venue === 'string' && venue.trim() !== '') ? venue.trim() : '-';
        const existing = await Client.findOne({ tenantId: req.user.tenantId, mobileNumber });
        if (existing) return res.status(400).json({ error: 'A client with this mobile number already exists.' });
        const client = await Client.create({ tenantId: req.user.tenantId, name, mobileNumber, companyName, emailId, venue: finalVenue, remark });
        setImmediate(() => evaluateClientTrigger(req.user.tenantId, client).catch(console.error));
        res.status(201).json(client);
    } catch (error) {
        res.status(500).json({ error: 'Failed to create client' });
    }
});

app.post('/api/clients/bulk', authenticate, async (req, res) => {
    try {
        const clients = req.body;
        if (!Array.isArray(clients) || clients.length === 0)
            return res.status(400).json({ error: 'Expected a non-empty array of clients' });

        const tenantId = req.user.tenantId;
        const results = { imported: [], skipped: [] };

        for (const { name, mobileNumber, companyName, emailId, venue, remark } of clients) {
            if (!name || !mobileNumber) { results.skipped.push({ mobileNumber, reason: 'Missing name or mobile' }); continue; }
            const exists = await Client.findOne({ tenantId, mobileNumber });
            if (exists) { results.skipped.push({ mobileNumber, reason: 'Duplicate' }); continue; }
            const finalVenue = (venue && typeof venue === 'string' && venue.trim() !== '') ? venue.trim() : '-';
            const created = await Client.create({ tenantId, name, mobileNumber, companyName, emailId, venue: finalVenue, remark });
            setImmediate(() => evaluateClientTrigger(tenantId, created).catch(console.error));
            results.imported.push(created);
        }

        res.status(201).json(results.imported);
    } catch (error) {
        res.status(500).json({ error: 'Bulk import failed' });
    }
});

app.post('/api/clients/bulk-resolve', authenticate, async (req, res) => {
    try {
        const clients = req.body;
        if (!Array.isArray(clients) || clients.length === 0)
            return res.status(400).json({ error: 'Expected a non-empty array of clients' });

        const tenantId = req.user.tenantId;
        const resolvedClients = [];
        const processedMobiles = new Set();

        for (const { name, mobileNumber, companyName, emailId, venue, remark } of clients) {
            if (!name || !mobileNumber) continue;
            if (processedMobiles.has(mobileNumber)) continue;
            processedMobiles.add(mobileNumber);

            let client = await Client.findOne({ tenantId, mobileNumber });
            if (!client) {
                const finalVenue = (venue && typeof venue === 'string' && venue.trim() !== '') ? venue.trim() : '-';
                client = await Client.create({
                    tenantId,
                    name,
                    mobileNumber,
                    companyName,
                    emailId,
                    venue: finalVenue,
                    remark
                });
                setImmediate(() => evaluateClientTrigger(tenantId, client).catch(console.error));
            }
            resolvedClients.push(client);
        }

        res.status(200).json(resolvedClients);
    } catch (error) {
        console.error('[server] POST /api/clients/bulk-resolve error:', error);
        res.status(500).json({ error: 'Bulk resolve failed' });
    }
});

app.get('/api/clients', authenticate, async (req, res) => {
    try {
        const search = req.query.search?.trim() || '';
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 50;
        const skip = (page - 1) * limit;

        const filter = { tenantId: req.user.tenantId };
        if (search) {
            filter.$or = [
                { name: { $regex: search, $options: 'i' } },
                { mobileNumber: { $regex: search, $options: 'i' } },
                { companyName: { $regex: search, $options: 'i' } },
            ];
        }

        // Support filtering by groupId
        if (req.query.groupId) {
            const group = await Group.findOne({ _id: req.query.groupId, tenantId: req.user.tenantId });
            if (group) {
                filter._id = { $in: group.clientIds };
            } else {
                return res.json({ clients: [], total: 0, page: 1, totalPages: 0 });
            }
        }

        // Support filtering by a list of ids
        if (req.query.ids) {
            const ids = req.query.ids.split(',').filter(id => id.trim() !== '');
            filter._id = { $in: ids };
        }

        // If filtering by groupId or list of ids and no page is explicitly requested, skip pagination
        const isTargetedQuery = req.query.groupId || req.query.ids;
        const total = await Client.countDocuments(filter);

        let clients;
        if (isTargetedQuery && !req.query.page) {
            clients = await Client.find(filter).sort({ name: 1 });
        } else {
            clients = await Client.find(filter)
                .sort({ name: 1 })
                .skip(skip)
                .limit(limit);
        }

        res.json({
            clients,
            total,
            page,
            totalPages: Math.ceil(total / limit)
        });
    } catch (error) {
        console.error('[server] GET /api/clients error:', error);
        res.status(500).json({ error: 'Failed to fetch clients' });
    }
});

// Client Trigger CRUD — must be before /api/clients/:id wildcard
app.post('/api/clients/trigger', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const { templateName, templateLanguage, mediaId, mediaType, variableMapping, isActive } = req.body;
        if (!templateName || !templateName.trim()) {
            return res.status(400).json({ error: 'templateName is required' });
        }
        const trigger = await ClientTrigger.findOneAndUpdate(
            { tenantId },
            { tenantId, templateName: templateName.trim(), templateLanguage, mediaId: mediaId || '', mediaType: mediaType || '', variableMapping: variableMapping || {}, isActive },
            { upsert: true, new: true, setDefaultsOnInsert: true }
        );
        res.status(201).json(trigger);
    } catch (error) {
        res.status(500).json({ error: 'Failed to save client trigger' });
    }
});

app.get('/api/clients/trigger', authenticate, async (req, res) => {
    try {
        const trigger = await ClientTrigger.findOne({ tenantId: req.user.tenantId });
        if (!trigger) return res.status(404).json({ error: 'No client trigger found' });
        res.json(trigger);
    } catch (error) {
        res.status(500).json({ error: 'Failed to fetch client trigger' });
    }
});

app.put('/api/clients/trigger', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const trigger = await ClientTrigger.findOne({ tenantId });
        if (!trigger) return res.status(404).json({ error: 'No client trigger found' });
        const { templateName, templateLanguage, mediaId, mediaType, variableMapping, isActive } = req.body;
        if (templateName !== undefined && !templateName.trim()) {
            return res.status(400).json({ error: 'templateName is required' });
        }
        if (templateName !== undefined) trigger.templateName = templateName.trim();
        if (templateLanguage !== undefined) trigger.templateLanguage = templateLanguage;
        if (mediaId !== undefined) trigger.mediaId = mediaId;
        if (mediaType !== undefined) trigger.mediaType = mediaType;
        if (variableMapping !== undefined) trigger.variableMapping = variableMapping;
        if (isActive !== undefined) trigger.isActive = isActive;
        await trigger.save();
        res.json(trigger);
    } catch (error) {
        res.status(500).json({ error: 'Failed to update client trigger' });
    }
});

app.delete('/api/clients/trigger', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const trigger = await ClientTrigger.findOne({ tenantId });
        if (!trigger) return res.status(404).json({ error: 'No client trigger found' });
        await trigger.deleteOne();
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: 'Failed to delete client trigger' });
    }
});

app.delete('/api/clients/:id', authenticate, async (req, res) => {
    try {
        const client = await Client.findOneAndDelete({ _id: req.params.id, tenantId: req.user.tenantId });
        if (!client) return res.status(404).json({ error: 'Client not found' });
        res.json({ success: true, message: 'Client deleted successfully' });
    } catch (error) {
        res.status(500).json({ error: 'Failed to delete client' });
    }
});

// Public client registration form (QR code landing page)
app.get('/register-client', async (req, res) => {
    const { tenantId, groupId } = req.query;
    if (!tenantId) return res.status(400).send('Invalid link');
    const tenant = await Tenant.findById(tenantId).catch(() => null);
    if (!tenant) return res.status(404).send('Not found');
    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Register - ${tenant.name}</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f0f4f8;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:16px}
  .card{background:#fff;border-radius:16px;padding:32px 28px;width:100%;max-width:420px;box-shadow:0 4px 24px rgba(0,0,0,.1)}
  h2{color:#1a5c4a;font-size:22px;margin-bottom:6px}
  .sub{color:#666;font-size:14px;margin-bottom:24px}
  label{display:block;font-size:13px;font-weight:600;color:#444;margin-bottom:6px}
  input,textarea{width:100%;padding:12px 14px;border:1.5px solid #ddd;border-radius:10px;font-size:15px;outline:none;transition:border .2s;font-family:inherit;resize:vertical}
  input:focus,textarea:focus{border-color:#25d366}
  .field{margin-bottom:16px}
  .req{color:#e53e3e}
  button{width:100%;padding:14px;background:#25d366;color:#fff;border:none;border-radius:10px;font-size:16px;font-weight:600;cursor:pointer;margin-top:8px;transition:background .2s}
  button:hover{background:#1ebe5d}
  button:disabled{background:#aaa;cursor:not-allowed}
  .msg{margin-top:16px;padding:12px;border-radius:8px;font-size:14px;text-align:center;display:none}
  .msg.success{background:#e6f9ee;color:#1a5c4a;display:block}
  .msg.error{background:#fde8e8;color:#c53030;display:block}
</style>
</head>
<body>
<div class="card">
  <h2>${tenant.name}</h2>
  <p class="sub">Please fill in your details to register</p>
  <form id="form">
    <input type="hidden" id="groupId" value="${groupId || ''}"/>
    <div class="field"><label>Name <span class="req">*</span></label><input id="name" placeholder="Your full name" required/></div>
    <div class="field"><label>Mobile Number <span class="req">*</span></label><input id="mobile" placeholder="Your mobile number" required/></div>
    <div class="field"><label>Company Name</label><input id="company" placeholder="Optional"/></div>
    <div class="field"><label>Email ID</label><input id="email" type="email" placeholder="Optional"/></div>
    <div class="field"><label>Venue <span class="req">*</span></label><input id="venue" placeholder="e.g. Main Street Store" required/></div>
    <div class="field"><label>Remark</label><textarea id="remark" rows="2" placeholder="Optional"></textarea></div>
    <button type="submit" id="btn">Submit</button>
  </form>
  <div class="msg" id="msg"></div>
</div>
<script>
document.getElementById('form').addEventListener('submit',async function(e){
  e.preventDefault();
  const btn=document.getElementById('btn');
  const msg=document.getElementById('msg');

  // Get mobile number (no validation)
  const mobile=document.getElementById('mobile').value.trim();
  if(!mobile){
    msg.className='msg error';msg.textContent='Mobile number is required';return;
  }

  // Get email (no validation)
  const email=document.getElementById('email').value.trim();

  btn.disabled=true; btn.textContent='Submitting...';
  msg.className='msg'; msg.textContent='';
  const body={name:document.getElementById('name').value.trim(),mobileNumber:mobile,companyName:document.getElementById('company').value.trim()||undefined,emailId:email||undefined,venue:document.getElementById('venue').value.trim()||undefined,remark:document.getElementById('remark').value.trim()||undefined,groupId:document.getElementById('groupId').value||undefined};
  try{
    const r=await fetch('/api/clients/public/${tenantId}',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await r.json();
    if(r.ok){msg.className='msg success';msg.textContent='Registered successfully! Thank you.';document.getElementById('form').reset();}
    else{msg.className='msg error';msg.textContent=d.error||'Something went wrong';}
  }catch(err){msg.className='msg error';msg.textContent='Network error. Please try again.';}
  btn.disabled=false; btn.textContent='Submit';
});
</script>
</body>
</html>`);
});

// Public client registration API (no auth — used by QR form)
app.post('/api/clients/public/:tenantId', async (req, res) => {
    try {
        const { tenantId } = req.params;
        const { name, mobileNumber, companyName, emailId, venue, remark, groupId } = req.body;
        if (!name || !mobileNumber) return res.status(400).json({ error: 'Name and Mobile Number are required' });
        if (!venue) return res.status(400).json({ error: 'Venue is required' });
        const tenant = await Tenant.findById(tenantId).catch(() => null);
        if (!tenant) return res.status(404).json({ error: 'Invalid registration link' });
        const existing = await Client.findOne({ tenantId, mobileNumber });
        if (existing) {
            // If client exists and groupId provided, still add to group if not already a member
            if (groupId) {
                const group = await Group.findOne({ _id: groupId, tenantId }).catch(() => null);
                if (group && !group.clientIds.includes(existing._id.toString())) {
                    group.clientIds.push(existing._id.toString());
                    await group.save();
                }
                return res.status(200).json(existing);
            }
            return res.status(400).json({ error: 'A client with this mobile number already exists.' });
        }
        const client = await Client.create({ tenantId, name, mobileNumber, companyName, emailId, venue, remark });
        // Add to group if groupId was provided
        if (groupId) {
            const group = await Group.findOne({ _id: groupId, tenantId }).catch(() => null);
            if (group) {
                group.clientIds.push(client._id.toString());
                await group.save();
            }
        }
        setImmediate(() => evaluateClientTrigger(tenantId, client).catch(console.error));
        res.status(201).json(client);
    } catch (error) {
        res.status(500).json({ error: 'Failed to register client' });
    }
});

//  Chatbots 
app.post('/api/chatbots', authenticate, async (req, res) => {
    const { name, triggerKeywords, flow } = req.body;
    if (!name || !flow) {
        return res.status(400).json({ error: 'Missing required fields: name and flow are required' });
    }
    try {
        const chatbot = await Chatbot.create({
            tenantId: req.user.tenantId,
            name,
            triggerKeywords: triggerKeywords || [],
            flow,
        });
        res.status(201).json(chatbot);
    } catch (error) {
        console.error(' Create Chatbot Error:', error);
        res.status(500).json({ error: 'Failed to create chatbot' });
    }
});

app.get('/api/chatbots', authenticate, async (req, res) => {
    try {
        const chatbots = await Chatbot.find({ tenantId: req.user.tenantId }).sort({ updatedAt: -1 });
        res.json(chatbots);
    } catch (error) {
        console.error(' Get Chatbots Error:', error);
        res.status(500).json({ error: 'Failed to fetch chatbots' });
    }
});

app.get('/api/chatbots/:id', authenticate, async (req, res) => {
    try {
        const chatbot = await Chatbot.findById(req.params.id);
        if (!chatbot) return res.status(404).json({ error: 'Chatbot not found' });
        if (chatbot.tenantId !== req.user.tenantId) return res.status(403).json({ error: 'Forbidden' });
        res.json(chatbot);
    } catch (error) {
        console.error(' Get Chatbot Error:', error);
        res.status(500).json({ error: 'Failed to fetch chatbot' });
    }
});

app.put('/api/chatbots/:id', authenticate, async (req, res) => {
    try {
        const chatbot = await Chatbot.findById(req.params.id);
        if (!chatbot) return res.status(404).json({ error: 'Chatbot not found' });
        if (chatbot.tenantId !== req.user.tenantId) return res.status(403).json({ error: 'Forbidden' });

        const { name, triggerKeywords, flow, isActive } = req.body;

        // 409 conflict check: activating and a duplicate trigger keyword exists across other active bots
        if (isActive === true) {
            const newKeywords = (triggerKeywords ?? chatbot.triggerKeywords).map(k => k.toLowerCase());
            if (newKeywords.length > 0) {
                const otherActiveBots = await Chatbot.find({
                    tenantId: req.user.tenantId,
                    isActive: true,
                    _id: { $ne: chatbot._id },
                });
                const hasConflict = otherActiveBots.some(bot =>
                    bot.triggerKeywords.some(k => newKeywords.includes(k.toLowerCase()))
                );
                if (hasConflict) {
                    return res.status(409).json({ error: 'Trigger keyword conflict with another active chatbot' });
                }
            }
        }

        // When deactivating: delete all sessions for this chatbot (Req 10.3)
        if (isActive === false && chatbot.isActive === true) {
            await ChatbotSession.deleteMany({ chatbotId: chatbot._id.toString() });
        }

        // Apply allowed field updates
        if (name !== undefined) chatbot.name = name;
        if (triggerKeywords !== undefined) chatbot.triggerKeywords = triggerKeywords;
        if (flow !== undefined) chatbot.flow = flow;
        if (isActive !== undefined) chatbot.isActive = isActive;

        await chatbot.save();
        res.json(chatbot);
    } catch (error) {
        console.error(' Update Chatbot Error:', error);
        res.status(500).json({ error: 'Failed to update chatbot' });
    }
});

app.delete('/api/chatbots/sessions/:contactId', authenticate, async (req, res) => {
    try {
        await ChatbotSession.deleteOne({ tenantId: req.user.tenantId, contactId: req.params.contactId });
        res.json({ message: 'Session reset successfully' });
    } catch (error) {
        console.error(' Reset Session Error:', error);
        res.status(500).json({ error: 'Failed to reset session' });
    }
});

// Task 9.1 — Analytics endpoint
app.get('/api/chatbots/:id/analytics', authenticate, async (req, res) => {
    try {
        const chatbot = await Chatbot.findById(req.params.id);
        if (!chatbot) return res.status(404).json({ error: 'Chatbot not found' });
        if (chatbot.tenantId !== req.user.tenantId) return res.status(403).json({ error: 'Forbidden' });

        const days = Math.min(parseInt(req.query.days) || 7, 90);
        const since = new Date();
        since.setHours(0, 0, 0, 0);
        since.setDate(since.getDate() - (days - 1));

        const records = await ChatbotAnalytics.find({
            tenantId: req.user.tenantId,
            chatbotId: req.params.id,
            date: { $gte: since },
        }).sort({ date: 1 });

        res.json(records.map(r => ({
            date: r.date,
            totalSessions: r.totalSessions,
            completedSessions: r.completedSessions,
            droppedSessions: r.droppedSessions,
            messagesSent: r.messagesSent,
        })));
    } catch (error) {
        console.error(' Analytics Error:', error);
        res.status(500).json({ error: 'Failed to fetch analytics' });
    }
});

app.delete('/api/chatbots/:id', authenticate, async (req, res) => {
    try {
        const chatbot = await Chatbot.findById(req.params.id);
        if (!chatbot) return res.status(404).json({ error: 'Chatbot not found' });
        if (chatbot.tenantId !== req.user.tenantId) return res.status(403).json({ error: 'Forbidden' });

        await ChatbotSession.deleteMany({ chatbotId: chatbot._id.toString() });
        await chatbot.deleteOne();

        res.json({ message: 'Chatbot deleted successfully' });
    } catch (error) {
        console.error(' Delete Chatbot Error:', error);
        res.status(500).json({ error: 'Failed to delete chatbot' });
    }
});

//  Send Message 
app.post('/send-message', authenticate, async (req, res) => {
    const { to, type, template, text, mediaId, mediaType, campaignId } = req.body;
    const tenantId = req.user.tenantId;
    try {
        const tenant = await Tenant.findById(tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });

        const config = tenant.whatsappConfig;
        if (!config.accessToken || !config.phoneNumberId)
            return res.status(400).json({ error: 'WhatsApp not configured' });

        // Build payload
        const payload = { messaging_product: 'whatsapp', to, type: type || 'template' };
        if (type === 'text') {
            payload.text = { body: text };
        } else if (type === 'template') {
            payload.template = { name: template.name, language: template.language };
            if (template.components?.length > 0) {
                payload.template.components = template.components;
            } else if (mediaId) {
                payload.template.components = [{
                    type: 'header',
                    parameters: [{ type: (mediaType || 'image').toLowerCase(), [(mediaType || 'image').toLowerCase()]: { id: mediaId } }]
                }];
            }
        }

        const response = await axios.post(
            `${WHATSAPP_API_URL}/${config.phoneNumberId}/messages`,
            payload,
            { headers: { Authorization: `Bearer ${config.accessToken}` } }
        );
        const wamid = response.data.messages[0].id;

        // Create StatusMapping for ALL messages (campaign or not) to track delivery status
        try {
            await StatusMapping.findOneAndUpdate(
                { wamid },
                { wamid, tenantId, campaignId: campaignId || null, to },
                { upsert: true }
            );
        } catch (err) {
            console.error(' StatusMapping creation failed:', err.message);
        }

        // Campaign tracking
        if (campaignId) {
            try {
                // Check if this campaign document already exists
                const existingCampaign = await Campaign.findOne({ tenantId, id: campaignId });

                if (!existingCampaign) {
                    // First message for this campaign — capture the active retry config snapshot.
                    // Fall back to a zero-phase default if no config exists for this tenant yet,
                    // so the campaign is always created even if retry config is not set up.
                    let lifecycleFields;
                    try {
                        lifecycleFields = await campaignLifecycleManager.buildCampaignCreationFields(tenantId);
                    } catch (configErr) {
                        console.warn(` buildCampaignCreationFields failed, using default: ${configErr.message}`);
                        lifecycleFields = {
                            retryConfig: { version: 0, phases: [] },
                            status: 'initial',
                            currentPhase: 1,
                            phaseStats: [{ phaseNumber: 1, successCount: 0, failureCount: 0, executedAt: new Date() }]
                        };
                    }
                    await Campaign.create({
                        tenantId,
                        id: campaignId,
                        template: type === 'template' ? template.name : 'Text Message',
                        timestamp: new Date(),
                        dispatchedAt: new Date(),
                        totalCount: 1,
                        successCount: 1,
                        ...lifecycleFields
                    });
                } else {
                    await Campaign.findOneAndUpdate(
                        { tenantId, id: campaignId },
                        {
                            $set: { template: type === 'template' ? template.name : 'Text Message' },
                            $inc: { totalCount: 1, successCount: 1 }
                        }
                    );
                }

                await Recipient.findOneAndUpdate(
                    { wamid },
                    {
                        wamid,
                        tenantId,
                        campaignId,
                        to,
                        status: 'sent',
                        sentAt: new Date().toISOString(),
                        phaseNumber: null,
                        deliveryTimestamp: null,
                        retryHistory: [{
                            phaseNumber: 1,
                            attemptedAt: new Date(),
                            status: 'sent'
                        }]
                    },
                    { upsert: true }
                );
                await broadcastCampaigns(tenantId);
            } catch (err) {
                console.error(' Campaign tracking failed:', err.message);
            }
        }

        // Log outbound chat message
        try {
            let outboundMessageType = type === 'text' ? 'text' : 'template';
            let outboundTemplateName = null;
            let outboundTemplateBody = null;

            if (type === 'template') {
                outboundTemplateName = template.name;
                // Try to extract the resolved body text from the template components
                try {
                    const tplComponents = await fetchTemplateComponents(tenant, template.name);
                    const bodyComponent = (tplComponents || []).find(c => c.type === 'BODY');
                    outboundTemplateBody = bodyComponent?.text || null;
                } catch (_) {
                    outboundTemplateBody = null;
                }
            }

            const previewOpts = {
                text: type === 'text' ? text : undefined,
                templateName: outboundTemplateName,
                templateBody: outboundTemplateBody,
            };
            const msgPreview = buildPreview(outboundMessageType, previewOpts);

            await Conversation.findOneAndUpdate(
                { tenantId, contactId: to },
                { lastMessage: msgPreview, lastActive: new Date(), $setOnInsert: { hasReply: false } },
                { upsert: true }
            );
            await Message.create({
                tenantId,
                contactId: to,
                text: type === 'text' ? text : (outboundTemplateBody || ''),
                isMe: true,
                time: new Date().toISOString(),
                messageType: outboundMessageType,
                templateName: outboundTemplateName,
                templateBody: outboundTemplateBody,
            });
            await broadcastConversations(tenantId);
            await broadcastMessages(tenantId, to);
        } catch (err) {
            console.error(' Chat log failed:', err.message);
        }

        res.json({ success: true, wamid });
    } catch (error) {
        console.error(' Send Message Error:', error.response?.data || error.message);
        res.status(500).json({ error: 'Failed to send message' });
    }
});

//  Campaigns & Reports 
app.get('/campaigns', authenticate, async (req, res) => {
    try {
        const campaigns = await Campaign.find({ tenantId: req.user.tenantId })
            .sort({ timestamp: -1 });

        // Attach hasPendingRetry flag: true if any ScheduledRetryPhase is pending/executing for this campaign
        const campaignIds = campaigns.map(c => c.id);
        const pendingPhases = await ScheduledRetryPhase.find({
            campaignId: { $in: campaignIds },
            status: { $in: ['pending', 'executing'] }
        }).select('campaignId').lean();
        const pendingSet = new Set(pendingPhases.map(p => p.campaignId));

        const enriched = campaigns.map(c => ({
            ...c.toObject(),
            hasPendingRetry: pendingSet.has(c.id),
        }));

        res.json({ campaigns: enriched });
    } catch (error) {
        res.status(500).json({ error: 'Failed to fetch campaigns' });
    }
});

// ── Campaign Lifecycle Endpoints ──────────────────────────────────────────────

/**
 * POST /api/campaigns/execute
 *
 * Create a campaign document and immediately kick off the server-side send loop.
 * The loop runs fire-and-forget — the HTTP response returns as soon as the Campaign
 * document is created, so the client is never blocked waiting for all messages to send.
 *
 * Authorization: Bearer <jwt>
 * Body: { template, language, recipients, mediaId?, mediaType? }
 * Response 200: { campaignId }
 *
 * Requirements: 2.4
 */
app.post('/api/campaigns/execute', authenticate, async (req, res) => {
    const tenantId = req.user.tenantId;
    const { template, language, recipients, mediaId, mediaType } = req.body;

    if (!template || !language || !recipients || !Array.isArray(recipients) || recipients.length === 0) {
        return res.status(400).json({ error: 'Missing required fields: template, language, recipients' });
    }

    try {
        // Build campaign creation fields (captures retry config snapshot)
        const lifecycleFields = await campaignLifecycleManager.buildCampaignCreationFields(tenantId);

        const campaignId = `campaign_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;

        await Campaign.create({
            tenantId,
            id: campaignId,
            template,
            timestamp: new Date(),
            dispatchedAt: new Date(),
            totalCount: recipients.length,
            successCount: 0,
            failureCount: 0,
            ...lifecycleFields
        });

        console.log(JSON.stringify({
            service: 'POST /api/campaigns/execute',
            event: 'campaign_created',
            campaignId,
            tenantId,
            totalCount: recipients.length,
            timestamp: new Date().toISOString()
        }));

        // Fire and forget — do NOT await; attach a .catch that logs only
        campaignExecutor.executeCampaign(campaignId, tenantId, recipients, template, language, mediaId, mediaType)
            .catch(err => console.error(JSON.stringify({
                service: 'POST /api/campaigns/execute',
                event: 'campaign_executor_error',
                campaignId,
                tenantId,
                error: err.message,
                timestamp: new Date().toISOString()
            })));

        // Immediately return the campaignId — loop is running on the server
        res.json({ campaignId });
    } catch (err) {
        console.error(`POST /api/campaigns/execute error: ${err.message}`);
        res.status(500).json({ error: 'Failed to start campaign execution' });
    }
});

/**
 * POST /api/campaigns/:campaignId/complete-phase1
 *
 * Schedule the phase 1 evaluation for a campaign.
 * Rather than evaluating immediately (before delivery webhooks arrive),
 * this schedules a ScheduledRetryPhase with phaseNumber=1 at
 * now + phase1IntervalHours. The scheduler will then call handlePhase1Completion
 * after the grace period, at which point any remaining 'sent' messages are
 * genuinely undelivered and should be retried.
 *
 * Requirements: 2.4, 2.5, 3.1
 */
app.post('/api/campaigns/:campaignId/complete-phase1', authenticate, async (req, res) => {
    const { campaignId } = req.params;
    const tenantId = req.user.tenantId;
    try {
        // Check if the campaign exists — if all sends failed, it may not have been created
        let campaign = await Campaign.findOne({ tenantId, id: campaignId });
        if (!campaign) {
            // All initial sends failed — no Campaign document was created (the document is
            // only created on the first successful send). Create a minimal campaign document
            // so that phase-1 evaluation can run and failed Recipient documents can be retried.
            let lifecycleFields;
            try {
                lifecycleFields = await campaignLifecycleManager.buildCampaignCreationFields(tenantId);
            } catch (configErr) {
                console.warn(
                    `[complete-phase1] buildCampaignCreationFields failed for tenant ${tenantId}: ` +
                    configErr.message + ' — using zero-phase fallback'
                );
                lifecycleFields = {
                    retryConfig: { version: 0, phases: [] },
                    status: 'initial',
                    currentPhase: 1,
                    phaseStats: [{
                        phaseNumber: 1,
                        successCount: 0,
                        failureCount: 0,
                        executedAt: new Date()
                    }]
                };
            }
            campaign = await Campaign.create({
                tenantId,
                id: campaignId,
                template: '',
                timestamp: new Date(),
                dispatchedAt: new Date(),
                totalCount: 0,
                successCount: 0,
                failureCount: 0,
                ...lifecycleFields
            });
            console.log(JSON.stringify({
                service: 'complete-phase1',
                event: 'minimal_campaign_created',
                campaignId,
                tenantId,
                reason: 'all_initial_sends_failed',
                timestamp: new Date().toISOString()
            }));
        }

        // Schedule phase 1 evaluation after the grace period (first retry interval).
        // schedulePhase returns null when no retry phases are configured (Bug 3 fix).
        const scheduledPhase = await retryScheduler.schedulePhase(campaignId, 1, new Date());
        if (!scheduledPhase) {
            // No retry phases configured — campaign will complete without retries
            return res.json({ status: 'skipped', reason: 'no_retry_phases_configured' });
        }
        await broadcastCampaigns(tenantId);
        res.json({ status: 'scheduled', scheduledAt: scheduledPhase.scheduledAt });
    } catch (err) {
        console.error(`POST /api/campaigns/${campaignId}/complete-phase1 error:`, err.message);
        if (err.message.includes('not found')) return res.status(404).json({ error: err.message });
        res.status(500).json({ error: 'Failed to schedule phase 1 evaluation' });
    }
});

/**
 * POST /api/campaigns/:campaignId/complete
 *
 * Explicitly complete a campaign (e.g. after the final retry phase).
 * Cancels pending phases, marks remaining failed messages, stores final stats.
 *
 * Requirements: 5.1, 5.2, 5.3, 5.4, 5.5
 */
app.post('/api/campaigns/:campaignId/complete', authenticate, async (req, res) => {
    const { campaignId } = req.params;
    const tenantId = req.user.tenantId;
    try {
        const result = await campaignLifecycleManager.completeCampaign(campaignId, tenantId);
        await broadcastCampaigns(tenantId);
        res.json(result);
    } catch (err) {
        console.error(`POST /api/campaigns/${campaignId}/complete error:`, err.message);
        if (err.message.includes('not found')) return res.status(404).json({ error: err.message });
        res.status(500).json({ error: 'Failed to complete campaign' });
    }
});

// ── Reporting Endpoints ────────────────────────────────────────────────────────

/**
 * GET /api/campaigns/:campaignId/report
 *
 * Retrieve phase-wise delivery report for a campaign.
 * Returns phase statistics with success rates, cumulative metrics,
 * and overall success rate. Includes scheduled phases with pending status.
 *
 * Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7
 */
app.get('/api/campaigns/:campaignId/report', authenticate, async (req, res) => {
    const { campaignId } = req.params;
    const tenantId = req.user.tenantId;
    try {
        const report = await reportGenerator.generatePhaseReport(campaignId, tenantId);
        res.json(report);
    } catch (err) {
        console.error(`GET /api/campaigns/${campaignId}/report error:`, err.message);
        if (err.message.includes('not found')) return res.status(404).json({ error: err.message });
        res.status(500).json({ error: 'Failed to generate campaign report' });
    }
});

/**
 * GET /api/campaigns/:campaignId/messages
 *
 * Retrieve messages for a campaign, optionally filtered by phase number.
 * Supports pagination via page and limit query parameters.
 *
 * Query params:
 *   - phaseNumber (optional): Filter messages by delivery phase
 *   - page (optional, default 1): Page number
 *   - limit (optional, default 50): Items per page
 *
 * Requirements: 6.4
 */
app.get('/api/campaigns/:campaignId/messages', authenticate, async (req, res) => {
    const { campaignId } = req.params;
    const tenantId = req.user.tenantId;
    try {
        // Parse optional phaseNumber query param
        let phaseNumber = null;
        if (req.query.phaseNumber !== undefined) {
            const parsed = parseInt(req.query.phaseNumber, 10);
            if (isNaN(parsed) || parsed < 1) {
                return res.status(400).json({ error: 'phaseNumber must be a positive integer' });
            }
            phaseNumber = parsed;
        }

        // Parse pagination params
        const page = req.query.page ? parseInt(req.query.page, 10) : 1;
        const limit = req.query.limit ? parseInt(req.query.limit, 10) : 50;

        if (isNaN(page) || page < 1) {
            return res.status(400).json({ error: 'page must be a positive integer' });
        }
        if (isNaN(limit) || limit < 1 || limit > 1000) {
            return res.status(400).json({ error: 'limit must be between 1 and 1000' });
        }

        const result = await reportGenerator.getMessagesByPhase(
            campaignId,
            tenantId,
            phaseNumber,
            page,
            limit
        );

        res.json(result);
    } catch (err) {
        console.error(`GET /api/campaigns/${campaignId}/messages error:`, err.message);
        if (err.message.includes('not found')) return res.status(404).json({ error: err.message });
        res.status(500).json({ error: 'Failed to fetch campaign messages' });
    }
});

// ── Retry Configuration Endpoints ─────────────────────────────────────────────

/**
 * POST /api/retry-config
 * Create a new retry configuration version.
 * Requirements: 1.1, 1.2, 1.3, 1.5
 */
app.post('/api/retry-config', authenticate, async (req, res) => {
    try {
        const { phases } = req.body;
        const tenantId = req.user.tenantId;
        if (!phases || !Array.isArray(phases)) {
            return res.status(400).json({ error: 'phases array is required' });
        }

        const newConfig = await configManager.createConfiguration({ phases }, tenantId);

        const previousConfig = await RetryConfiguration.findOne({ tenantId, version: newConfig.version - 1 });
        let affectedCampaigns = 0;
        if (previousConfig) {
            affectedCampaigns = await configManager.getActiveCampaignsCount(previousConfig.version, Campaign, tenantId);
        }

        res.status(201).json({
            version: newConfig.version,
            phases: newConfig.phases,
            createdAt: newConfig.createdAt,
            affectedCampaigns
        });
    } catch (err) {
        console.error('POST /api/retry-config error:', err);
        if (err.message === 'Configuration validation failed') {
            return res.status(400).json({ error: 'Validation failed', details: err.validationErrors });
        }
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

/**
 * GET /api/retry-config/current
 * Get the currently active retry configuration.
 * Requirements: 8.5
 */
app.get('/api/retry-config/current', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const activeConfig = await configManager.getActiveConfiguration(tenantId);
        if (!activeConfig) return res.status(404).json({ error: 'No active configuration found' });

        const activeCampaignsCount = await configManager.getActiveCampaignsCount(activeConfig.version, Campaign, tenantId);
        res.json({
            version: activeConfig.version,
            phases: activeConfig.phases,
            isActive: activeConfig.isActive,
            createdAt: activeConfig.createdAt,
            activeCampaignsCount
        });
    } catch (err) {
        console.error('GET /api/retry-config/current error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

/**
 * GET /api/retry-config/history
 * List all configuration versions with active campaign counts.
 * Requirements: 8.5
 */
app.get('/api/retry-config/history', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const history = await configManager.getConfigurationHistory(Campaign, tenantId);
        res.json({
            configs: history.map(c => ({
                version: c.version,
                phases: c.phases,
                createdAt: c.createdAt,
                isActive: c.isActive,
                activeCampaigns: c.activeCampaigns
            }))
        });
    } catch (err) {
        console.error('GET /api/retry-config/history error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

app.get('/campaign-analytics', authenticate, async (req, res) => {
    try {
        const tenant = await Tenant.findById(req.user.tenantId).select('whatsappConfig');
        if (!tenant?.whatsappConfig?.businessAccountId)
            return res.status(400).json({ error: 'WABA ID not configured' });

        const { start, end } = req.query;
        const { businessAccountId, accessToken } = tenant.whatsappConfig;

        // Use the Graph API base directly — analytics is a WABA-level endpoint
        const url = `${WHATSAPP_API_URL}/${businessAccountId}/analytics`;

        const response = await axios.get(url, {
            params: {
                start,
                end,
                granularity: 'DAILY',
                // Meta expects repeated params: metric_types[]=SENT&metric_types[]=DELIVERED
                // axios serializes arrays correctly when passed as an array
                'metric_types[]': ['SENT', 'DELIVERED', 'READ'],
                access_token: accessToken,
            },
            // Tell axios not to encode [] in param names
            paramsSerializer: (params) => {
                const parts = [];
                for (const [key, val] of Object.entries(params)) {
                    if (Array.isArray(val)) {
                        val.forEach(v => parts.push(`${encodeURIComponent(key)}=${encodeURIComponent(v)}`));
                    } else {
                        parts.push(`${encodeURIComponent(key)}=${encodeURIComponent(val)}`);
                    }
                }
                return parts.join('&');
            },
        });

        const data = response.data?.data || [];
        const totals = { sent: 0, delivered: 0, read: 0 };
        for (const metric of data) {
            const sum = (metric.data_points || []).reduce((acc, p) => acc + (p.value || 0), 0);
            if (metric.type === 'SENT') totals.sent = sum;
            if (metric.type === 'DELIVERED') totals.delivered = sum;
            if (metric.type === 'READ') totals.read = sum;
        }

        res.json(totals);
    } catch (error) {
        const metaError = error.response?.data;
        console.error('/campaign-analytics error:', error.message, metaError ? JSON.stringify(metaError) : '');
        res.status(500).json({
            error: 'Failed to fetch analytics',
            detail: metaError || error.message,
        });
    }
});

//  Scheduled Campaigns 
app.post('/scheduled-campaigns', authenticate, async (req, res) => {
    try {
        const { campaignName, template, language, recipients, mediaId, mediaType, scheduledAt } = req.body;
        if (!template || !recipients?.length || !scheduledAt)
            return res.status(400).json({ error: 'template, recipients and scheduledAt are required' });
        const sc = await ScheduledCampaign.create({
            tenantId: req.user.tenantId,
            campaignName: campaignName || '',
            template, language: language || 'en_US',
            recipients, mediaId, mediaType,
            scheduledAt: new Date(scheduledAt),
        });
        res.json({ id: sc._id.toString(), scheduledAt: sc.scheduledAt });
    } catch (err) {
        console.error(' Schedule campaign error:', err.message);
        res.status(500).json({ error: 'Failed to schedule campaign' });
    }
});

app.get('/scheduled-campaigns', authenticate, async (req, res) => {
    try {
        const list = await ScheduledCampaign.find({ tenantId: req.user.tenantId }).sort({ scheduledAt: -1 });
        res.json(list.map(s => ({ ...s.toObject(), id: s._id.toString() })));
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch scheduled campaigns' });
    }
});

app.delete('/scheduled-campaigns/:id', authenticate, async (req, res) => {
    try {
        const sc = await ScheduledCampaign.findOneAndUpdate(
            { _id: req.params.id, tenantId: req.user.tenantId, status: 'pending' },
            { $set: { status: 'cancelled' } },
            { new: true }
        );
        if (!sc) return res.status(404).json({ error: 'Not found or already processed' });
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: 'Failed to cancel' });
    }
});

//  Conversations & Messages REST
app.get('/conversations', authenticate, async (req, res) => {
    try {
        const convs = await Conversation.find({ tenantId: req.user.tenantId, hasReply: true })
            .sort({ lastActive: -1 }).lean();
        res.json(convs.map(c => ({
            id: c.contactId,
            name: c.name || c.contactId,
            lastMessage: c.lastMessage || '',
            lastActive: c.lastActive,
            hasReply: c.hasReply || false,
        })));
    } catch (error) {
        res.status(500).json({ error: 'Failed to fetch conversations' });
    }
});

app.get('/conversations/:contactId/messages', authenticate, async (req, res) => {
    try {
        const messages = await Message.find({ tenantId: req.user.tenantId, contactId: req.params.contactId })
            .sort({ timestamp: 1 }).lean();
        res.json(messages.map(m => ({
            id: m._id.toString(),
            text: m.text || '',
            isMe: m.isMe === true,
            timestamp: m.timestamp,
            time: m.time || new Date(m.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
            messageType: m.messageType || null,
            templateName: m.templateName || null,
            templateBody: m.templateBody || null,
            mediaUrl: m.mediaUrl || null,
            interactivePayload: m.interactivePayload || null,
        })));
    } catch (error) {
        res.status(500).json({ error: 'Failed to fetch messages' });
    }
});

// Media proxy — fetches a WhatsApp media file from Meta and streams it to the client.
// Keeps the access token server-side so the Flutter client never needs it directly.
app.get('/media/:mediaId', authenticate, async (req, res) => {
    try {
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant?.whatsappConfig?.accessToken) {
            return res.status(400).json({ error: 'WhatsApp not configured' });
        }
        const { accessToken } = tenant.whatsappConfig;
        const { mediaId } = req.params;

        // Step 1: Retrieve the download URL from Meta
        let downloadUrl;
        try {
            const metaResp = await axios.get(
                `${WHATSAPP_API_URL}/${mediaId}`,
                { headers: { Authorization: `Bearer ${accessToken}` } }
            );
            downloadUrl = metaResp.data?.url;
            if (!downloadUrl) throw new Error('No URL in Meta response');
        } catch (err) {
            console.error(`[MediaProxy] Failed to get download URL for ${mediaId}:`, err.message);
            return res.status(404).json({ error: 'Media not found' });
        }

        // Step 2: Stream the binary to the client
        const mediaResp = await axios.get(downloadUrl, {
            headers: { Authorization: `Bearer ${accessToken}` },
            responseType: 'stream',
        });

        res.setHeader('Content-Type', mediaResp.headers['content-type'] || 'application/octet-stream');
        mediaResp.data.pipe(res);
    } catch (error) {
        console.error('[MediaProxy] Error:', error.message);
        res.status(404).json({ error: 'Media not found' });
    }
});

app.get('/campaigns/:campaignId/recipients', authenticate, async (req, res) => {
    try {
        const recipients = await Recipient.find({ tenantId: req.user.tenantId, campaignId: req.params.campaignId });
        res.json(recipients);
    } catch (error) {
        res.status(500).json({ error: 'Failed to fetch recipients' });
    }
});

app.post('/status-mappings', authenticate, async (req, res) => {
    try {
        const { wamid, campaignId } = req.body;
        await StatusMapping.findOneAndUpdate({ wamid }, { wamid, campaignId, tenantId: req.user.tenantId }, { upsert: true });
        res.sendStatus(200);
    } catch (error) {
        res.status(500).json({ error: 'Failed to save status mapping' });
    }
});


// ─────────────────────────────────────────────────────────────────────────────
//  META ONBOARDING HELPER — Steps 4 + 5 + 6
//  Called from:
//    1. POST /webhook when event === 'PARTNER_ADDED' (Step 3)
//    2. POST /facebook-embedded-signup as a synchronous fallback when
//       businessPortfolioId is available from the popup postMessage
//
//  @param {string} wabaId              - Customer WhatsApp Business Account ID
//  @param {string} businessPortfolioId - Customer Business Portfolio ID
//  @param {string|null} tenantId       - MongoDB _id of the tenant to update
//                                        (null when called from webhook)
// ─────────────────────────────────────────────────────────────────────────────
async function processOnboarding(wabaId, businessPortfolioId, tenantId, sessionId = null) {
    const tag = '[processOnboarding]';
    const systemToken = process.env.META_SYSTEM_TOKEN;
    const appSecret = process.env.META_APP_SECRET;
    const apiVersion = process.env.META_API_VERSION || 'v25.0';

    await saveOnboardingLog({
        tenantId,
        sessionId,
        wabaId,
        businessPortfolioId,
        step: 'ONBOARDING_PROCESS_START',
        status: 'info',
        message: 'processOnboarding task started.'
    });

    if (!systemToken || systemToken === 'PASTE_YOUR_SYSTEM_USER_TOKEN_HERE') {
        await saveOnboardingLog({
            tenantId,
            sessionId,
            wabaId,
            businessPortfolioId,
            step: 'ONBOARDING_PROCESS_SKIPPED_NO_SYSTEM_TOKEN',
            status: 'warning',
            message: 'META_SYSTEM_TOKEN is not set on the server environment. Skipping Steps 4-6.'
        });
        return null;
    }
    if (!businessPortfolioId) {
        await saveOnboardingLog({
            tenantId,
            sessionId,
            wabaId,
            businessPortfolioId,
            step: 'ONBOARDING_PROCESS_SKIPPED_NO_PORTFOLIO',
            status: 'warning',
            message: 'businessPortfolioId is missing. Cannot fetch permanent system user access token.'
        });
        return null;
    }

    try {
        // ── Step 4: Generate HMAC-SHA256 appsecret_proof ────────────────────
        const appSecretProof = generateAppSecretProof(systemToken, appSecret);
        await saveOnboardingLog({
            tenantId,
            sessionId,
            wabaId,
            businessPortfolioId,
            step: 'ONBOARDING_PROCESS_HMAC_GEN',
            status: 'info',
            message: 'Step 4: HMAC-SHA256 appsecret_proof generated successfully.'
        });

        // ── Step 5: Get customer business token ─────────────────────────────
        await saveOnboardingLog({
            tenantId,
            sessionId,
            wabaId,
            businessPortfolioId,
            step: 'ONBOARDING_PROCESS_FETCH_TOKEN_START',
            status: 'info',
            message: `Step 5: Querying customer business system user token for portfolio: ${businessPortfolioId}`
        });

        let businessToken = null;
        try {
            const tokenRes = await axios.post(
                `https://graph.facebook.com/${apiVersion}/${businessPortfolioId}/system_user_access_tokens`,
                new URLSearchParams({
                    appsecret_proof: appSecretProof,
                    fetch_only: 'true',
                }),
                {
                    headers: {
                        'Authorization': `Bearer ${systemToken}`,
                        'Content-Type': 'application/x-www-form-urlencoded',
                    },
                }
            );
            businessToken = tokenRes.data.access_token;

            await saveOnboardingLog({
                tenantId,
                sessionId,
                wabaId,
                businessPortfolioId,
                step: 'ONBOARDING_PROCESS_FETCH_TOKEN_SUCCESS',
                status: 'success',
                message: 'Step 5: Permanent business system user access token retrieved successfully.'
            });
        } catch (tokenErr) {
            const tokenErrData = tokenErr.response?.data || tokenErr.message;
            await saveOnboardingLog({
                tenantId,
                sessionId,
                wabaId,
                businessPortfolioId,
                step: 'ONBOARDING_PROCESS_FETCH_TOKEN_FAIL',
                status: 'error',
                message: 'Step 5: Fetching business system user access token failed. Falling back to system token.',
                details: tokenErrData
            });
        }

        const tokenForPhoneQuery = businessToken || systemToken;

        // ── Step 6: Get phone number ID from Phone Numbers API ──────────────
        await saveOnboardingLog({
            tenantId,
            sessionId,
            wabaId,
            businessPortfolioId,
            step: 'ONBOARDING_PROCESS_FETCH_PHONE_START',
            status: 'info',
            message: `Step 6: Querying authoritative phone numbers for WABA ID: ${wabaId}`
        });

        let phoneNumberId = null;
        let displayPhone = null;
        let verifiedName = null;
        let qualityRating = null;
        let throughputLevel = null;

        try {
            const phoneRes = await axios.get(
                `https://graph.facebook.com/${apiVersion}/${wabaId}/phone_numbers`,
                {
                    params: {
                        fields: 'id,display_phone_number,verified_name,code_verification_status,quality_rating,platform_type,throughput,last_onboarded_time,webhook_configuration',
                        access_token: tokenForPhoneQuery,
                    },
                }
            );
            const phones = phoneRes.data?.data || [];

            if (phones.length > 0) {
                const phone = phones[0];
                phoneNumberId = phone.id;
                displayPhone = phone.display_phone_number;
                verifiedName = phone.verified_name;
                qualityRating = phone.quality_rating;
                throughputLevel = phone.throughput?.level;

                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    wabaId,
                    phoneNumberId,
                    step: 'ONBOARDING_PROCESS_FETCH_PHONE_SUCCESS',
                    status: 'success',
                    message: `Step 6: Phone details fetched successfully: ${displayPhone} (Quality: ${qualityRating || 'N/A'}, Throughput: ${throughputLevel || 'N/A'})`,
                    details: phone
                });
            } else {
                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    wabaId,
                    step: 'ONBOARDING_PROCESS_FETCH_PHONE_EMPTY',
                    status: 'warning',
                    message: 'Step 6: No phone numbers found under the registered WABA ID.'
                });
            }
        } catch (phoneErr) {
            const phoneErrData = phoneErr.response?.data || phoneErr.message;
            await saveOnboardingLog({
                tenantId,
                sessionId,
                wabaId,
                step: 'ONBOARDING_PROCESS_FETCH_PHONE_FAIL',
                status: 'error',
                message: 'Step 6: Querying phone numbers API failed.',
                details: phoneErrData
            });
        }

        // ── Step 7: Persist to MongoDB ──────────────────────────────────────
        const updateFields = {
            'whatsappConfig.businessAccountId': wabaId,
            'whatsappConfig.businessPortfolioId': businessPortfolioId,
            'whatsappConfig.onboardedAt': new Date(),
            'whatsappConfig.verified': !!(businessToken && phoneNumberId),
        };
        if (businessToken) updateFields['whatsappConfig.accessToken'] = businessToken;
        if (phoneNumberId) updateFields['whatsappConfig.phoneNumberId'] = phoneNumberId;
        if (displayPhone) updateFields['whatsappConfig.displayPhone'] = displayPhone;
        if (verifiedName) updateFields['whatsappConfig.verifiedName'] = verifiedName;
        if (qualityRating) updateFields['whatsappConfig.qualityRating'] = qualityRating;
        if (throughputLevel) updateFields['whatsappConfig.throughputLevel'] = throughputLevel;

        await saveOnboardingLog({
            tenantId,
            sessionId,
            wabaId,
            businessPortfolioId,
            phoneNumberId,
            step: 'ONBOARDING_PROCESS_DB_SAVE_START',
            status: 'info',
            message: 'Step 7: Persisting finalized onboarding variables to MongoDB...',
            details: updateFields
        });

        let updatedTenant = null;
        if (tenantId) {
            updatedTenant = await Tenant.findByIdAndUpdate(
                tenantId,
                { $set: updateFields },
                { new: true }
            );
            await saveOnboardingLog({
                tenantId,
                sessionId,
                wabaId,
                step: 'ONBOARDING_PROCESS_DB_SAVE_SUCCESS',
                status: 'success',
                message: `Step 7: Successfully updated tenant config for tenant ID: ${tenantId}`,
                details: updatedTenant?.whatsappConfig
            });
        } else {
            updatedTenant = await Tenant.findOneAndUpdate(
                { 'whatsappConfig.businessAccountId': wabaId },
                { $set: updateFields },
                { new: true }
            );
            if (updatedTenant) {
                await saveOnboardingLog({
                    tenantId: updatedTenant._id.toString(),
                    sessionId,
                    wabaId,
                    step: 'ONBOARDING_PROCESS_DB_SAVE_SUCCESS',
                    status: 'success',
                    message: `Step 7: Found tenant by WABA ID and updated successfully.`,
                    details: updatedTenant?.whatsappConfig
                });
            } else {
                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    wabaId,
                    step: 'ONBOARDING_PROCESS_DB_SAVE_NOTFOUND',
                    status: 'warning',
                    message: `Step 7: No active tenant document found matching WABA ID: ${wabaId}`
                });
            }
        }

        return {
            businessToken,
            phoneNumberId,
            displayPhone,
            verifiedName,
            qualityRating,
            throughputLevel,
        };
    } catch (err) {
        await saveOnboardingLog({
            tenantId,
            sessionId,
            wabaId,
            businessPortfolioId,
            step: 'ONBOARDING_PROCESS_FATAL',
            status: 'error',
            message: `Unexpected fatal error in processOnboarding: ${err.message}`,
            details: err.stack
        });
        return null;
    }
}

app.post('/log-signup-event', authenticate, async (req, res) => {
    try {
        const { eventName, sessionId, data } = req.body;
        const tenantId = req.user.tenantId;

        let status = 'info';
        if (eventName.includes('ERROR') || eventName.includes('FAIL')) {
            status = 'error';
        } else if (eventName.includes('CANCEL')) {
            status = 'warning';
        } else if (eventName.includes('SUCCESS')) {
            status = 'success';
        }

        await saveOnboardingLog({
            tenantId,
            sessionId,
            step: eventName,
            status,
            message: `Frontend event: ${eventName}`,
            details: data,
            wabaId: data?.wabaId || null,
            phoneNumberId: data?.phoneNumberId || null,
            businessPortfolioId: data?.businessPortfolioId || null,
        });

        res.json({ success: true });
    } catch (err) {
        console.error('Error logging signup event:', err.message);
        res.status(500).json({ error: 'Failed to write log' });
    }
});


//  Facebook Embedded Signup (Steps 3→7 of Meta Onboarding)
app.post('/facebook-embedded-signup', authenticate, async (req, res) => {
    const {
        code, appId, wabaId, phoneNumberId,
        sessionId, sessionInfoResponse,
        businessPortfolioId,  // ← NEW: Business Portfolio ID from popup postMessage (Step 3)
    } = req.body;
    const tenantId = req.user.tenantId;

    await saveOnboardingLog({
        tenantId,
        sessionId,
        wabaId,
        businessPortfolioId,
        phoneNumberId,
        step: 'BACKEND_START',
        status: 'info',
        message: 'Backend onboarding flow started.',
        details: { appId, hasCode: !!code, sessionId }
    });

    if (!appId) {
        await saveOnboardingLog({
            tenantId,
            sessionId,
            step: 'BACKEND_REJECTED_MISSING_APPID',
            status: 'error',
            message: 'Onboarding rejected: Missing appId'
        });
        return res.status(400).json({ error: 'Missing appId' });
    }

    const appSecret = process.env.META_APP_SECRET;
    if (!appSecret) {
        await saveOnboardingLog({
            tenantId,
            sessionId,
            step: 'BACKEND_FATAL_MISSING_SECRET',
            status: 'error',
            message: 'META_APP_SECRET is not configured on the server environment.'
        });
        return res.status(500).json({ error: 'META_APP_SECRET not configured on server' });
    }

    try {
        let accessToken = null;

        // Exchange code for access token only if a code was provided
        if (code && code.trim() !== '') {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                step: 'BACKEND_EXCHANGE_CODE_START',
                status: 'info',
                message: 'Exchanging auth code for user access token...'
            });

            const apiVersion = process.env.META_API_VERSION || 'v25.0';
            const tokenUrl = `https://graph.facebook.com/${apiVersion}/oauth/access_token`;

            try {
                const tokenResponse = await axios.get(tokenUrl, {
                    params: { client_id: appId, client_secret: appSecret, code: code.trim() }
                });
                accessToken = tokenResponse.data.access_token;
                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    step: 'BACKEND_EXCHANGE_CODE_SUCCESS',
                    status: 'success',
                    message: 'Successfully exchanged auth code for access token.'
                });
            } catch (exErr) {
                const exErrData = exErr.response?.data || exErr.message;
                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    step: 'BACKEND_EXCHANGE_CODE_FAIL',
                    status: 'error',
                    message: 'Failed to exchange auth code for user token.',
                    details: exErrData
                });
                throw exErr;
            }
        } else {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                step: 'BACKEND_EXCHANGE_CODE_SKIPPED',
                status: 'warning',
                message: 'No auth code provided in request body. Skipping exchange.'
            });
        }

        // If no token obtained (no code), we cannot proceed — fall back to manual config
        if (!accessToken) {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                step: 'BACKEND_FALLBACK_NO_TOKEN',
                status: 'warning',
                message: 'No access token was exchanged. Returning fallback redirect/status.'
            });
            return res.status(400).json({
                error: 'No auth code received from Meta. Please configure manually.',
                fallback: true,
            });
        }

        // Legacy fallback: resolve WABA ID / phone number ID from the user token if not in postMessage
        let resolvedWabaId = wabaId || null;
        let resolvedPhoneNumberId = phoneNumberId || null;
        let resolvedBusinessId = businessPortfolioId || null;

        await saveOnboardingLog({
            tenantId,
            sessionId,
            wabaId: resolvedWabaId,
            businessPortfolioId: resolvedBusinessId,
            phoneNumberId: resolvedPhoneNumberId,
            step: 'BACKEND_CHECK_IDS',
            status: 'info',
            message: 'Checking for missing IDs from Meta postMessage.',
            details: { resolvedWabaId, resolvedPhoneNumberId, resolvedBusinessId }
        });

        if (!resolvedWabaId || !resolvedPhoneNumberId) {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                step: 'BACKEND_AUTO_RESOLVE_START',
                status: 'info',
                message: 'Missing IDs. Querying Meta Graph API for WABA/business properties...'
            });

            try {
                const wabaRes = await axios.get(`${WHATSAPP_API_URL}/me/whatsapp_business_accounts`, {
                    params: { access_token: accessToken, fields: 'id,name,business' }
                });
                const wabaAccounts = wabaRes.data?.data || [];
                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    step: 'BACKEND_AUTO_RESOLVE_WABA_COUNT',
                    status: 'info',
                    message: `Found ${wabaAccounts.length} WABA accounts associated with user token.`
                });

                if (wabaAccounts.length > 0) {
                    resolvedWabaId = resolvedWabaId || wabaAccounts[0].id;
                    resolvedBusinessId = resolvedBusinessId || wabaAccounts[0].business?.id || null;
                    await saveOnboardingLog({
                        tenantId,
                        sessionId,
                        wabaId: resolvedWabaId,
                        businessPortfolioId: resolvedBusinessId,
                        step: 'BACKEND_AUTO_RESOLVE_WABA_SUCCESS',
                        status: 'success',
                        message: `Resolved WABA ID: ${resolvedWabaId}, Business Portfolio ID: ${resolvedBusinessId}`
                    });
                }
            } catch (e1) {
                const e1Data = e1.response?.data || e1.message;
                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    step: 'BACKEND_AUTO_RESOLVE_WABA_FAIL',
                    status: 'warning',
                    message: '/me/whatsapp_business_accounts query failed. Falling back to /me/businesses.',
                    details: e1Data
                });

                // Fallback via /me/businesses
                try {
                    const bizRes = await axios.get(`${WHATSAPP_API_URL}/me/businesses`, {
                        params: { access_token: accessToken, fields: 'id,name,owned_whatsapp_business_accounts' }
                    });
                    const businesses = bizRes.data?.data || [];
                    for (const biz of businesses) {
                        const owned = biz.owned_whatsapp_business_accounts?.data || [];
                        if (owned.length > 0) {
                            resolvedWabaId = resolvedWabaId || owned[0].id;
                            resolvedBusinessId = resolvedBusinessId || biz.id;
                            await saveOnboardingLog({
                                tenantId,
                                sessionId,
                                wabaId: resolvedWabaId,
                                businessPortfolioId: resolvedBusinessId,
                                step: 'BACKEND_AUTO_RESOLVE_BIZ_SUCCESS',
                                status: 'success',
                                message: `Resolved WABA ID: ${resolvedWabaId} via Business: ${biz.id}`
                            });
                            break;
                        }
                    }
                } catch (e2) {
                    const e2Data = e2.response?.data || e2.message;
                    await saveOnboardingLog({
                        tenantId,
                        sessionId,
                        step: 'BACKEND_AUTO_RESOLVE_BIZ_FAIL',
                        status: 'warning',
                        message: '/me/businesses query failed.',
                        details: e2Data
                    });
                }
            }

            // fetch phone numbers if we now have a WABA ID but no phone ID
            if (resolvedWabaId && !resolvedPhoneNumberId) {
                try {
                    const phoneRes = await axios.get(`${WHATSAPP_API_URL}/${resolvedWabaId}/phone_numbers`, {
                        params: { access_token: accessToken, fields: 'id,display_phone_number,verified_name' }
                    });
                    const phones = phoneRes.data?.data || [];
                    if (phones.length > 0) {
                        resolvedPhoneNumberId = phones[0].id;
                        await saveOnboardingLog({
                            tenantId,
                            sessionId,
                            wabaId: resolvedWabaId,
                            phoneNumberId: resolvedPhoneNumberId,
                            step: 'BACKEND_AUTO_RESOLVE_PHONE_SUCCESS',
                            status: 'success',
                            message: `Resolved Phone Number ID: ${resolvedPhoneNumberId} (${phones[0].display_phone_number})`
                        });
                    } else {
                        await saveOnboardingLog({
                            tenantId,
                            sessionId,
                            wabaId: resolvedWabaId,
                            step: 'BACKEND_AUTO_RESOLVE_PHONE_EMPTY',
                            status: 'warning',
                            message: 'No phone numbers found in WABA account.'
                        });
                    }
                } catch (e3) {
                    const e3Data = e3.response?.data || e3.message;
                    await saveOnboardingLog({
                        tenantId,
                        sessionId,
                        wabaId: resolvedWabaId,
                        step: 'BACKEND_AUTO_RESOLVE_PHONE_FAIL',
                        status: 'warning',
                        message: 'Phone number list fetch failed.',
                        details: e3Data
                    });
                }
            }
        } else {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                step: 'BACKEND_AUTO_RESOLVE_SKIPPED',
                status: 'info',
                message: 'Skipping auto-resolution of IDs because both WABA ID and Phone ID are already provided.'
            });
        }

        // Base update: store the user token and IDs resolved above
        const baseUpdateFields = {
            'whatsappConfig.accessToken': accessToken,
            'whatsappConfig.metaAppId': appId,
            'whatsappConfig.verified': !!(accessToken && resolvedWabaId && resolvedPhoneNumberId),
        };
        if (resolvedWabaId) baseUpdateFields['whatsappConfig.businessAccountId'] = resolvedWabaId;
        if (resolvedPhoneNumberId) baseUpdateFields['whatsappConfig.phoneNumberId'] = resolvedPhoneNumberId;
        if (resolvedBusinessId) baseUpdateFields['whatsappConfig.businessPortfolioId'] = resolvedBusinessId;
        if (resolvedBusinessId) baseUpdateFields['whatsappConfig.businessId'] = resolvedBusinessId;

        await saveOnboardingLog({
            tenantId,
            sessionId,
            wabaId: resolvedWabaId,
            businessPortfolioId: resolvedBusinessId,
            phoneNumberId: resolvedPhoneNumberId,
            step: 'BACKEND_DB_UPDATE_START',
            status: 'info',
            message: 'Saving initial configuration parameters to MongoDB Tenant...',
            details: baseUpdateFields
        });

        const updatedTenant = await Tenant.findByIdAndUpdate(tenantId, { $set: baseUpdateFields }, { new: true });

        await saveOnboardingLog({
            tenantId,
            sessionId,
            step: 'BACKEND_DB_UPDATE_SUCCESS',
            status: 'success',
            message: 'Tenant document updated successfully with Meta SDK parameters.',
            details: updatedTenant?.whatsappConfig
        });

        // Auto-subscribe the WABA to receive webhook events from our app
        if (resolvedWabaId && accessToken) {
            try {
                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    wabaId: resolvedWabaId,
                    step: 'BACKEND_SUBSCRIBE_WEBHOOKS_START',
                    status: 'info',
                    message: `Subscribing WABA ${resolvedWabaId} to our app webhooks...`
                });

                await axios.post(
                    `${WHATSAPP_API_URL}/${resolvedWabaId}/subscribed_apps`,
                    null,
                    { params: { access_token: accessToken } }
                );

                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    wabaId: resolvedWabaId,
                    step: 'BACKEND_SUBSCRIBE_WEBHOOKS_SUCCESS',
                    status: 'success',
                    message: `Automatically subscribed WABA ${resolvedWabaId} to webhooks successfully.`
                });
            } catch (subErr) {
                const subErrData = subErr.response?.data || subErr.message;
                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    wabaId: resolvedWabaId,
                    step: 'BACKEND_SUBSCRIBE_WEBHOOKS_FAIL',
                    status: 'error',
                    message: `Failed to subscribe WABA to app webhooks.`,
                    details: subErrData
                });
            }
        } else {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                step: 'BACKEND_SUBSCRIBE_WEBHOOKS_SKIPPED',
                status: 'warning',
                message: 'Skipping webhook subscription because WABA ID or Access Token is missing.'
            });
        }

        // ── Official Steps 4→5→6: If businessPortfolioId is known, run processOnboarding() ──
        let onboardingResult = null;
        if (resolvedWabaId && resolvedBusinessId) {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                wabaId: resolvedWabaId,
                businessPortfolioId: resolvedBusinessId,
                step: 'BACKEND_PROCESS_ONBOARDING_START',
                status: 'info',
                message: 'Triggering processOnboarding() to exchange user token for a permanent System User business token...'
            });

            onboardingResult = await processOnboarding(resolvedWabaId, resolvedBusinessId, tenantId, sessionId);

            await saveOnboardingLog({
                tenantId,
                sessionId,
                wabaId: resolvedWabaId,
                businessPortfolioId: resolvedBusinessId,
                step: 'BACKEND_PROCESS_ONBOARDING_FINISH',
                status: onboardingResult ? 'success' : 'warning',
                message: 'processOnboarding finished execution.',
                details: onboardingResult
            });
        } else {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                step: 'BACKEND_PROCESS_ONBOARDING_SKIPPED',
                status: 'warning',
                message: `Skipping processOnboarding because resolvedWabaId (${resolvedWabaId}) or resolvedBusinessId (${resolvedBusinessId}) is missing.`
            });
        }

        // Refresh tenant from DB to get the final merged state
        const finalTenant = await Tenant.findById(tenantId);
        const finalConfig = finalTenant?.whatsappConfig || {};

        const successResp = {
            success: true,
            message: finalConfig.phoneNumberId && finalConfig.businessAccountId
                ? 'WhatsApp Account connected successfully!'
                : 'Token saved. IDs could not be auto-fetched — please verify in settings.',
            partialConnect: !(finalConfig.phoneNumberId && finalConfig.businessAccountId),
            config: {
                accessToken: finalConfig.accessToken,
                wabaId: finalConfig.businessAccountId,
                phoneNumberId: finalConfig.phoneNumberId,
                businessId: finalConfig.businessId,
                businessPortfolioId: finalConfig.businessPortfolioId,
                displayPhone: finalConfig.displayPhone,
                verifiedName: finalConfig.verifiedName,
                qualityRating: finalConfig.qualityRating,
                throughputLevel: finalConfig.throughputLevel,
                metaAppId: appId,
                verified: finalConfig.verified,
            }
        };

        await saveOnboardingLog({
            tenantId,
            sessionId,
            step: 'BACKEND_ONBOARDING_COMPLETE',
            status: 'success',
            message: 'Meta onboarding backend workflow completed successfully.',
            details: { partialConnect: successResp.partialConnect, verified: finalConfig.verified }
        });

        res.json(successResp);
    } catch (error) {
        const errorData = error.response?.data || error.message;
        await saveOnboardingLog({
            tenantId,
            sessionId,
            step: 'BACKEND_ONBOARDING_FATAL',
            status: 'error',
            message: 'Fatal error occurred in backend onboarding execution.',
            details: errorData
        });
        res.status(500).json({ error: 'Failed to exchange Meta code for token', details: errorData });
    }
});

// Re-fetch WABA ID + Phone ID using the already-stored access token (no re-auth needed)
// Re-fetch WABA ID + Phone ID + business token using stored credentials
app.post('/refresh-meta-account', authenticate, async (req, res) => {
    const tenantId = req.user.tenantId;
    const { sessionId } = req.body; // Allow optional sessionId from client for tracking

    await saveOnboardingLog({
        tenantId,
        sessionId,
        step: 'BACKEND_REFRESH_START',
        status: 'info',
        message: 'Refresh Meta account credentials requested.'
    });

    try {
        const tenant = await Tenant.findById(tenantId);
        if (!tenant) {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                step: 'BACKEND_REFRESH_TENANT_NOT_FOUND',
                status: 'error',
                message: 'Refresh failed: Tenant not found in DB.'
            });
            return res.status(404).json({ error: 'Tenant not found' });
        }

        const accessToken = tenant.whatsappConfig?.accessToken;
        if (!accessToken) {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                step: 'BACKEND_REFRESH_NO_TOKEN',
                status: 'error',
                message: 'Refresh failed: No access token stored. Connect Meta account first.'
            });
            return res.status(400).json({ error: 'No access token stored. Please reconnect.' });
        }

        let resolvedWabaId = tenant.whatsappConfig.businessAccountId || null;
        let resolvedPhoneNumberId = tenant.whatsappConfig.phoneNumberId || null;
        let resolvedBusinessId = tenant.whatsappConfig.businessPortfolioId || tenant.whatsappConfig.businessId || null;

        await saveOnboardingLog({
            tenantId,
            sessionId,
            wabaId: resolvedWabaId,
            businessPortfolioId: resolvedBusinessId,
            phoneNumberId: resolvedPhoneNumberId,
            step: 'BACKEND_REFRESH_CHECK_PARAMS',
            status: 'info',
            message: 'Retrieved stored configs for refresh.',
            details: { resolvedWabaId, resolvedPhoneNumberId, resolvedBusinessId }
        });

        // If we have a stored businessPortfolioId, run the official Steps 4→5→6
        if (resolvedWabaId && resolvedBusinessId) {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                wabaId: resolvedWabaId,
                businessPortfolioId: resolvedBusinessId,
                step: 'BACKEND_REFRESH_TRIGGER_ONBOARDING',
                status: 'info',
                message: 'Triggering processOnboarding() for permanent token exchange...'
            });
            const onboardResult = await processOnboarding(resolvedWabaId, resolvedBusinessId, tenantId, sessionId);
            if (onboardResult?.phoneNumberId) {
                resolvedPhoneNumberId = onboardResult.phoneNumberId;
            }
        } else {
            await saveOnboardingLog({
                tenantId,
                sessionId,
                wabaId: resolvedWabaId,
                businessPortfolioId: resolvedBusinessId,
                step: 'BACKEND_REFRESH_ONBOARDING_SKIPPED',
                status: 'warning',
                message: 'Skipping processOnboarding exchange because resolvedWabaId or resolvedBusinessId is missing from config.'
            });
        }

        // Auto-subscribe/re-subscribe the WABA to receive webhook events
        if (resolvedWabaId && accessToken) {
            try {
                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    wabaId: resolvedWabaId,
                    step: 'BACKEND_REFRESH_SUBSCRIBE_START',
                    status: 'info',
                    message: `Subscribing WABA ${resolvedWabaId} to app webhooks...`
                });

                const apiVer = process.env.META_API_VERSION || 'v25.0';
                await axios.post(
                    `https://graph.facebook.com/${apiVer}/${resolvedWabaId}/subscribed_apps`,
                    null,
                    { params: { access_token: accessToken } }
                );

                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    wabaId: resolvedWabaId,
                    step: 'BACKEND_REFRESH_SUBSCRIBE_SUCCESS',
                    status: 'success',
                    message: `Subscribed WABA ${resolvedWabaId} to app webhooks successfully.`
                });
            } catch (subErr) {
                const subErrData = subErr.response?.data || subErr.message;
                await saveOnboardingLog({
                    tenantId,
                    sessionId,
                    wabaId: resolvedWabaId,
                    step: 'BACKEND_REFRESH_SUBSCRIBE_FAIL',
                    status: 'error',
                    message: 'Failed to subscribe WABA to app webhooks during refresh.',
                    details: subErrData
                });
            }
        }

        // Read final state from DB
        const finalTenant = await Tenant.findById(tenantId);
        const finalConfig = finalTenant?.whatsappConfig || {};

        await saveOnboardingLog({
            tenantId,
            sessionId,
            step: 'BACKEND_REFRESH_COMPLETE',
            status: 'success',
            message: 'Refresh Meta account credentials completed successfully.',
            details: { verified: finalConfig.verified }
        });

        res.json({
            success: true,
            config: {
                accessToken: finalConfig.accessToken,
                wabaId: finalConfig.businessAccountId,
                phoneNumberId: finalConfig.phoneNumberId,
                businessId: finalConfig.businessId,
                businessPortfolioId: finalConfig.businessPortfolioId,
                displayPhone: finalConfig.displayPhone,
                verifiedName: finalConfig.verifiedName,
                qualityRating: finalConfig.qualityRating,
                throughputLevel: finalConfig.throughputLevel,
                verified: finalConfig.verified,
            }
        });
    } catch (error) {
        const errorData = error.response?.data || error.message;
        await saveOnboardingLog({
            tenantId,
            sessionId,
            step: 'BACKEND_REFRESH_FATAL',
            status: 'error',
            message: 'Fatal error occurred in backend refresh execution.',
            details: errorData
        });
        res.status(500).json({ error: 'Failed to refresh Meta account', details: errorData });
    }
});

// GET Onboarding Logs for a Tenant
app.get('/onboarding-logs', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const logs = await OnboardingLog.find({ tenantId }).sort({ timestamp: -1 }).limit(100);
        res.json({ success: true, logs });
    } catch (err) {
        console.error('Error fetching onboarding logs:', err.message);
        res.status(500).json({ error: 'Failed to fetch onboarding logs' });
    }
});

// GET Onboarding Logs for a specific session ID
app.get('/onboarding-logs/:sessionId', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const { sessionId } = req.params;
        const logs = await OnboardingLog.find({ tenantId, sessionId }).sort({ timestamp: 1 });
        res.json({ success: true, logs });
    } catch (err) {
        console.error(`Error fetching onboarding logs for session ${req.params.sessionId}:`, err.message);
        res.status(500).json({ error: 'Failed to fetch onboarding logs' });
    }
});


//  Media Upload Proxies 
app.post('/upload-media', authenticate, async (req, res) => {
    const { name, size, type, accessToken, appId } = req.query;
    const fileData = req.body;
    try {
        const initResponse = await axios.post(`${WHATSAPP_API_URL}/${appId}/uploads`, null, {
            params: { file_name: name, file_length: parseInt(size), file_type: type, access_token: accessToken }
        });
        const sessionId = initResponse.data.id;
        const uploadResponse = await axios.post(`${WHATSAPP_API_URL}/${sessionId}`, fileData, {
            headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/octet-stream', file_offset: '0' }
        });
        res.json({ h: uploadResponse.data.h });
    } catch (error) {
        const errorData = error.response?.data || error.message;
        res.status(error.response?.status || 500).json({ error: { message: 'Failed to proxy media upload', details: errorData } });
    }
});

app.post('/media-upload', authenticate, async (req, res) => {
    const { phoneNumberId, accessToken, fileName, fileType } = req.query;
    const fileData = req.body;
    try {
        const FormData = require('form-data');
        const form = new FormData();
        form.append('messaging_product', 'whatsapp');
        form.append('file', fileData, { filename: fileName, contentType: fileType });
        const response = await axios.post(
            `${WHATSAPP_API_URL}/${phoneNumberId}/media`,
            form,
            { headers: { ...form.getHeaders(), Authorization: `Bearer ${accessToken}` } }
        );
        res.json({ id: response.data.id });
    } catch (error) {
        const errorData = error.response?.data || error.message;
        res.status(error.response?.status || 500).json({ error: { message: 'Failed to proxy standard media upload', details: errorData } });
    }
});

// ─── Chatbot Engine ───────────────────────────────────────────────────────────

// Task 2.3 — per-contact in-memory processing lock (promise chain)
const chatbotProcessingLocks = new Map();

function withContactLock(lockKey, fn) {
    const prev = chatbotProcessingLocks.get(lockKey) || Promise.resolve();
    const next = prev.then(() => fn()).catch(err => {
        console.error(` [ChatbotEngine] Lock error for ${lockKey}:`, err.message);
    });
    chatbotProcessingLocks.set(lockKey, next);
    // Clean up after chain settles to avoid memory leak
    next.finally(() => {
        if (chatbotProcessingLocks.get(lockKey) === next) {
            chatbotProcessingLocks.delete(lockKey);
        }
    });
    return next;
}

// Task 2.14 — handle Meta API 401: deactivate all tenant chatbots + emit socket event
async function handleMetaAuth401(tenantId) {
    try {
        await Chatbot.updateMany({ tenantId }, { $set: { isActive: false } });
        io.to(tenantId).emit('chatbot_auth_error', { tenantId });
        console.error(` [ChatbotEngine] 401 from Meta API — deactivated all chatbots for tenant ${tenantId}`);
    } catch (err) {
        console.error(` [ChatbotEngine] handleMetaAuth401 error:`, err.message);
    }
}

// Helper — send a plain text message via Meta API (returns true on success)
// chatbotId is optional; pass session.chatbotId when available for accurate analytics
async function sendChatbotText(tenant, to, text, chatbotId) {
    const { phoneNumberId, accessToken } = tenant.whatsappConfig;
    try {
        await axios.post(
            `${WHATSAPP_API_URL}/${phoneNumberId}/messages`,
            {
                messaging_product: 'whatsapp',
                to,
                type: 'text',
                text: { body: text },
            },
            { headers: { Authorization: `Bearer ${accessToken}` } }
        );
        // Increment messagesSent analytics (best-effort)
        if (chatbotId) {
            const todayDate = new Date(); todayDate.setHours(0, 0, 0, 0);
            ChatbotAnalytics.findOneAndUpdate(
                { tenantId: tenant._id.toString(), chatbotId, date: todayDate },
                { $inc: { messagesSent: 1 } },
                { upsert: true }
            ).catch(() => { });
        }
        return true;
    } catch (err) {
        if (err.response?.status === 401) await handleMetaAuth401(tenant._id.toString());
        console.error(` [ChatbotEngine] sendChatbotText error:`, err.response?.data || err.message);
        return false;
    }
}

// Helper — re-send the prompt for the current node (used by media guard and condition fallback)
async function resendNodePrompt(session, node, tenant, from) {
    if (!node) return;
    const type = node.type;
    const data = node.data || {};
    const { phoneNumberId, accessToken } = tenant.whatsappConfig;

    if (type === 'question') {
        await sendChatbotText(tenant, from, data.text || data.question || '', session.chatbotId);
    } else if (type === 'quickReply') {
        const buttons = (data.buttons || []).slice(0, 3).map((b, i) => ({
            type: 'reply',
            reply: { id: `btn_${i}`, title: (b.label || b.title || '').substring(0, 20) },
        }));
        if (buttons.length === 0) return;
        try {
            await axios.post(
                `${WHATSAPP_API_URL}/${phoneNumberId}/messages`,
                {
                    messaging_product: 'whatsapp',
                    to: from,
                    type: 'interactive',
                    interactive: {
                        type: 'button',
                        body: { text: data.text || data.body || 'Choose an option:' },
                        action: { buttons },
                    },
                },
                { headers: { Authorization: `Bearer ${accessToken}` } }
            );
        } catch (err) {
            if (err.response?.status === 401) await handleMetaAuth401(tenant._id.toString());
            console.error(` [ChatbotEngine] resendNodePrompt quickReply error:`, err.response?.data || err.message);
        }
    } else if (type === 'listMessage') {
        const rows = (data.items || data.rows || []).slice(0, 10).map((item, i) => ({
            id: `row_${i}`,
            title: (item.title || '').substring(0, 24),
            description: (item.description || '').substring(0, 72),
        }));
        if (rows.length === 0) return;
        try {
            await axios.post(
                `${WHATSAPP_API_URL}/${phoneNumberId}/messages`,
                {
                    messaging_product: 'whatsapp',
                    to: from,
                    type: 'interactive',
                    interactive: {
                        type: 'list',
                        body: { text: data.text || data.body || 'Choose an option:' },
                        action: {
                            button: data.buttonLabel || 'View Options',
                            sections: [{ title: data.sectionTitle || 'Options', rows }],
                        },
                    },
                },
                { headers: { Authorization: `Bearer ${accessToken}` } }
            );
        } catch (err) {
            if (err.response?.status === 401) await handleMetaAuth401(tenant._id.toString());
            console.error(` [ChatbotEngine] resendNodePrompt listMessage error:`, err.response?.data || err.message);
        }
    } else if (type === 'condition') {
        if (data.prompt) await sendChatbotText(tenant, from, data.prompt, session.chatbotId);
    }
}

// Helper — find a node by id in the flow
function findNode(flow, nodeId) {
    return (flow.nodes || []).find(n => n.id === nodeId) || null;
}

// Helper — find the next node id following an edge from sourceNodeId (optionally matching a port label)
function findNextNodeId(flow, sourceNodeId, portLabel) {
    const edges = flow.edges || [];
    if (!portLabel) {
        // No label — return first edge from source
        const match = edges.find(e => e.sourceNodeId === sourceNodeId);
        return match ? match.targetNodeId : null;
    }
    const label = portLabel.toLowerCase().trim();
    // Match by edgeLabel first, then sourcePort as fallback
    const match = edges.find(e =>
        e.sourceNodeId === sourceNodeId &&
        (
            (e.edgeLabel || '').toLowerCase().trim() === label ||
            (e.sourcePort || '').toLowerCase().trim() === label
        )
    );
    return match ? match.targetNodeId : null;
}

// Helper — find fallback edge target from a node
function findFallbackNodeId(flow, sourceNodeId) {
    const edges = flow.edges || [];
    const fallback = edges.find(e =>
        e.sourceNodeId === sourceNodeId &&
        (e.sourcePort || '').toLowerCase() === 'fallback'
    );
    return fallback ? fallback.targetNodeId : null;
}

// Task 2.4 — node executor dispatcher
async function executeNode(session, node, tenant, from) {
    if (!node) {
        console.warn(` [ChatbotEngine] executeNode called with null node for session ${session._id}`);
        return;
    }
    const type = node.type;
    console.log(` [ChatbotEngine] Executing node type=${type} id=${node.id} for ${from}`);

    if (type === 'message') return handleMessageNode(session, node, tenant, from);
    if (type === 'question') return handleQuestionNode(session, node, tenant, from);
    if (type === 'quickReply') return handleQuickReplyNode(session, node, tenant, from);
    if (type === 'listMessage') return handleListMessageNode(session, node, tenant, from);
    if (type === 'condition') return handleConditionNode(session, node, tenant, from);
    if (type === 'action') return handleActionNode(session, node, tenant, from);
    if (type === 'end') return handleEndNode(session, node, tenant, from);
    // Unknown node type — skip to next
    const flow = session._flow;
    const nextId = findNextNodeId(flow, node.id);
    if (nextId) {
        const nextNode = findNode(flow, nextId);
        return executeNode(session, nextNode, tenant, from);
    }
}

// Task 2.5 — Message Node: send text, advance, recurse
async function handleMessageNode(session, node, tenant, from) {
    const data = node.data || {};
    const text = data.text || data.body || '';
    if (text) {
        const ok = await sendChatbotText(tenant, from, text, session.chatbotId);
        if (!ok) return; // Meta API error — do not advance
    }
    const flow = session._flow;
    const nextId = findNextNodeId(flow, node.id);
    if (!nextId) return; // No next node — stay put (shouldn't happen in valid flow)
    // NOTE: do NOT set currentNodeId or save here — let the next node set it when it parks.
    // Pre-saving causes interactive nodes (listMessage, quickReply, question) to think
    // they are already parked and process the trigger text as a reply instead of sending the prompt.
    const nextNode = findNode(flow, nextId);
    return executeNode(session, nextNode, tenant, from);
}

// Task 2.6 — Question Node: send question, pause
// Task 2.6 — Question Node: send question, pause OR process reply if already waiting
async function handleQuestionNode(session, node, tenant, from) {
    const data = node.data || {};
    const flow = session._flow;

    // If we are already parked on this node, the user just replied — process it
    // (currentNodeId matches AND this is not the first time we're executing this node,
    //  i.e. the session was already saved pointing here before this message arrived)
    const alreadyWaiting = session.currentNodeId === node.id && session.waitingForReply;

    if (alreadyWaiting) {
        const reply = (session.lastReply || '').toLowerCase().trim();
        const keywords = data.keywords || [];

        // Try to match a keyword route
        for (const kw of keywords) {
            const keyword = (kw.keyword || '').toLowerCase().trim();
            if (keyword && reply.includes(keyword)) {
                const nextId = findNextNodeId(flow, node.id, kw.edgeLabel || kw.keyword);
                if (nextId) {
                    session.currentNodeId = nextId;
                    await session.save();
                    return executeNode(session, findNode(flow, nextId), tenant, from);
                }
            }
        }

        // No keyword matched — try fallback edge
        const fallbackId = findFallbackNodeId(flow, node.id);
        if (fallbackId) {
            session.currentNodeId = fallbackId;
            await session.save();
            return executeNode(session, findNode(flow, fallbackId), tenant, from);
        }

        // No fallback — re-prompt with the question
        const text = data.text || data.question || '';
        if (text) await sendChatbotText(tenant, from, text, session.chatbotId);
        session.currentNodeId = node.id;
        session.waitingForReply = true;
        await session.save();
        return;
    }

    // First visit — send the question and pause
    const text = data.text || data.question || '';
    if (text) await sendChatbotText(tenant, from, text, session.chatbotId);
    session.currentNodeId = node.id;
    session.waitingForReply = true;
    await session.save();
}

// Task 2.7 — Quick Reply Node: send interactive button message, pause OR process reply
async function handleQuickReplyNode(session, node, tenant, from) {
    const data = node.data || {};
    const flow = session._flow;

    // If already parked here, process the button reply
    if (session.currentNodeId === node.id && session.waitingForReply) {
        const reply = (session.lastReply || '').toLowerCase().trim();
        const buttons = data.buttons || [];

        for (const btn of buttons) {
            const label = (btn.label || btn.title || '').toLowerCase().trim();
            if (label && reply.includes(label)) {
                const nextId = findNextNodeId(flow, node.id, btn.label || btn.title);
                if (nextId) {
                    session.currentNodeId = nextId;
                    session.waitingForReply = false;
                    await session.save();
                    return executeNode(session, findNode(flow, nextId), tenant, from);
                }
            }
        }

        // No match — fallback or first available edge
        const fallbackId = findFallbackNodeId(flow, node.id) || findNextNodeId(flow, node.id);
        if (fallbackId) {
            session.currentNodeId = fallbackId;
            session.waitingForReply = false;
            await session.save();
            return executeNode(session, findNode(flow, fallbackId), tenant, from);
        }
        return;
    }

    // First visit — send the button message
    const { phoneNumberId, accessToken } = tenant.whatsappConfig;
    const buttons = (data.buttons || []).slice(0, 3).map((b, i) => ({
        type: 'reply',
        reply: { id: `btn_${i}`, title: (b.label || b.title || '').substring(0, 20) },
    }));
    if (buttons.length === 0) {
        const nextId = findNextNodeId(flow, node.id);
        if (nextId) { session.currentNodeId = nextId; await session.save(); return executeNode(session, findNode(flow, nextId), tenant, from); }
        return;
    }
    try {
        await axios.post(
            `${WHATSAPP_API_URL}/${phoneNumberId}/messages`,
            {
                messaging_product: 'whatsapp',
                to: from,
                type: 'interactive',
                interactive: {
                    type: 'button',
                    body: { text: data.text || data.body || 'Choose an option:' },
                    action: { buttons },
                },
            },
            { headers: { Authorization: `Bearer ${accessToken}` } }
        );
    } catch (err) {
        if (err.response?.status === 401) await handleMetaAuth401(tenant._id.toString());
        console.error(` [ChatbotEngine] quickReply send error:`, err.response?.data || err.message);
        return;
    }
    session.currentNodeId = node.id;
    session.waitingForReply = true;
    await session.save();
}

// Task 2.8 — List Message Node: send interactive list message, pause OR process reply
async function handleListMessageNode(session, node, tenant, from) {
    const data = node.data || {};
    const flow = session._flow;

    // If already parked here AND waiting for a reply, process the list selection
    if (session.currentNodeId === node.id && session.waitingForReply) {
        const reply = (session.lastReply || '').toLowerCase().trim();
        const items = data.items || data.rows || [];
        console.log(`[ListMessage] Processing reply="${reply}" against ${items.length} items: ${items.map(i => i.title).join(', ')}`);
        console.log(`[ListMessage] session.currentNodeId=${session.currentNodeId} node.id=${node.id} waitingForReply=${session.waitingForReply}`);

        for (const item of items) {
            const title = (item.title || '').toLowerCase().trim();
            if (title && reply.includes(title)) {
                const nextId = findNextNodeId(flow, node.id, item.title);
                if (nextId) {
                    session.currentNodeId = nextId;
                    session.waitingForReply = false;
                    await session.save();
                    return executeNode(session, findNode(flow, nextId), tenant, from);
                }
            }
        }

        // No match — fallback or first available edge
        const fallbackId = findFallbackNodeId(flow, node.id) || findNextNodeId(flow, node.id);
        if (fallbackId) {
            session.currentNodeId = fallbackId;
            session.waitingForReply = false;
            await session.save();
            return executeNode(session, findNode(flow, fallbackId), tenant, from);
        }
        return;
    }

    // First visit — send the list message
    console.log(`[ListMessage] First visit — sending prompt. currentNodeId=${session.currentNodeId} node.id=${node.id} waitingForReply=${session.waitingForReply}`);
    const { phoneNumberId, accessToken } = tenant.whatsappConfig;
    const rows = (data.items || data.rows || []).slice(0, 10).map((item, i) => ({
        id: `row_${i}`,
        title: (item.title || '').substring(0, 24),
        description: (item.description || '').substring(0, 72),
    }));
    if (rows.length === 0) {
        const nextId = findNextNodeId(flow, node.id);
        if (nextId) { session.currentNodeId = nextId; await session.save(); return executeNode(session, findNode(flow, nextId), tenant, from); }
        return;
    }
    try {
        await axios.post(
            `${WHATSAPP_API_URL}/${phoneNumberId}/messages`,
            {
                messaging_product: 'whatsapp',
                to: from,
                type: 'interactive',
                interactive: {
                    type: 'list',
                    body: { text: data.text || data.body || 'Choose an option:' },
                    action: {
                        button: (data.buttonLabel || 'View Options').substring(0, 20),
                        sections: [{ title: (data.sectionTitle || 'Options').substring(0, 24), rows }],
                    },
                },
            },
            { headers: { Authorization: `Bearer ${accessToken}` } }
        );
    } catch (err) {
        if (err.response?.status === 401) await handleMetaAuth401(tenant._id.toString());
        console.error(` [ChatbotEngine] listMessage send error:`, JSON.stringify(err.response?.data || err.message));
        return;
    }
    session.currentNodeId = node.id;
    session.waitingForReply = true;
    await session.save();
}

// Task 2.9 — Condition Node: case-insensitive keyword match, fallback or re-prompt
async function handleConditionNode(session, node, tenant, from) {
    const data = node.data || {};
    const flow = session._flow;
    const reply = (session.lastReply || '').toLowerCase().trim();
    const rules = data.rules || data.conditions || [];

    // Try to match a rule
    let matchedEdgeLabel = null;
    for (const rule of rules) {
        const keyword = (rule.keyword || rule.label || '').toLowerCase().trim();
        if (keyword && reply.includes(keyword)) {
            matchedEdgeLabel = rule.edgeLabel || rule.keyword || rule.label;
            break;
        }
    }

    if (matchedEdgeLabel) {
        const nextId = findNextNodeId(flow, node.id, matchedEdgeLabel);
        if (nextId) {
            session.currentNodeId = nextId;
            await session.save();
            return executeNode(session, findNode(flow, nextId), tenant, from);
        }
    }

    // No match — try fallback edge
    const fallbackId = findFallbackNodeId(flow, node.id);
    if (fallbackId) {
        session.currentNodeId = fallbackId;
        await session.save();
        return executeNode(session, findNode(flow, fallbackId), tenant, from);
    }

    // No fallback — re-prompt
    await sendChatbotText(tenant, from, data.noMatchMessage || "I didn't understand that. Please try again.", session.chatbotId);
    await resendNodePrompt(session, node, tenant, from);
    session.currentNodeId = node.id;
    await session.save();
}

// Task 2.10 — Action Node handlers
async function handleActionNode(session, node, tenant, from) {
    const data = node.data || {};
    const subType = data.subType || data.action || data.type;
    const tenantId = tenant._id.toString();
    const flow = session._flow;

    if (subType === 'assign_tag') {
        const tag = data.tag || data.tagName || '';
        if (tag) {
            try {
                await Client.findOneAndUpdate(
                    { tenantId, mobileNumber: from },
                    { $addToSet: { tags: tag } }
                );
            } catch (err) {
                console.error(` [ChatbotEngine] assign_tag error:`, err.message);
            }
        }
    } else if (subType === 'send_template') {
        const { phoneNumberId, accessToken } = tenant.whatsappConfig;
        const templateName = data.templateName || data.template || '';
        const language = data.language || 'en_US';
        if (templateName) {
            const components = [];
            const mediaUrl = data.mediaUrl || data.mediaId || '';
            const mediaType = data.mediaType || ''; // 'image' | 'video' | 'document'

            if (mediaUrl && mediaType) {
                const isUrl = mediaUrl.startsWith('http://') || mediaUrl.startsWith('https://');
                const mediaObject = isUrl ? { link: mediaUrl } : { id: mediaUrl };
                const type = mediaType.toLowerCase();
                components.push({
                    type: 'header',
                    parameters: [
                        { type, [type]: mediaObject }
                    ]
                });
            }

            const variables = data.variables || [];
            if (Array.isArray(variables) && variables.length > 0) {
                components.push({
                    type: 'body',
                    parameters: variables.map(v => ({ type: 'text', text: String(v) }))
                });
            }

            try {
                await axios.post(
                    `${WHATSAPP_API_URL}/${phoneNumberId}/messages`,
                    {
                        messaging_product: 'whatsapp',
                        to: from,
                        type: 'template',
                        template: { 
                            name: templateName, 
                            language: { code: language },
                            ...(components.length ? { components } : {})
                        },
                    },
                    { headers: { Authorization: `Bearer ${accessToken}` } }
                );
            } catch (err) {
                if (err.response?.status === 401) await handleMetaAuth401(tenantId);
                console.error(` [ChatbotEngine] send_template error:`, err.response?.data || err.message);
                return;
            }
        }
    } else if (subType === 'end_session') {
        await ChatbotSession.deleteOne({ _id: session._id });
        // Fall through to live chat for this message
        const message = session._originalMessage;
        const profileName = session._profileName || 'Unknown';
        if (message) await handleLiveChat(tenantId, message, profileName, from);
        return;
    }

    // Advance to next node
    const nextId = findNextNodeId(flow, node.id);
    if (nextId) {
        session.currentNodeId = nextId;
        await session.save();
        return executeNode(session, findNode(flow, nextId), tenant, from);
    }
}

// Task 2.11 — End Node: delete session, increment completedSessions analytics
async function handleEndNode(session, node, tenant, from) {
    const tenantId = tenant._id.toString();
    const chatbotId = session.chatbotId;
    try {
        await ChatbotSession.deleteOne({ _id: session._id });
        const todayDate = new Date(); todayDate.setHours(0, 0, 0, 0);
        await ChatbotAnalytics.findOneAndUpdate(
            { tenantId, chatbotId, date: todayDate },
            { $inc: { completedSessions: 1 } },
            { upsert: true }
        );
        console.log(` [ChatbotEngine] Session ended (End Node) for ${from}, tenant ${tenantId}`);
    } catch (err) {
        console.error(` [ChatbotEngine] handleEndNode error:`, err.message);
    }
}

// Task 2.2 — main entry point
async function chatbotEngineProcessMessage(tenantId, message, profileName, from, tenant) {
    const lockKey = `${tenantId}:${from}`;
    return withContactLock(lockKey, async () => {
        const msgType = message.type || 'text';

        // Extract text from plain text OR interactive replies (list/button selections)
        let rawText = '';
        if (msgType === 'text') {
            rawText = message.text?.body || '';
        } else if (msgType === 'interactive') {
            const interactive = message.interactive || {};
            if (interactive.type === 'list_reply') {
                rawText = interactive.list_reply?.title || interactive.list_reply?.id || '';
            } else if (interactive.type === 'button_reply') {
                rawText = interactive.button_reply?.title || interactive.button_reply?.id || '';
            }
        } else if (msgType === 'button') {
            rawText = message.button?.text || message.button?.payload || '';
        }

        console.log(`[ChatbotEngine] Full message object:`, JSON.stringify(message, null, 2));
        console.log(`[ChatbotEngine] msgType=${msgType} rawText="${rawText}" from=${from}`);

        // Task 2.14 — truncate replies > 4096 chars
        const msgText = rawText.length > 4096 ? rawText.substring(0, 4096) : rawText;

        // Check for open session
        let session = await ChatbotSession.findOne({ tenantId, contactId: from });

        if (session) {
            // Media guard: block non-text, non-interactive, non-button messages (images, audio, etc.)
            if (msgType !== 'text' && msgType !== 'interactive' && msgType !== 'button') {
                await sendChatbotText(tenant, from, 'Please reply with text.', session.chatbotId);
                // Re-send current node prompt
                const flow = await getChatbotFlow(session.chatbotId);
                if (flow) {
                    session._flow = flow;
                    const currentNode = findNode(flow, session.currentNodeId);
                    await resendNodePrompt(session, currentNode, tenant, from);
                }
                return;
            }

            // Update lastReply
            session.lastReply = msgText;
            session._originalMessage = message;
            session._profileName = profileName;
            await session.save();

            const flow = await getChatbotFlow(session.chatbotId);
            if (!flow) {
                // Chatbot deleted — clean up and fall through
                await ChatbotSession.deleteOne({ _id: session._id });
                return handleLiveChat(tenantId, message, profileName, from);
            }
            session._flow = flow;
            const currentNode = findNode(flow, session.currentNodeId);
            return executeNode(session, currentNode, tenant, from);
        }

        // No open session — check trigger keywords (text OR template button replies)
        if (msgType !== 'text' && msgType !== 'button') {
            return handleLiveChat(tenantId, message, profileName, from);
        }

        // For template button replies, extract button label
        const buttonText = msgType === 'button'
            ? (message.button?.text || message.button?.payload || '').trim()
            : null;

        const activeChatbots = await Chatbot.find({ tenantId, isActive: true });
        let matchedChatbot = null;
        for (const bot of activeChatbots) {
            for (const kw of bot.triggerKeywords) {
                if (msgType === 'button') {
                    // Button reply: exact match only (case-insensitive)
                    if (buttonText.toLowerCase() === kw.toLowerCase()) {
                        matchedChatbot = bot;
                        break;
                    }
                } else {
                    // Plain text: existing includes-based matching
                    if (msgText.toLowerCase().includes(kw.toLowerCase())) {
                        matchedChatbot = bot;
                        break;
                    }
                }
            }
            if (matchedChatbot) break;
        }

        if (!matchedChatbot) {
            return handleLiveChat(tenantId, message, profileName, from);
        }

        // Trigger matched — create session and execute first node
        const flow = matchedChatbot.flow;
        if (!flow || !flow.nodes) {
            return handleLiveChat(tenantId, message, profileName, from);
        }

        // Find start node's first child
        const startNode = (flow.nodes || []).find(n => n.type === 'start');
        const firstNodeId = startNode ? findNextNodeId(flow, startNode.id) : (flow.nodes[0]?.id || null);
        if (!firstNodeId) {
            return handleLiveChat(tenantId, message, profileName, from);
        }

        const todayDate = new Date(); todayDate.setHours(0, 0, 0, 0);
        try {
            session = await ChatbotSession.create({
                tenantId,
                contactId: from,
                chatbotId: matchedChatbot._id.toString(),
                currentNodeId: null,
                waitingForReply: false,
                lastReply: msgText,
            });
            // Increment totalSessions analytics
            await ChatbotAnalytics.findOneAndUpdate(
                { tenantId, chatbotId: matchedChatbot._id.toString(), date: todayDate },
                { $inc: { totalSessions: 1 } },
                { upsert: true }
            );
        } catch (err) {
            // Unique index violation — session already exists (race condition), re-fetch
            session = await ChatbotSession.findOne({ tenantId, contactId: from });
            if (!session) return handleLiveChat(tenantId, message, profileName, from);
        }

        session._flow = flow;
        session._originalMessage = message;
        session._profileName = profileName;
        const firstNode = findNode(flow, firstNodeId);
        return executeNode(session, firstNode, tenant, from);
    });
}

// Helper — fetch chatbot flow by chatbotId
async function getChatbotFlow(chatbotId) {
    try {
        const bot = await Chatbot.findById(chatbotId);
        return bot ? bot.flow : null;
    } catch { return null; }
}

// Task 2.13 — session auto-expiry (runs every hour)
setInterval(async () => {
    try {
        const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000);
        const expiredSessions = await ChatbotSession.find({ updatedAt: { $lt: cutoff } });
        for (const session of expiredSessions) {
            const todayDate = new Date(); todayDate.setHours(0, 0, 0, 0);
            await ChatbotAnalytics.findOneAndUpdate(
                { tenantId: session.tenantId, chatbotId: session.chatbotId, date: todayDate },
                { $inc: { droppedSessions: 1 } },
                { upsert: true }
            );
            await ChatbotSession.deleteOne({ _id: session._id });
        }
        if (expiredSessions.length > 0) {
            console.log(` [ChatbotEngine] Expired ${expiredSessions.length} stale chatbot session(s)`);
        }
    } catch (err) {
        console.error(` [ChatbotEngine] Session expiry error:`, err.message);
    }
}, 60 * 60 * 1000);

// ─── End Chatbot Engine ───────────────────────────────────────────────────────

/**
 * buildPreview — derives a human-readable lastMessage preview string from a structured message.
 * Used by all three write paths (inbound, /send-message, dispatchTemplate) to keep preview logic centralised.
 *
 * @param {string} messageType  - The WhatsApp message type string
 * @param {object} opts         - Options object
 * @param {string} [opts.text]          - Plain text body (for 'text' type)
 * @param {string} [opts.templateName]  - Template name (for 'template' type)
 * @param {string} [opts.templateBody]  - Resolved template body (for 'template' type)
 * @param {string} [opts.filename]      - Document filename (for 'document' type)
 * @param {string} [opts.emoji]         - Reaction emoji (for 'reaction' type)
 * @param {string} [opts.interactiveTitle] - Button/list reply title (for 'interactive' type)
 * @returns {string}
 */
function buildPreview(messageType, opts = {}) {
    const truncate = (str, max = 80) => (str && str.length > max ? str.substring(0, max) + '…' : str || '');

    switch (messageType) {
        case 'text':
            return truncate(opts.text);
        case 'template':
            if (opts.templateBody) return truncate(opts.templateBody);
            return `📋 Template: ${opts.templateName || 'unknown'}`;
        case 'image':
            return '📷 Image';
        case 'video':
            return '🎥 Video';
        case 'audio':
            return '🎵 Audio message';
        case 'voice':
            return '🎵 Audio message';
        case 'document':
            return opts.filename ? `📄 Document: ${opts.filename}` : '📄 Document';
        case 'sticker':
            return '😊 Sticker';
        case 'location':
            return '📍 Location';
        case 'interactive':
            return opts.interactiveTitle || '↩ Reply';
        case 'button':
            return opts.text ? '↩ ' + opts.text : '↩ Reply';
        case 'reaction':
            return opts.emoji ? `${opts.emoji} Reaction` : '👍 Reaction';
        case 'contacts':
            return '👤 Contact';
        default:
            return '⚠️ Unsupported message type';
    }
}

// Live Chat handler — processes an incoming message through the live chat pipeline
async function handleLiveChat(tenantId, message, profileName, from) {
    const msgType = message.type || 'unsupported';

    // Extract structured fields per message type
    let msgText = '';
    let mediaUrl = null;
    let interactivePayload = null;

    const MEDIA_TYPES = ['image', 'video', 'audio', 'voice', 'document', 'sticker'];

    if (msgType === 'text') {
        msgText = message.text?.body || '';
    } else if (MEDIA_TYPES.includes(msgType)) {
        mediaUrl = message[msgType]?.id || null;
        // For document, capture filename in text for display
        if (msgType === 'document' && message.document?.filename) {
            msgText = message.document.filename;
        }
    } else if (msgType === 'interactive') {
        const interactive = message.interactive || {};
        if (interactive.type === 'button_reply') {
            interactivePayload = {
                type: 'button_reply',
                title: interactive.button_reply?.title || '',
                id: interactive.button_reply?.id || '',
            };
        } else if (interactive.type === 'list_reply') {
            interactivePayload = {
                type: 'list_reply',
                title: interactive.list_reply?.title || '',
                id: interactive.list_reply?.id || '',
            };
        }
    } else if (msgType === 'button') {
        msgText = message.button?.text || message.button?.payload || '⚠️ Unsupported message type';
    } else if (msgType === 'location') {
        const lat = message.location?.latitude;
        const lng = message.location?.longitude;
        msgText = (lat != null && lng != null) ? `Lat: ${lat}, Lng: ${lng}` : '';
    } else if (msgType === 'reaction') {
        msgText = message.reaction?.emoji || '';
    } else if (msgType === 'contacts') {
        msgText = '';
    } else {
        msgText = '⚠️ Unsupported message type';
    }

    // Build the lastMessage preview string
    const previewOpts = {
        text: msgText,
        filename: msgType === 'document' ? message.document?.filename : undefined,
        emoji: msgType === 'reaction' ? message.reaction?.emoji : undefined,
        interactiveTitle: interactivePayload?.title,
    };
    const lastMessagePreview = buildPreview(msgType, previewOpts);

    console.log(` [Tenant: ${tenantId}] Incoming from ${from} (${profileName}): ${lastMessagePreview}`);

    await Conversation.findOneAndUpdate(
        { tenantId, contactId: from },
        { name: profileName, lastMessage: lastMessagePreview, lastActive: new Date(), hasReply: true },
        { upsert: true }
    );
    await Message.create({
        tenantId,
        contactId: from,
        text: msgText,
        isMe: false,
        time: new Date().toISOString(),
        messageType: msgType,
        mediaUrl,
        interactivePayload,
    });
    await broadcastConversations(tenantId);
    await broadcastMessages(tenantId, from);
}

//  Incoming Webhook (Messages & Status Updates) 
app.post('/webhook', async (req, res) => {
    const body = req.body;
    console.log(' Incoming Webhook:', JSON.stringify(body, null, 2));

    if (body.object !== 'whatsapp_business_account') return res.sendStatus(404);

    // ── Step 3: Handle account_update PARTNER_ADDED event & message_template_status_update ──
    for (const entry of body.entry || []) {
        for (const change of entry.changes || []) {
            const val = change.value;
            if (change.field === 'account_update' && val?.event === 'PARTNER_ADDED') {
                res.sendStatus(200); // Respond immediately to Meta
                const wabaId = val.waba_id || entry.id;
                const businessPortfolioId = val.business_portfolio_id || null;

                await saveOnboardingLog({
                    tenantId: null,
                    sessionId: null,
                    wabaId,
                    businessPortfolioId,
                    step: 'WEBHOOK_PARTNER_ADDED',
                    status: 'info',
                    message: `Received account_update PARTNER_ADDED webhook from Meta. Triggering background onboarding...`,
                    details: val
                });

                // Trigger Steps 4→5→6 asynchronously (null tenantId = match by WABA ID)
                processOnboarding(wabaId, businessPortfolioId, null).catch(async (err) => {
                    await saveOnboardingLog({
                        tenantId: null,
                        sessionId: null,
                        wabaId,
                        businessPortfolioId,
                        step: 'WEBHOOK_PROCESS_ONBOARDING_FAIL',
                        status: 'error',
                        message: `Background onboarding triggered by webhook failed: ${err.message}`,
                        details: err.stack
                    });
                });
                return; // already sent 200
            }

            if (change.field === 'message_template_status_update') {
                res.sendStatus(200); // Respond immediately to Meta
                const wabaId = entry.id;
                const templateName = val?.message_template_name;
                const eventStatus = val?.event; // APPROVED / REJECTED / PENDING / etc.
                const reason = val?.reason || null;
                const lang = val?.message_template_language || null;

                console.log(`[Webhook] Template Status Update for WABA ${wabaId}: ${templateName} -> ${eventStatus} (Reason: ${reason})`);

                Tenant.findOne({ 'whatsappConfig.businessAccountId': wabaId }).then(async (tenant) => {
                    if (tenant) {
                        const tenantId = tenant._id.toString();
                        
                        // Update our local cache of template rejection reasons if template was rejected
                        if (eventStatus === 'REJECTED' && reason) {
                            if (!tenant.whatsappConfig.templateRejections) {
                                tenant.whatsappConfig.templateRejections = new Map();
                            }
                            tenant.whatsappConfig.templateRejections.set(templateName, reason);
                            await tenant.save();
                        } else if (eventStatus === 'APPROVED') {
                            // If it's approved, we can remove any existing rejection reason
                            if (tenant.whatsappConfig.templateRejections) {
                                tenant.whatsappConfig.templateRejections.delete(templateName);
                                await tenant.save();
                            }
                        }

                        // Emit socket event to notify frontend
                        io.to(tenantId).emit('template_status_update', {
                            name: templateName,
                            status: eventStatus,
                            reason: reason,
                            language: lang
                        });
                        console.log(`[Webhook] Emitted template_status_update to tenant ${tenantId}`);
                    } else {
                        console.warn(`[Webhook] Received template status update for WABA ${wabaId} but no tenant found.`);
                    }
                }).catch(err => {
                    console.error('[Webhook] Error handling template status update:', err);
                });
                return; // already sent 200
            }
        }
    }

    try {
        const entry = body.entry[0];
        const value = entry.changes[0].value;
        const receiverPhoneNumberId = value.metadata?.phone_number_id;


        // Handle Status Updates
        if (value.statuses) {
            const statusUpdate = value.statuses[0];
            const wamid = statusUpdate.id;
            const status = statusUpdate.status;
            if (!wamid) return res.sendStatus(200);

            console.log(JSON.stringify({
                service: 'webhook',
                event: 'delivery_status_received',
                wamid,
                status,
                timestamp: new Date().toISOString()
            }));

            const mapping = await StatusMapping.findOne({ wamid });
            if (mapping?.tenantId) {
                if (mapping.campaignId) {
                    const inc = {};
                    if (status === 'delivered') inc.deliveredCount = 1;
                    if (status === 'read') { inc.readCount = 1; inc.deliveredCount = -1; }
                    if (status === 'failed') inc.failureCount = 1;

                    if (Object.keys(inc).length > 0) {
                        await Campaign.findOneAndUpdate({ tenantId: mapping.tenantId, id: mapping.campaignId }, { $inc: inc });
                    }
                }

                if (mapping.campaignId) {
                    const recipientUpdate = { status };
                    if (status === 'delivered') recipientUpdate.deliveredAt = new Date().toISOString();
                    if (status === 'read') recipientUpdate.readAt = new Date().toISOString();
                    if (status === 'failed') recipientUpdate.failedAt = new Date().toISOString();
                    await Recipient.findOneAndUpdate({ wamid }, { $set: recipientUpdate });

                    // Phase tracking: record delivery phase for successfully delivered messages.
                    // recordDelivery is idempotent — it only sets phaseNumber when it is still null,
                    // so duplicate webhooks and delayed webhooks are handled safely.
                    if (status === 'delivered' || status === 'read') {
                        try {
                            const campaign = await Campaign.findOne(
                                { tenantId: mapping.tenantId, id: mapping.campaignId },
                                { currentPhase: 1 }
                            );
                            if (campaign) {
                                const deliveryTimestamp = statusUpdate.timestamp
                                    ? new Date(parseInt(statusUpdate.timestamp, 10) * 1000)
                                    : new Date();
                                await messageTracker.recordDelivery(
                                    wamid,
                                    campaign.currentPhase,
                                    deliveryTimestamp
                                );
                                console.log(JSON.stringify({
                                    service: 'webhook',
                                    event: 'delivery_phase_recorded',
                                    wamid,
                                    campaignId: mapping.campaignId,
                                    phaseNumber: campaign.currentPhase,
                                    deliveryStatus: status,
                                    timestamp: new Date().toISOString()
                                }));
                            }
                        } catch (trackErr) {
                            // Non-fatal: log but don't fail the webhook response
                            console.error(JSON.stringify({
                                service: 'webhook',
                                event: 'delivery_phase_record_failed',
                                wamid,
                                campaignId: mapping.campaignId,
                                error: trackErr.message,
                                stack: trackErr.stack,
                                timestamp: new Date().toISOString()
                            }));
                        }
                    }

                    await broadcastCampaigns(mapping.tenantId);
                }
            }
        }

        // Handle Incoming Messages
        if (value.messages) {
            let tenantId = null;
            let tenant = null;
            if (receiverPhoneNumberId) {
                tenant = await Tenant.findOne({ 'whatsappConfig.phoneNumberId': receiverPhoneNumberId });
                if (tenant) tenantId = tenant._id.toString();
            }

            if (!tenantId) {
                console.warn(` Could not map incoming message to a tenant (Phone: ${receiverPhoneNumberId})`);
            } else {
                const message = value.messages[0];
                const from = message.from;
                const profileName = value.contacts?.[0]?.profile?.name || 'Unknown Sender';

                await chatbotEngineProcessMessage(tenantId, message, profileName, from, tenant);
            }
        }

        res.sendStatus(200);
    } catch (error) {
        console.error(' Error processing webhook:', error);
        res.sendStatus(500);
    }
});

//  Socket.IO 
io.use((socket, next) => {
    const token = socket.handshake.auth?.token;
    if (!token) return next(new Error('Unauthorized'));
    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) return next(new Error('Unauthorized'));
        socket.user = user;
        next();
    });
});

io.on('connection', (socket) => {
    console.log(` Socket connected: ${socket.id}`);

    socket.on('join', async (tenantId) => {
        socket.join(tenantId);
        console.log(` Tenant ${tenantId} joined socket room`);

        // Send initial conversations
        try {
            const convs = await Conversation.find({ tenantId, hasReply: true })
                .sort({ lastActive: -1 }).lean();
            const formattedConvs = convs.map(function (c) {
                return { id: c.contactId, name: c.name || c.contactId, lastMessage: c.lastMessage || "", lastActive: c.lastActive, hasReply: c.hasReply || false };
            });
            socket.emit("conversations_update", formattedConvs);
        } catch (e) { console.error("Socket init conversations error:", e); }

        // Send initial campaigns
        try {
            const campaigns = await Campaign.find({ tenantId })
                .sort({ timestamp: -1 }).lean();
            const campaignIds = campaigns.map(c => c.id);
            const pendingPhases = await ScheduledRetryPhase.find({
                campaignId: { $in: campaignIds },
                status: { $in: ['pending', 'executing'] }
            }).select('campaignId').lean();
            const pendingSet = new Set(pendingPhases.map(p => p.campaignId));
            const enriched = campaigns.map(c => ({ ...c, hasPendingRetry: pendingSet.has(c.id) }));
            socket.emit('campaigns_update', enriched);
        } catch (e) { console.error('Socket init campaigns error:', e); }
    });

    socket.on('get_messages', async (contactId) => {
        try {
            const tenantId = socket.user && socket.user.tenantId;
            if (!tenantId || !contactId) return;
            const messages = await Message.find({ tenantId, contactId })
                .sort({ timestamp: 1 }).lean();
            const formatted = messages.map(function (m) {
                return {
                    id: m._id.toString(),
                    text: m.text || "",
                    isMe: m.isMe === true,
                    timestamp: m.timestamp,
                    time: m.time || new Date(m.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
                    messageType: m.messageType || null,
                    templateName: m.templateName || null,
                    templateBody: m.templateBody || null,
                    mediaUrl: m.mediaUrl || null,
                    interactivePayload: m.interactivePayload || null,
                };
            });
            socket.emit('messages_' + contactId, formatted);
        } catch (e) { console.error('get_messages error:', e); }
    });

    socket.on('disconnect', () => {
        console.log(` Socket disconnected: ${socket.id}`);
    });
});

//  Broadcast Helpers 
async function broadcastConversations(tenantId) {
    try {
        const convs = await Conversation.find({ tenantId })
            .sort({ lastActive: -1 }).lean();
        const formatted = convs.map(function (c) {
            return {
                id: c.contactId,
                name: c.name || c.contactId,
                lastMessage: c.lastMessage || "",
                lastActive: c.lastActive,
                hasReply: c.hasReply || false
            };
        });
        io.to(tenantId).emit("conversations_update", formatted);
    } catch (e) { console.error("broadcastConversations error:", e); }
}

async function broadcastMessages(tenantId, contactId) {
    try {
        const messages = await Message.find({ tenantId, contactId })
            .sort({ timestamp: 1 }).lean();
        const formatted = messages.map(function (m) {
            return {
                id: m._id.toString(),
                text: m.text || "",
                isMe: m.isMe === true,
                timestamp: m.timestamp,
                time: m.time || new Date(m.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
                messageType: m.messageType || null,
                templateName: m.templateName || null,
                templateBody: m.templateBody || null,
                mediaUrl: m.mediaUrl || null,
                interactivePayload: m.interactivePayload || null,
            };
        });
        io.to(tenantId).emit("messages_" + contactId, formatted);
    } catch (e) { console.error("broadcastMessages error:", e); }
}

async function broadcastCampaigns(tenantId) {
    try {
        const campaigns = await Campaign.find({ tenantId })
            .sort({ timestamp: -1 }).lean();
        const campaignIds = campaigns.map(c => c.id);
        const pendingPhases = await ScheduledRetryPhase.find({
            campaignId: { $in: campaignIds },
            status: { $in: ['pending', 'executing'] }
        }).select('campaignId').lean();
        const pendingSet = new Set(pendingPhases.map(p => p.campaignId));
        const enriched = campaigns.map(c => ({ ...c, hasPendingRetry: pendingSet.has(c.id) }));
        io.to(tenantId).emit('campaigns_update', enriched);
    } catch (e) { console.error('broadcastCampaigns error:', e); }
}

//  Scheduled Campaign Runner (checks every minute) 
async function runScheduledCampaigns() {
    try {
        const now = new Date();
        const due = await ScheduledCampaign.find({ status: 'pending', scheduledAt: { $lte: now } });
        for (const sc of due) {
            await ScheduledCampaign.updateOne({ _id: sc._id }, { $set: { status: 'running' } });
            const tenant = await Tenant.findById(sc.tenantId);
            if (!tenant) {
                await ScheduledCampaign.updateOne({ _id: sc._id }, { $set: { status: 'failed', errorMessage: 'Tenant not found' } });
                continue;
            }
            const config = tenant.whatsappConfig;
            if (!config?.accessToken || !config?.phoneNumberId) {
                await ScheduledCampaign.updateOne({ _id: sc._id }, { $set: { status: 'failed', errorMessage: 'WhatsApp not configured' } });
                continue;
            }
            const campaignId = `sched_${sc._id}`;
            let success = 0, failure = 0;
            for (const recipient of sc.recipients) {
                try {
                    const components = [];
                    if (sc.mediaId && sc.mediaType) {
                        components.push({ type: 'header', parameters: [{ type: sc.mediaType.toLowerCase(), [sc.mediaType.toLowerCase()]: { id: sc.mediaId } }] });
                    }
                    const vars = recipient.variables || {};
                    const bodyParams = Object.keys(vars).sort().map(k => ({ type: 'text', text: vars[k] }));
                    if (bodyParams.length) components.push({ type: 'body', parameters: bodyParams });
                    const payload = {
                        messaging_product: 'whatsapp', to: recipient.mobileNumber, type: 'template',
                        template: { name: sc.template, language: { code: sc.language || 'en_US' }, ...(components.length ? { components } : {}) }
                    };
                    const resp = await axios.post(`${WHATSAPP_API_URL}/${config.phoneNumberId}/messages`, payload, {
                        headers: { Authorization: `Bearer ${config.accessToken}`, 'Content-Type': 'application/json' }
                    });
                    if (resp.data?.messages?.[0]?.id) {
                        success++;
                    } else { failure++; }
                } catch { failure++; }
            }
            await ScheduledCampaign.updateOne({ _id: sc._id }, { $set: { status: 'completed', resultCampaignId: campaignId, successCount: success, failureCount: failure } });
            console.log(` Scheduled campaign ${sc._id} completed: ${success} sent, ${failure} failed`);
        }
    } catch (err) {
        console.error(' Scheduled campaign runner error:', err.message);
    }
}
setInterval(runScheduledCampaigns, 60 * 1000);

// ── Lead Trigger CRUD Routes ──────────────────────────────────────────────────────

// 9.1 — POST /api/leads/triggers — create trigger
app.post('/api/leads/triggers', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const { source, formName, action, templateName, templateLanguage, mediaId, mediaType, variableMapping, chatbotId, isActive } = req.body;
        if (!action) return res.status(400).json({ error: 'action is required' });
        const trigger = await LeadTrigger.create({
            tenantId,
            source: source || 'any',
            formName: formName || '',
            action,
            templateName: templateName || '',
            templateLanguage: templateLanguage || 'en_US',
            mediaId: mediaId || '',
            mediaType: mediaType || '',
            variableMapping: variableMapping || {},
            chatbotId: chatbotId || '',
            isActive: isActive !== undefined ? isActive : true,
        });
        res.status(201).json(trigger);
    } catch (err) {
        console.error('POST /api/leads/triggers error:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 9.2 — GET /api/leads/triggers — list triggers for tenant
// NOTE: Must be placed BEFORE /api/leads/:id
app.get('/api/leads/triggers', authenticate, async (req, res) => {
    try {
        const triggers = await LeadTrigger.find({ tenantId: req.user.tenantId }).sort({ createdAt: -1 });
        res.json(triggers);
    } catch (err) {
        console.error('GET /api/leads/triggers error:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 9.3 — PUT /api/leads/triggers/:id — update trigger with ownership check
app.put('/api/leads/triggers/:id', authenticate, async (req, res) => {
    try {
        const trigger = await LeadTrigger.findById(req.params.id);
        if (!trigger) return res.status(404).json({ error: 'Not found' });
        if (trigger.tenantId !== req.user.tenantId) return res.status(403).json({ error: 'Forbidden' });
        const { source, formName, action, templateName, templateLanguage, mediaId, mediaType, variableMapping, chatbotId, isActive } = req.body;
        if (source !== undefined) trigger.source = source;
        if (formName !== undefined) trigger.formName = formName;
        if (action !== undefined) trigger.action = action;
        if (templateName !== undefined) trigger.templateName = templateName;
        if (templateLanguage !== undefined) trigger.templateLanguage = templateLanguage;
        if (mediaId !== undefined) trigger.mediaId = mediaId;
        if (mediaType !== undefined) trigger.mediaType = mediaType;
        if (variableMapping !== undefined) trigger.variableMapping = variableMapping;
        if (chatbotId !== undefined) trigger.chatbotId = chatbotId;
        if (isActive !== undefined) trigger.isActive = isActive;
        await trigger.save();
        res.json(trigger);
    } catch (err) {
        console.error('PUT /api/leads/triggers/:id error:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 9.4 — DELETE /api/leads/triggers/:id — delete trigger with ownership check
app.delete('/api/leads/triggers/:id', authenticate, async (req, res) => {
    try {
        const trigger = await LeadTrigger.findById(req.params.id);
        if (!trigger) return res.status(404).json({ error: 'Not found' });
        if (trigger.tenantId !== req.user.tenantId) return res.status(403).json({ error: 'Forbidden' });
        await trigger.deleteOne();
        res.json({ success: true });
    } catch (err) {
        console.error('DELETE /api/leads/triggers/:id error:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// ── Lead Management Routes ──────────────────────────────────────────────────────

// 8.1 — GET /api/leads — full list with filters, tenant-scoped (no pagination)
app.get('/api/leads', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const { source, status, startDate, endDate, search } = req.query;

        const query = { tenantId };
        if (source) query.source = source;
        if (status) query.status = status;
        if (startDate || endDate) {
            query.createdAt = {};
            if (startDate) query.createdAt.$gte = new Date(startDate);
            if (endDate) query.createdAt.$lte = new Date(endDate);
        }
        if (search) {
            query.$or = [
                { name: { $regex: search, $options: 'i' } },
                { mobileNumber: { $regex: search, $options: 'i' } },
            ];
        }

        const leads = await Lead.find(query).sort({ createdAt: -1 }).lean();

        res.json({ leads, total: leads.length });
    } catch (err) {
        console.error('GET /api/leads error:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 8.2 — GET /api/leads/analytics — aggregate counts by source, status, and daily buckets
// NOTE: Must be placed BEFORE /api/leads/:id to avoid Express matching 'analytics' as :id
app.get('/api/leads/analytics', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const days = parseInt(req.query.days) || 30;
        const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

        const [bySource, byStatus, byDay] = await Promise.all([
            Lead.aggregate([
                { $match: { tenantId, createdAt: { $gte: since } } },
                { $group: { _id: '$source', count: { $sum: 1 } } },
            ]),
            Lead.aggregate([
                { $match: { tenantId, createdAt: { $gte: since } } },
                { $group: { _id: '$status', count: { $sum: 1 } } },
            ]),
            Lead.aggregate([
                { $match: { tenantId, createdAt: { $gte: since } } },
                {
                    $group: {
                        _id: { $dateToString: { format: '%Y-%m-%d', date: '$createdAt' } },
                        shopify: { $sum: { $cond: [{ $eq: ['$source', 'shopify'] }, 1, 0] } },
                        wordpress: { $sum: { $cond: [{ $eq: ['$source', 'wordpress'] }, 1, 0] } },
                        total: { $sum: 1 },
                    },
                },
                { $sort: { _id: 1 } },
            ]),
        ]);

        res.json({ bySource, byStatus, byDay });
    } catch (err) {
        console.error('GET /api/leads/analytics error:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 8.3 — GET /api/leads/:id — single lead with tenant ownership check
app.get('/api/leads/:id', authenticate, async (req, res) => {
    try {
        const lead = await Lead.findById(req.params.id).lean();
        if (!lead) return res.status(404).json({ error: 'Not found' });
        if (lead.tenantId !== req.user.tenantId) return res.status(403).json({ error: 'Not found' });
        res.json(lead);
    } catch (err) {
        console.error('GET /api/leads/:id error:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 8.4 — PATCH /api/leads/:id/status — update status with enum validation and tenant ownership check
const VALID_LEAD_STATUSES = ['new', 'contacted', 'converted', 'failed'];

app.patch('/api/leads/:id/status', authenticate, async (req, res) => {
    try {
        const { status } = req.body;
        if (!VALID_LEAD_STATUSES.includes(status)) {
            return res.status(400).json({ error: 'Invalid status value' });
        }
        const lead = await Lead.findById(req.params.id);
        if (!lead) return res.status(404).json({ error: 'Not found' });
        if (lead.tenantId !== req.user.tenantId) return res.status(403).json({ error: 'Not found' });
        lead.status = status;
        await lead.save();
        res.json(lead);
    } catch (err) {
        console.error('PATCH /api/leads/:id/status error:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 8.4b — DELETE /api/leads/:id — delete a lead (tenant-scoped)
app.delete('/api/leads/:id', authenticate, async (req, res) => {
    try {
        const lead = await Lead.findById(req.params.id);
        if (!lead) return res.status(404).json({ error: 'Not found' });
        if (lead.tenantId !== req.user.tenantId) return res.status(403).json({ error: 'Forbidden' });
        await Lead.findByIdAndDelete(req.params.id);
        res.json({ success: true });
    } catch (err) {
        console.error('DELETE /api/leads/:id error:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 8.5 — POST /api/leads/:id/merge — merge duplicate lead into linked Client
app.post('/api/leads/:id/merge', authenticate, async (req, res) => {
    try {
        const lead = await Lead.findById(req.params.id);
        if (!lead) return res.status(404).json({ error: 'Not found' });
        if (lead.tenantId !== req.user.tenantId) return res.status(403).json({ error: 'Not found' });
        if (!lead.isDuplicate) return res.status(400).json({ error: 'Lead is not a duplicate' });

        const updateFields = {};
        if (lead.name) updateFields.name = lead.name;
        if (lead.email) updateFields.emailId = lead.email;
        if (lead.companyName) updateFields.companyName = lead.companyName;

        await Client.findByIdAndUpdate(lead.clientId, { $set: updateFields });
        res.json({ success: true });
    } catch (err) {
        console.error('POST /api/leads/:id/merge error:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// ── Auto-Trigger Service ──────────────────────────────────────────────────────

/**
 * Evaluates active LeadTriggers for a tenant and dispatches actions.
 * Called asynchronously after lead ingestion.
 * Requirements: 7.1-7.8
 */
async function evaluateTriggers(tenantId, lead) {
    // 7.5 — Skip for duplicate leads
    if (lead.isDuplicate) return;

    // 7.1 — Query matching active triggers
    const triggers = await LeadTrigger.find({
        tenantId,
        isActive: true,
        $or: [
            { source: 'any' },
            { source: lead.source },
        ],
    });

    const matchingTriggers = triggers.filter(trigger => {
        // formName filter: if trigger has formName set, it must match lead's formName
        if (trigger.formName && trigger.formName !== lead.formName) return false;
        return true;
    });

    for (const trigger of matchingTriggers) {
        try {
            if (trigger.action === 'send_template') {
                await dispatchTemplate(tenantId, lead, trigger);
            } else if (trigger.action === 'start_chatbot') {
                await dispatchChatbot(tenantId, lead, trigger);
            }
        } catch (err) {
            console.error(`[AutoTrigger] Error executing trigger ${trigger._id}:`, err.message);
        }
    }
}

/**
 * Extracts {{N}} variable count from a template's BODY component text.
 */
function extractVariableCount(components) {
    const body = (components || []).find(c => c.type === 'BODY');
    if (!body || !body.text) return 0;
    const matches = body.text.match(/\{\{\d+\}\}/g) || [];
    // unique indices
    const indices = new Set(matches.map(m => parseInt(m.replace(/\D/g, ''))));
    return indices.size;
}

/**
 * Resolves variable values from a data object using a variableMapping.
 * variableMapping: {"1": "name", "2": "mobileNumber", "3": "companyName"}
 * Returns array of {type:'text', text:'value'} sorted by key index.
 */
function resolveVariables(variableMapping, dataObj) {
    if (!variableMapping || !Object.keys(variableMapping).length) return [];
    return Object.keys(variableMapping)
        .sort((a, b) => parseInt(a) - parseInt(b))
        .map(k => {
            const field = variableMapping[k];
            const value = dataObj[field] ?? '';
            return { type: 'text', text: String(value) };
        });
}

/**
 * Fetches template components from Meta for a given tenant + template name.
 * Returns the components array or [] on failure.
 */
async function fetchTemplateComponents(tenant, templateName) {
    try {
        const { accessToken, businessAccountId } = tenant.whatsappConfig;
        const resp = await axios.get(
            `${WHATSAPP_API_URL}/${businessAccountId}/message_templates`,
            { headers: { Authorization: `Bearer ${accessToken}` }, params: { name: templateName } }
        );
        const tpl = (resp.data?.data || []).find(t => t.name === templateName);
        return tpl?.components || [];
    } catch (_) {
        return [];
    }
}

/**
 * 7.2 — Send WhatsApp template via Meta Cloud API v25.0
 */
async function dispatchTemplate(tenantId, lead, trigger) {
    // 7.6 — Skip if whatsappConfig is incomplete
    const tenant = await Tenant.findById(tenantId);
    if (!tenant || !tenant.whatsappConfig || !tenant.whatsappConfig.phoneNumberId || !tenant.whatsappConfig.accessToken) {
        console.warn(`[AutoTrigger] Tenant ${tenantId} has incomplete whatsappConfig — skipping dispatch`);
        return;
    }

    const { phoneNumberId, accessToken } = tenant.whatsappConfig;
    const url = `${WHATSAPP_API_URL}/${phoneNumberId}/messages`;

    // Resolve body variables
    const components = await fetchTemplateComponents(tenant, trigger.templateName);
    const varCount = extractVariableCount(components);
    const mapping = trigger.variableMapping || {};

    // Auto-fill any unmapped positions with lead fields in order
    const autoFields = ['name', 'mobileNumber', 'email', 'companyName', 'formName'];
    const resolvedMapping = { ...mapping };
    if (varCount > 0) {
        let autoIdx = 0;
        for (let i = 1; i <= varCount; i++) {
            if (!resolvedMapping[String(i)]) {
                resolvedMapping[String(i)] = autoFields[autoIdx] || 'name';
                autoIdx++;
            }
        }
    }

    const bodyParams = resolveVariables(resolvedMapping, {
        name: lead.name || '',
        mobileNumber: lead.mobileNumber || '',
        email: lead.email || '',
        companyName: lead.companyName || '',
        formName: lead.formName || '',
        source: lead.source || '',
        ...Object.fromEntries(
            Object.entries(lead.metadata || {}).map(([k, v]) => [`metadata.${k}`, v])
        ),
    });

    const body = {
        messaging_product: 'whatsapp',
        to: lead.mobileNumber,
        type: 'template',
        template: {
            name: trigger.templateName,
            language: { code: trigger.templateLanguage || 'en_US' },
        },
    };

    const templateComponents = [];
    if (trigger.mediaId) {
        const mediaType = (trigger.mediaType || 'image').toLowerCase();
        templateComponents.push({
            type: 'header',
            parameters: [{ type: mediaType, [mediaType]: { id: trigger.mediaId } }],
        });
    }
    if (bodyParams.length) {
        templateComponents.push({ type: 'body', parameters: bodyParams });
    }
    if (templateComponents.length) {
        body.template.components = templateComponents;
    }

    try {
        await axios.post(url, body, {
            headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
        });
        // 7.4 — Update status to contacted on success
        await Lead.findByIdAndUpdate(lead._id, { status: 'contacted' });

        // Store outbound template message so it appears in the conversation/chat screen
        // Extract template body text from the already-fetched components
        const bodyComponent = (components || []).find(c => c.type === 'BODY');
        const templateBodyText = bodyComponent?.text || null;
        const contactName = lead.name || lead.mobileNumber;
        const previewText = buildPreview('template', { templateName: trigger.templateName, templateBody: templateBodyText });

        await Conversation.findOneAndUpdate(
            { tenantId, contactId: lead.mobileNumber },
            { name: contactName, lastMessage: previewText, lastActive: new Date() },
            { upsert: true }
        );
        await Message.create({
            tenantId,
            contactId: lead.mobileNumber,
            text: templateBodyText || '',
            isMe: true,
            time: new Date().toISOString(),
            messageType: 'template',
            templateName: trigger.templateName,
            templateBody: templateBodyText,
        });
        await broadcastConversations(tenantId);
        await broadcastMessages(tenantId, lead.mobileNumber);
    } catch (err) {
        const metaCode = err.response?.data?.error?.code || 'unknown';
        console.error(`[AutoTrigger] Meta API error for lead ${lead._id}: code=${metaCode}`, err.response?.data);
        // 7.4 — Update status to failed on Meta API error
        await Lead.findByIdAndUpdate(lead._id, { status: 'failed' });
    }
}

/**
 * 7.3 — Start chatbot session via existing ChatbotSession model
 */
async function dispatchChatbot(tenantId, lead, trigger) {
    if (!trigger.chatbotId) {
        console.warn(`[AutoTrigger] Trigger ${trigger._id} has no chatbotId — skipping`);
        return;
    }

    try {
        // Create or reuse ChatbotSession (unique index on tenantId+contactId)
        await ChatbotSession.findOneAndUpdate(
            { tenantId, contactId: lead.mobileNumber },
            {
                tenantId,
                contactId: lead.mobileNumber,
                chatbotId: trigger.chatbotId,
                currentNodeId: 'start',
            },
            { upsert: true, new: true }
        );
        // 7.4 — Update status to contacted
        await Lead.findByIdAndUpdate(lead._id, { status: 'contacted' });

        // Store chatbot session start so it appears in the conversation/chat screen
        const chatbotLabel = '🤖 Chatbot session started';
        const contactName = lead.name || lead.mobileNumber;
        await Conversation.findOneAndUpdate(
            { tenantId, contactId: lead.mobileNumber },
            { name: contactName, lastMessage: chatbotLabel, lastActive: new Date() },
            { upsert: true }
        );
        await Message.create({
            tenantId,
            contactId: lead.mobileNumber,
            text: chatbotLabel,
            isMe: true,
            time: new Date().toISOString(),
        });
        await broadcastConversations(tenantId);
        await broadcastMessages(tenantId, lead.mobileNumber);
    } catch (err) {
        console.error(`[AutoTrigger] Chatbot dispatch error for lead ${lead._id}:`, err.message);
        await Lead.findByIdAndUpdate(lead._id, { status: 'failed' });
    }
}

// ── Client Auto-Trigger Service ──────────────────────────────────────────────

/**
 * Evaluates the active ClientTrigger for a tenant and dispatches a WhatsApp
 * template message to the newly created client.
 * Called asynchronously (via setImmediate) after client creation.
 * Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 6.1, 6.2
 */
async function evaluateClientTrigger(tenantId, client) {
    try {
        // Step 1: Query active ClientTrigger for tenant; return early if none found
        const trigger = await ClientTrigger.findOne({ tenantId, isActive: true });
        if (!trigger) return;

        // Step 2: Load tenant; return early if whatsappConfig is incomplete
        const tenant = await Tenant.findById(tenantId);
        if (!tenant || !tenant.whatsappConfig || !tenant.whatsappConfig.phoneNumberId || !tenant.whatsappConfig.accessToken) {
            console.warn(`[ClientAutoTrigger] Tenant ${tenantId} has incomplete whatsappConfig — skipping dispatch`);
            return;
        }

        const { phoneNumberId, accessToken } = tenant.whatsappConfig;
        const url = `${WHATSAPP_API_URL}/${phoneNumberId}/messages`;

        // Step 3: POST to Meta Cloud API v25.0
        const tplComponents = await fetchTemplateComponents(tenant, trigger.templateName);
        const varCount = extractVariableCount(tplComponents);
        const mapping = trigger.variableMapping || {};

        // Auto-fill unmapped positions with client fields in order
        const autoFields = ['name', 'mobileNumber', 'companyName', 'emailId', 'venue', 'remark'];
        const resolvedMapping = { ...mapping };
        if (varCount > 0) {
            let autoIdx = 0;
            for (let i = 1; i <= varCount; i++) {
                if (!resolvedMapping[String(i)]) {
                    resolvedMapping[String(i)] = autoFields[autoIdx] || 'name';
                    autoIdx++;
                }
            }
        }

        const bodyParams = resolveVariables(resolvedMapping, {
            name: client.name || '',
            mobileNumber: client.mobileNumber || '',
            companyName: client.companyName || '',
            emailId: client.emailId || '',
            venue: client.venue || '',
            remark: client.remark || '',
        });

        const templatePayload = {
            name: trigger.templateName,
            language: { code: trigger.templateLanguage || 'en_US' },
        };

        const tplComponentsOut = [];
        if (trigger.mediaId) {
            const mediaType = (trigger.mediaType || 'image').toLowerCase();
            tplComponentsOut.push({
                type: 'header',
                parameters: [{ type: mediaType, [mediaType]: { id: trigger.mediaId } }],
            });
        }
        if (bodyParams.length) {
            tplComponentsOut.push({ type: 'body', parameters: bodyParams });
        }
        if (tplComponentsOut.length) {
            templatePayload.components = tplComponentsOut;
        }

        const response = await axios.post(
            url,
            {
                messaging_product: 'whatsapp',
                to: client.mobileNumber,
                type: 'template',
                template: templatePayload,
            },
            {
                headers: {
                    Authorization: `Bearer ${accessToken}`,
                    'Content-Type': 'application/json',
                },
            }
        );

        const wamid = response.data.messages[0].id;

        // Step 5: Create StatusMapping record for tracking message delivery status
        try {
            await StatusMapping.findOneAndUpdate(
                { wamid },
                { wamid, tenantId, campaignId: null, to: client.mobileNumber },
                { upsert: true }
            );
        } catch (err) {
            console.error(`[ClientAutoTrigger] StatusMapping creation failed:`, err.message);
        }

        // Step 6: Upsert Conversation and create Message document
        const templateLabel = `📋 Template: ${trigger.templateName}`;
        const contactName = client.name || client.mobileNumber;
        await Conversation.findOneAndUpdate(
            { tenantId, contactId: client.mobileNumber },
            { name: contactName, lastMessage: templateLabel, lastActive: new Date() },
            { upsert: true }
        );
        await Message.create({
            tenantId,
            contactId: client.mobileNumber,
            text: templateLabel,
            isMe: true,
            role: 'assistant',
            type: 'template',
            time: new Date().toISOString(),
        });
        await broadcastConversations(tenantId);
        await broadcastMessages(tenantId, client.mobileNumber);

    } catch (err) {
        // Step 7: Catch all errors, log them, never rethrow
        const metaCode = err.response?.data?.error?.code || 'unknown';
        console.error(`[ClientAutoTrigger] Error for tenant ${tenantId}, client ${client.mobileNumber}: code=${metaCode}`, err.response?.data || err.message);
    }
}

// ── Lead Webhook Route ────────────────────────────────────────────────────────
const rateLimit = require('express-rate-limit');
const { encryptSecret, decryptSecret, maskSecret } = require('./cryptoUtils');
const { parseLeadPayload } = require('./leadPayloadParser');
const { normaliseMobileNumber, validateMobileNumber } = require('./mobileUtils');
const { sanitiseLeadFields } = require('./fieldSanitiser');

// 6.1 — Per-tenant rate limiter: 100 req/min keyed on :tenantId
const webhookRateLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 100,
    keyGenerator: (req) => req.params.tenantId || req.ip,
    handler: (req, res) => {
        res.status(429).set('Retry-After', '60').json({ error: 'Too many requests' });
    },
    standardHeaders: false,
    legacyHeaders: false,
});

// POST /api/leads/webhook/:tenantId (public — no JWT auth)
// 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8
app.post('/api/leads/webhook/:tenantId', webhookRateLimiter, async (req, res) => {
    const { tenantId } = req.params;
    const sourceIp = req.ip || req.connection.remoteAddress;
    let httpStatus = 200;

    try {
        // 6.2 — Tenant lookup
        const tenant = await Tenant.findById(tenantId);
        if (!tenant) {
            httpStatus = 404;
            await WebhookLog.create({ tenantId, sourceIp, httpStatus });
            return res.status(404).json({ error: 'Tenant not found' });
        }

        // 6.2 — Decrypt secret and constant-time HMAC comparison
        if (!tenant.webhookSecret) {
            httpStatus = 401;
            await WebhookLog.create({ tenantId, sourceIp, httpStatus });
            return res.status(401).json({ error: 'Unauthorized' });
        }
        const plainSecret = decryptSecret(tenant.webhookSecret);
        const providedSecret = req.headers['x-webhook-secret'] || '';
        const secretBuf = Buffer.from(plainSecret);
        const providedBuf = Buffer.from(providedSecret);
        let secretValid = false;
        if (secretBuf.length === providedBuf.length) {
            try { secretValid = crypto.timingSafeEqual(secretBuf, providedBuf); } catch (e) { secretValid = false; }
        }

        // 6.3 — Shopify HMAC verification (Shopify cannot send custom headers)
        const shopifyHmac = req.headers['x-shopify-hmac-sha256'];
        if (shopifyHmac) {
            const rawBody = JSON.stringify(req.body);
            const expectedHmac = crypto.createHmac('sha256', plainSecret).update(rawBody).digest('base64');
            if (shopifyHmac === expectedHmac) secretValid = true;
        }

        if (!secretValid) {
            httpStatus = 401;
            await WebhookLog.create({ tenantId, sourceIp, httpStatus });
            return res.status(401).json({ error: 'Unauthorized' });
        }

        // 6.4 — Parse, normalise, validate, sanitise
        const rawPayload = req.body;
        const shopifyTopic = req.headers['x-shopify-topic'];
        const source = rawPayload.source
            || (shopifyTopic ? 'shopify' : null)
            || (rawPayload.billing_address || rawPayload.default_address ? 'shopify' : 'wordpress');
        const parsed = parseLeadPayload(rawPayload, source);
        const normalisedMobile = normaliseMobileNumber(parsed.mobileNumber);

        if (!normalisedMobile) {
            httpStatus = 400;
            await WebhookLog.create({ tenantId, sourceIp, httpStatus });
            return res.status(400).json({ error: 'mobileNumber is required' });
        }
        if (!validateMobileNumber(normalisedMobile)) {
            httpStatus = 422;
            await WebhookLog.create({ tenantId, sourceIp, httpStatus });
            return res.status(422).json({ error: 'Invalid mobile number format' });
        }

        const sanitised = sanitiseLeadFields({
            name: parsed.name,
            email: parsed.email,
            companyName: parsed.companyName,
            formName: parsed.formName,
        });

        // 6.5 — Deduplication check against Clients collection
        const existingClient = await Client.findOne({ tenantId, mobileNumber: normalisedMobile });
        const isDuplicate = !!existingClient;

        // 6.6 — Persist Lead_Record and upsert/create Client document
        let clientId;
        if (isDuplicate) {
            clientId = existingClient._id;
        } else {
            const newClient = await Client.create({
                tenantId,
                name: sanitised.name || '',
                mobileNumber: normalisedMobile,
                emailId: sanitised.email || '',
                companyName: sanitised.companyName || '',
            });
            clientId = newClient._id;
        }

        const lead = await Lead.create({
            tenantId,
            name: sanitised.name || '',
            mobileNumber: normalisedMobile,
            email: sanitised.email || '',
            companyName: sanitised.companyName || '',
            source: parsed.source,
            formName: sanitised.formName || '',
            metadata: rawPayload,
            isDuplicate,
            clientId,
        });

        // 6.7 — Return HTTP 200 immediately before async processing
        res.status(200).json({ status: 'received', leadId: lead._id.toString() });

        // 6.8 — Log webhook request to WebhookLogs (async, after response)
        WebhookLog.create({ tenantId, sourceIp, httpStatus: 200 }).catch(console.error);

        // 7.7 — Evaluate triggers asynchronously after returning HTTP 200
        evaluateTriggers(tenantId, lead).catch(console.error);

    } catch (err) {
        console.error('Webhook error:', err);
        if (!res.headersSent) {
            httpStatus = 500;
            WebhookLog.create({ tenantId, sourceIp, httpStatus }).catch(console.error);
            res.status(500).json({ error: 'Internal server error' });
        }
    }
});

// ── Webhook Secret Routes ─────────────────────────────────────────────────────

// POST /api/leads/webhook-secret/generate (authenticated)
app.post('/api/leads/webhook-secret/generate', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const plainSecret = require('crypto').randomBytes(32).toString('hex');
        const encrypted = encryptSecret(plainSecret);
        await Tenant.findByIdAndUpdate(tenantId, { webhookSecret: encrypted });
        res.json({ secret: plainSecret });
    } catch (err) {
        console.error('Error generating webhook secret:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// GET /api/leads/webhook-secret/reveal (authenticated) — returns plain secret for copying
app.get('/api/leads/webhook-secret/reveal', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const tenant = await Tenant.findById(tenantId);
        if (!tenant || !tenant.webhookSecret) {
            return res.status(404).json({ error: 'No secret configured' });
        }
        const plainSecret = decryptSecret(tenant.webhookSecret);
        res.json({ secret: plainSecret });
    } catch (err) {
        console.error('Error revealing webhook secret:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// GET /api/leads/webhook-secret (authenticated)
app.get('/api/leads/webhook-secret', authenticate, async (req, res) => {
    try {
        const tenantId = req.user.tenantId;
        const tenant = await Tenant.findById(tenantId);
        if (!tenant || !tenant.webhookSecret) {
            return res.json({ maskedSecret: null });
        }
        const plainSecret = decryptSecret(tenant.webhookSecret);
        res.json({ maskedSecret: maskSecret(plainSecret) });
    } catch (err) {
        console.error('Error fetching webhook secret:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// ── OpenAI Key Routes ─────────────────────────────────────────────────────────

// GET /api/tenant/openai-key-status (authenticated)
app.get('/api/tenant/openai-key-status', authenticate, async (req, res) => {
    try {
        const tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
        const key = tenant.openaiApiKey;
        if (!key || key.trim() === '') {
            return res.json({ configured: false, maskedKey: null });
        }
        const maskedKey = '*'.repeat(Math.max(0, key.length - 4)) + key.slice(-4);
        res.json({ configured: true, maskedKey });
    } catch (err) {
        console.error('Error fetching OpenAI key status:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// PUT /api/tenant/openai-key (authenticated)
app.put('/api/tenant/openai-key', authenticate, async (req, res) => {
    const { apiKey } = req.body;
    if (!apiKey || typeof apiKey !== 'string' || apiKey.trim() === '') {
        return res.status(400).json({ error: 'apiKey must be a non-empty string' });
    }
    try {
        await Tenant.findByIdAndUpdate(req.user.tenantId, { openaiApiKey: apiKey });
        res.json({ message: 'Saved' });
    } catch (err) {
        console.error('Error saving OpenAI key:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// ── AI Template Generation ────────────────────────────────────────────────────

// POST /api/ai/generate-template (authenticated)
app.post('/api/ai/generate-template', authenticate, async (req, res) => {
    const { prompt } = req.body;

    // Validate prompt
    if (!prompt || typeof prompt !== 'string' || prompt.trim() === '') {
        return res.status(400).json({ error: 'prompt must be a non-empty string' });
    }
    if (prompt.length > 500) {
        return res.status(400).json({ error: 'prompt must not exceed 500 characters' });
    }

    // Look up tenant and check OpenAI key
    let tenant;
    try {
        tenant = await Tenant.findById(req.user.tenantId);
        if (!tenant) return res.status(404).json({ error: 'Tenant not found' });
        if (!tenant.openaiApiKey || tenant.openaiApiKey.trim() === '') {
            return res.status(422).json({ error: 'OpenAI API key is not configured. Please add your key in Integration Settings.' });
        }
    } catch (err) {
        console.error('Error looking up tenant for generate-template:', err);
        return res.status(500).json({ error: 'Internal server error' });
    }

    // Call OpenAI Chat Completions API
    const systemPrompt = `You are a WhatsApp Business template content generator.
Your task is to produce template content based on the user's description.
Follow WhatsApp template content policies strictly:
- No URLs, phone numbers, or email addresses in the body unless they are template variables.
- No promotional language in UTILITY templates.
- Keep the message professional and clear.
- The header must be plain text only (no media), 60 characters or fewer.
- The footer must be 60 characters or fewer.

Respond with ONLY a valid JSON object in this exact format (no markdown, no extra text):
{
  "body": "<required: the main message body>",
  "header": "<optional: short plain-text header, max 60 chars — omit key if not needed>",
  "footer": "<optional: short footer text, max 60 chars — omit key if not needed>"
}`;

    try {
        const openaiResponse = await axios.post(
            'https://api.openai.com/v1/chat/completions',
            {
                model: 'gpt-4o-mini',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: prompt },
                ],
                temperature: 0.7,
            },
            {
                headers: {
                    'Authorization': `Bearer ${tenant.openaiApiKey}`,
                    'Content-Type': 'application/json',
                },
            }
        );

        const rawContent = openaiResponse.data.choices[0].message.content.trim();
        let parsed;
        try {
            parsed = JSON.parse(rawContent);
        } catch (parseErr) {
            console.error('Failed to parse OpenAI response as JSON:', rawContent);
            return res.status(502).json({ error: 'OpenAI returned an unexpected response format.' });
        }

        if (!parsed.body || typeof parsed.body !== 'string' || parsed.body.trim() === '') {
            return res.status(502).json({ error: 'OpenAI response is missing the required body field.' });
        }

        const result = { body: parsed.body };
        if (parsed.header && typeof parsed.header === 'string' && parsed.header.trim() !== '') {
            result.header = parsed.header;
        }
        if (parsed.footer && typeof parsed.footer === 'string' && parsed.footer.trim() !== '') {
            result.footer = parsed.footer;
        }

        return res.status(200).json(result);
    } catch (err) {
        const status = err.response?.status;
        const message = err.response?.data?.error?.message || err.message || 'Unknown OpenAI error';
        console.error(`OpenAI API error (HTTP ${status}):`, message);
        return res.status(502).json({ error: `OpenAI API error: ${message}` });
    }
});

// ── Admin: Retry System Monitoring Endpoints ──────────────────────────────────

/**
 * GET /api/admin/retry-system/health
 *
 * Returns health metrics for the retry system:
 *   - pendingRetries: count of ScheduledRetryPhase documents with status 'pending'
 *   - executingRetries: count of ScheduledRetryPhase documents with status 'executing'
 *   - averageDelaySeconds: average (executedAt - scheduledAt) for completed phases
 *   - failedExecutions: count of phases with status 'failed' updated in the last hour
 *   - lastExecutionTime: most recent executedAt from completed phases
 *
 * Requires tenant authentication (same JWT middleware used throughout the app).
 *
 * Requirements: 10.3, 10.4
 */
app.get('/api/admin/retry-system/health', authenticate, async (req, res) => {
    try {
        const now = new Date();
        const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
        const tenantId = req.user.tenantId;

        // Run all queries in parallel for efficiency
        const [
            pendingRetries,
            executingRetries,
            failedExecutions,
            delayAgg,
            lastExecAgg
        ] = await Promise.all([
            // Count pending phases
            ScheduledRetryPhase.countDocuments({ tenantId, status: 'pending' }),

            // Count executing phases
            ScheduledRetryPhase.countDocuments({ tenantId, status: 'executing' }),

            // Count failed phases in the last hour
            ScheduledRetryPhase.countDocuments({
                tenantId,
                status: 'failed',
                updatedAt: { $gte: oneHourAgo }
            }),

            // Average execution delay (executedAt - scheduledAt) for completed phases
            ScheduledRetryPhase.aggregate([
                { $match: { tenantId, status: 'completed', executedAt: { $exists: true } } },
                {
                    $project: {
                        delayMs: { $subtract: ['$executedAt', '$scheduledAt'] }
                    }
                },
                {
                    $group: {
                        _id: null,
                        avgDelayMs: { $avg: '$delayMs' }
                    }
                }
            ]),

            // Most recent executedAt from completed phases
            ScheduledRetryPhase.findOne(
                { tenantId, status: 'completed', executedAt: { $exists: true } },
                { executedAt: 1 },
                { sort: { executedAt: -1 } }
            )
        ]);

        const averageDelaySeconds = delayAgg.length > 0 && delayAgg[0].avgDelayMs != null
            ? parseFloat((delayAgg[0].avgDelayMs / 1000).toFixed(2))
            : null;

        const lastExecutionTime = lastExecAgg ? lastExecAgg.executedAt : null;

        console.log(
            `[admin] GET /api/admin/retry-system/health — pending=${pendingRetries}, ` +
            `executing=${executingRetries}, failed1h=${failedExecutions}, ` +
            `avgDelay=${averageDelaySeconds}s, lastExec=${lastExecutionTime}`
        );

        res.json({
            pendingRetries,
            executingRetries,
            averageDelaySeconds,
            failedExecutions,
            lastExecutionTime
        });
    } catch (err) {
        console.error('[admin] GET /api/admin/retry-system/health error:', err.message, err.stack);
        res.status(500).json({ error: 'Failed to fetch retry system health' });
    }
});

/**
 * GET /api/admin/retry-system/logs
 *
 * Returns paginated execution log entries sourced from the ScheduledRetryPhase
 * collection. Each document represents a scheduled/executed retry phase and
 * serves as a structured log entry.
 *
 * Query params:
 *   - campaignId (optional): filter logs to a specific campaign
 *   - page (optional, default 1): page number (1-indexed)
 *   - limit (optional, default 20): items per page (max 100)
 *
 * Requires tenant authentication.
 *
 * Requirements: 10.1, 10.2
 */
app.get('/api/admin/retry-system/logs', authenticate, async (req, res) => {
    try {
        const { campaignId } = req.query;

        // Parse pagination params
        const page = req.query.page ? parseInt(req.query.page, 10) : 1;
        const limit = Math.min(req.query.limit ? parseInt(req.query.limit, 10) : 20, 100);

        if (isNaN(page) || page < 1) {
            return res.status(400).json({ error: 'page must be a positive integer' });
        }
        if (isNaN(limit) || limit < 1) {
            return res.status(400).json({ error: 'limit must be a positive integer' });
        }

        // Build filter — always scope to the authenticated tenant
        // Exclude phaseNumber=1 — that's the internal grace-period evaluation, not a user-visible retry phase
        const filter = { tenantId: req.user.tenantId, phaseNumber: { $gt: 1 } };
        if (campaignId && typeof campaignId === 'string' && campaignId.trim()) {
            filter.campaignId = campaignId.trim();
        }

        const skip = (page - 1) * limit;

        const [phases, total] = await Promise.all([
            ScheduledRetryPhase.find(filter)
                .sort({ createdAt: -1 })
                .skip(skip)
                .limit(limit)
                .lean(),
            ScheduledRetryPhase.countDocuments(filter)
        ]);

        // Map ScheduledRetryPhase documents to log entry shape
        const logs = phases.map(phase => {
            // Derive a human-readable event name from the phase status
            let event;
            switch (phase.status) {
                case 'completed': event = 'phase_executed'; break;
                case 'executing': event = 'phase_executing'; break;
                case 'failed': event = 'phase_failed'; break;
                case 'cancelled': event = 'phase_cancelled'; break;
                default: event = 'phase_scheduled'; break;
            }

            return {
                campaignId: phase.campaignId,
                phaseNumber: phase.phaseNumber,
                event,
                timestamp: phase.executedAt || phase.scheduledAt || phase.createdAt,
                details: {
                    status: phase.status,
                    scheduledAt: phase.scheduledAt,
                    executedAt: phase.executedAt || null,
                    errorMessage: phase.errorMessage || null,
                    tenantId: phase.tenantId
                }
            };
        });

        console.log(
            `[admin] GET /api/admin/retry-system/logs — page=${page}, limit=${limit}, ` +
            `campaignId=${campaignId || 'all'}, total=${total}`
        );

        res.json({
            logs,
            pagination: {
                page,
                limit,
                total,
                totalPages: Math.ceil(total / limit)
            }
        });
    } catch (err) {
        console.error('[admin] GET /api/admin/retry-system/logs error:', err.message, err.stack);
        res.status(500).json({ error: 'Failed to fetch retry system logs' });
    }
});

// --- Groups API ---

// GET /api/groups — list all groups for the authenticated tenant
app.get('/api/groups', authenticate, async (req, res) => {
    try {
        const groups = await Group.find({ tenantId: req.user.tenantId }).sort({ createdAt: -1 });
        res.json(groups);
    } catch (err) {
        console.error(' GET /api/groups error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// POST /api/groups — create a new group for the authenticated tenant
app.post('/api/groups', authenticate, async (req, res) => {
    const { name, clientIds } = req.body;
    if (!name || name.trim() === '') return res.status(400).json({ error: 'Group name is required' });
    if (!clientIds || clientIds.length === 0) return res.status(400).json({ error: 'At least one client must be selected' });
    try {
        const group = await Group.create({ tenantId: req.user.tenantId, name: name.trim(), clientIds });
        res.status(201).json(group);
    } catch (err) {
        console.error(' POST /api/groups error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// PUT /api/groups/:id — update an existing group for the authenticated tenant
app.put('/api/groups/:id', authenticate, async (req, res) => {
    const { name, clientIds } = req.body;
    if (!name || name.trim() === '') return res.status(400).json({ error: 'Group name is required' });
    if (!clientIds || clientIds.length === 0) return res.status(400).json({ error: 'At least one client must be selected' });
    try {
        const group = await Group.findOneAndUpdate(
            { _id: req.params.id, tenantId: req.user.tenantId },
            { name: name.trim(), clientIds },
            { new: true }
        );
        if (!group) return res.status(404).json({ error: 'Group not found' });
        res.status(200).json(group);
    } catch (err) {
        console.error(' PUT /api/groups/:id error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// DELETE /api/groups/:id — delete a group for the authenticated tenant
app.delete('/api/groups/:id', authenticate, async (req, res) => {
    try {
        const group = await Group.findOneAndDelete({ _id: req.params.id, tenantId: req.user.tenantId });
        if (!group) return res.status(404).json({ error: 'Group not found' });
        res.status(200).json({ message: 'Group deleted' });
    } catch (err) {
        console.error(' DELETE /api/groups/:id error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// Global error handling middleware to capture body-parser errors (like 413 Payload Too Large)
app.use((err, req, res, next) => {
    if (err) {
        if (err.status === 413) {
            return res.status(413).json({ error: 'Payload too large. Request data cannot exceed 50MB.' });
        }
        if (err.status === 400) {
            return res.status(400).json({ error: 'Bad Request. Invalid request data.' });
        }
        console.error('Unhandled request error:', err);
        return res.status(err.status || 500).json({ error: err.message || 'Internal Server Error' });
    }
    next();
});

//  Start 
const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, async () => {
    console.log(` Webhook server is listening on port ${PORT}`);
    console.log(` Next step: Run 'ngrok http ${PORT}' in another terminal`);

    // ── Drop stale global unique index on RetryConfiguration.version ──────────
    // The old schema had { version: 1, unique: true } globally. The new schema
    // uses { tenantId: 1, version: 1, unique: true }. Drop the old index if it
    // still exists to prevent 500 errors when tenants save their first config.
    try {
        const collection = mongoose.connection.collection('retryconfigurations');
        const indexes = await collection.indexes();
        const staleIndex = indexes.find(idx =>
            idx.key && idx.key.version === 1 &&
            !idx.key.tenantId &&
            idx.unique === true
        );
        if (staleIndex) {
            await collection.dropIndex(staleIndex.name);
            console.log(' Dropped stale global RetryConfiguration.version index');
        }
    } catch (err) {
        console.warn(' Could not clean up RetryConfiguration index:', err.message);
    }

    // ── Retry Scheduler: startup recovery + cron job (Tasks 13.1 & 13.2) ──────
    try {
        const { initScheduler } = require('./scheduler');
        await initScheduler(retryScheduler, ScheduledRetryPhase);
        console.log(' Retry scheduler initialised (cron: every minute)');
    } catch (err) {
        console.error(' Failed to initialise retry scheduler:', err.message, err.stack);
    }
});
