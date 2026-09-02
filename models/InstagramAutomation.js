const mongoose = require('mongoose');

const instagramQuickReplyButtonSchema = new mongoose.Schema({
    title:   { type: String, required: true, maxlength: 20 }, // Instagram limit ~20 chars
    payload: { type: String, required: true },                // e.g. "PRICING", "BOOK_DEMO"
}, { _id: false });

const instagramAutomationSchema = new mongoose.Schema({
    tenantId:       { type: String, required: true },
    name:           { type: String, required: true },          // Internal label e.g. "Welcome Flow"
    triggerKeywords: { type: [String], default: [] },          // empty = fires on ANY first message
    triggerType:    { type: String, enum: ['keyword', 'any_dm'], default: 'keyword' },
    replyMessage:   { type: String, required: true },
    quickReplies:   { type: [instagramQuickReplyButtonSchema], default: [] },
    payloadResponses: { type: Map, of: String, default: {} },
    isActive:       { type: Boolean, default: true },
}, { timestamps: true });

instagramAutomationSchema.index({ tenantId: 1, isActive: 1 });

module.exports = mongoose.model('InstagramAutomation', instagramAutomationSchema);
