const mongoose = require('mongoose');

const instagramAutomationSessionSchema = new mongoose.Schema({
    tenantId:    { type: String, required: true },
    igsid:       { type: String, required: true }, // Instagram Scoped ID of the sender
    automationId:{ type: String, required: true },
    lastTriggerAt: { type: Date, default: Date.now },
}, { timestamps: true });

instagramAutomationSessionSchema.index({ tenantId: 1, igsid: 1 }, { unique: true });

module.exports = mongoose.model('InstagramAutomationSession', instagramAutomationSessionSchema);
