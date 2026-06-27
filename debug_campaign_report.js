/**
 * Debug script to check campaign phase report data
 * 
 * Usage: node debug_campaign_report.js <campaignId>
 */

require('dotenv').config();
const mongoose = require('mongoose');

// Import models
const Campaign = require('./models/Campaign');
const Recipient = require('./models/Recipient');
const ScheduledRetryPhase = require('./models/ScheduledRetryPhase');

async function debugCampaignReport(campaignId) {
    try {
        await mongoose.connect(process.env.MONGODB_URI);
        console.log('Connected to MongoDB\n');

        // Fetch campaign
        const campaign = await Campaign.findOne({ id: campaignId });
        if (!campaign) {
            console.error(`Campaign not found: ${campaignId}`);
            process.exit(1);
        }

        console.log('=== CAMPAIGN DATA ===');
        console.log(`ID: ${campaign.id}`);
        console.log(`Status: ${campaign.status}`);
        console.log(`Current Phase: ${campaign.currentPhase}`);
        console.log(`\nRetry Config:`, JSON.stringify(campaign.retryConfig, null, 2));
        console.log(`\nPhase Stats:`, JSON.stringify(campaign.phaseStats, null, 2));

        // Check scheduled phases
        const scheduledPhases = await ScheduledRetryPhase.find({ campaignId }).sort({ phaseNumber: 1 });
        console.log(`\n=== SCHEDULED PHASES (${scheduledPhases.length}) ===`);
        scheduledPhases.forEach(sp => {
            console.log(`Phase ${sp.phaseNumber}: ${sp.status} - scheduled at ${sp.scheduledAt}`);
        });

        // Check recipient counts by phase
        console.log('\n=== RECIPIENT COUNTS ===');
        const totalRecipients = await Recipient.countDocuments({ campaignId });
        console.log(`Total Recipients: ${totalRecipients}`);

        for (let i = 1; i <= 5; i++) {
            const count = await Recipient.countDocuments({ campaignId, phaseNumber: i });
            if (count > 0) {
                console.log(`Phase ${i} Delivered: ${count}`);
            }
        }

        const nullPhase = await Recipient.countDocuments({ campaignId, phaseNumber: null });
        console.log(`Not Delivered (phaseNumber=null): ${nullPhase}`);

        // Check recipient statuses
        console.log('\n=== RECIPIENT STATUS BREAKDOWN ===');
        const statuses = await Recipient.aggregate([
            { $match: { campaignId } },
            { $group: { _id: '$status', count: { $sum: 1 } } }
        ]);
        statuses.forEach(s => {
            console.log(`${s._id}: ${s.count}`);
        });

        // Check retry history
        console.log('\n=== RETRY HISTORY SAMPLE ===');
        const sampleRecipients = await Recipient.find({ campaignId })
            .limit(5)
            .select('to status phaseNumber retryHistory');
        sampleRecipients.forEach(r => {
            console.log(`${r.to}: status=${r.status}, phaseNumber=${r.phaseNumber}, retryHistory=${JSON.stringify(r.retryHistory)}`);
        });

    } catch (error) {
        console.error('Error:', error);
    } finally {
        await mongoose.disconnect();
    }
}

const campaignId = process.argv[2];
if (!campaignId) {
    console.error('Usage: node debug_campaign_report.js <campaignId>');
    process.exit(1);
}

debugCampaignReport(campaignId);
