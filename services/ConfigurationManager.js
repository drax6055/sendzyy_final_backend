const mongoose = require('mongoose');

/**
 * ConfigurationManager Service
 * 
 * Manages retry configuration CRUD operations without version tracking.
 * Uses simple snapshot approach where campaigns capture current config at creation time.
 * 
 * Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6
 */
class ConfigurationManager {
    /**
     * Initialize ConfigurationManager with RetryConfiguration model
     * @param {mongoose.Model} RetryConfiguration - Mongoose model for retry configurations
     */
    constructor(RetryConfiguration) {
        if (!RetryConfiguration) {
            throw new Error('RetryConfiguration model is required');
        }
        this.RetryConfiguration = RetryConfiguration;
    }

    /**
     * Create or update retry configuration for a tenant
     * 
     * This method:
     * 1. Validates the configuration
     * 2. Replaces the existing configuration for the tenant (upsert)
     * 3. Returns the saved configuration
     * 
     * Only one configuration exists per tenant at any time.
     * 
     * @param {Object} config - Configuration object
     * @param {Array} config.phases - Array of phase configurations
     * @param {number} config.phases[].phaseNumber - Phase number (1-5)
     * @param {number} config.phases[].intervalHours - Interval in hours (1-48)
     * @param {string} tenantId - Tenant identifier
     * @returns {Promise<Object>} Created/updated configuration document
     * @throws {Error} If validation fails or database operation fails
     * 
     * Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6
     */
    async createConfiguration(config, tenantId) {
        if (!tenantId) throw new Error('tenantId is required');

        // Validate configuration
        const validationResult = this.validateConfiguration(config);
        if (!validationResult.valid) {
            const error = new Error('Configuration validation failed');
            error.validationErrors = validationResult.errors;
            throw error;
        }

        // Upsert: replace existing config or create new one with incremented version
        const existing = await this.RetryConfiguration.findOne({ tenantId });
        const nextVersion = existing ? (existing.version || 0) + 1 : 1;

        // Use replaceOne to avoid unique-index conflicts when updating version
        await this.RetryConfiguration.replaceOne(
            { tenantId },
            {
                tenantId,
                phases: config.phases,
                version: nextVersion,
                isActive: true,
                createdAt: new Date(),
            },
            { upsert: true }
        );

        // Fetch the saved document to return it
        const newConfig = await this.RetryConfiguration.findOne({ tenantId });
        return newConfig;
    }

    /**
     * Get the current retry configuration for a tenant
     * 
     * Returns the single configuration document for the tenant.
     * 
     * @param {string} tenantId - Tenant identifier
     * @returns {Promise<Object|null>} Configuration or null if none exists
     * 
     * Requirements: 1.6
     */
    async getActiveConfiguration(tenantId) {
        if (!tenantId) throw new Error('tenantId is required');
        return await this.RetryConfiguration.findOne({ tenantId });
    }

    /**
     * Get count of active campaigns using a specific config version
     * 
     * @param {number} version - Config version number
     * @param {mongoose.Model} Campaign - Campaign model
     * @param {string} tenantId - Tenant identifier
     * @returns {Promise<number>} Count of active campaigns
     */
    async getActiveCampaignsCount(version, Campaign, tenantId) {
        if (!Campaign) return 0;
        try {
            return await Campaign.countDocuments({
                tenantId,
                'retryConfig.version': version,
                status: { $in: ['initial', 'retrying'] }
            });
        } catch (_) {
            return 0;
        }
    }

    /**
     * Get configuration history for a tenant
     * 
     * Returns all configuration versions sorted by version descending,
     * with active campaign counts for each.
     * 
     * @param {mongoose.Model} Campaign - Campaign model
     * @param {string} tenantId - Tenant identifier
     * @returns {Promise<Array>} Array of config objects with activeCampaigns count
     */
    async getConfigurationHistory(Campaign, tenantId) {
        if (!tenantId) throw new Error('tenantId is required');

        const configs = await this.RetryConfiguration
            .find({ tenantId })
            .sort({ version: -1 })
            .lean();

        // Attach active campaign counts
        const results = await Promise.all(
            configs.map(async (cfg) => {
                const activeCampaigns = await this.getActiveCampaignsCount(
                    cfg.version, Campaign, tenantId
                );
                return { ...cfg, activeCampaigns };
            })
        );

        return results;
    }

    /**
     * Validate retry configuration
     * 
     * Validates:
     * - Phase count (0-5)
     * - Interval range (1-48 hours)
     * - Interval type (must be integers)
     * - Ascending order of intervals
     * - Sequential phase numbers
     * 
     * @param {Object} config - Configuration to validate
     * @param {Array} config.phases - Array of phase configurations
     * @returns {Object} Validation result with valid flag and errors array
     * 
     * Requirements: 1.1, 1.2, 1.3, 1.4, 1.5
     */
    validateConfiguration(config) {
        const errors = [];

        // Check if config exists and has phases array
        if (!config || !Array.isArray(config.phases)) {
            errors.push('Configuration must have a phases array');
            return { valid: false, errors };
        }

        const phases = config.phases;

        // Requirement 1.1: Phase count must be between 0 and 5
        if (phases.length < 0 || phases.length > 5) {
            errors.push('Phase count must be between 0 and 5');
        }

        // Validate each phase
        for (let i = 0; i < phases.length; i++) {
            const phase = phases[i];

            // Check phase structure
            if (!phase || typeof phase !== 'object') {
                errors.push(`Phase ${i + 1} must be an object`);
                continue;
            }

            // Requirement 1.1: Phase number must be sequential (1, 2, 3, ...)
            if (phase.phaseNumber !== i + 1) {
                errors.push(`Phase numbers must be sequential starting from 1. Expected ${i + 1}, got ${phase.phaseNumber}`);
            }

            // Requirement 1.2: Interval must be between 1 and 48 hours
            if (typeof phase.intervalHours !== 'number') {
                errors.push(`Phase ${i + 1} intervalHours must be a number`);
            } else if (phase.intervalHours < 1 || phase.intervalHours > 48) {
                errors.push(`Phase ${i + 1} interval must be between 1 and 48 hours`);
            }

            // Requirement 1.3: Interval must be an integer
            if (typeof phase.intervalHours === 'number' && !Number.isInteger(phase.intervalHours)) {
                errors.push(`Phase ${i + 1} intervalHours must be an integer`);
            }

            // Requirement 1.4: Intervals must be in ascending order
            if (i > 0 && typeof phase.intervalHours === 'number' && typeof phases[i - 1].intervalHours === 'number') {
                if (phase.intervalHours <= phases[i - 1].intervalHours) {
                    errors.push(`Phase ${i + 1} interval (${phase.intervalHours}h) must be greater than phase ${i} interval (${phases[i - 1].intervalHours}h)`);
                }
            }
        }

        return {
            valid: errors.length === 0,
            errors
        };
    }
}

module.exports = ConfigurationManager;
