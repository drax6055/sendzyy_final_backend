# ConfigurationManager Service

The ConfigurationManager service manages retry configuration CRUD operations with version control and validation for the multi-phase campaign retry system.

## Features

- **Version Control**: Auto-increments version numbers for each configuration
- **Active Configuration Management**: Ensures only one configuration is active at a time
- **Validation**: Validates phase count (0-5), intervals (1-48 hours), and ascending order
- **Configuration History**: Retrieves all configuration versions with active campaign counts
- **Campaign Association**: Tracks which campaigns use which configuration versions

## Usage

### Initialize the Service

```javascript
const mongoose = require('mongoose');
const ConfigurationManager = require('./services/ConfigurationManager');

// Assuming RetryConfiguration model is already defined
const RetryConfiguration = mongoose.model('RetryConfiguration');

const configManager = new ConfigurationManager(RetryConfiguration);
```

### Create a New Configuration

```javascript
const config = {
    phases: [
        { phaseNumber: 1, intervalHours: 2 },
        { phaseNumber: 2, intervalHours: 6 },
        { phaseNumber: 3, intervalHours: 24 }
    ]
};

try {
    const newConfig = await configManager.createConfiguration(config);
    console.log('Created configuration version:', newConfig.version);
    console.log('Is active:', newConfig.isActive);
} catch (error) {
    if (error.validationErrors) {
        console.error('Validation errors:', error.validationErrors);
    } else {
        console.error('Error creating configuration:', error.message);
    }
}
```

### Get Active Configuration

```javascript
const activeConfig = await configManager.getActiveConfiguration();

if (activeConfig) {
    console.log('Active configuration version:', activeConfig.version);
    console.log('Phases:', activeConfig.phases);
} else {
    console.log('No active configuration found');
}
```

### Get Specific Configuration Version

```javascript
const version = 5;
const config = await configManager.getConfigurationVersion(version);

if (config) {
    console.log('Configuration version', version, ':', config);
} else {
    console.log('Configuration version', version, 'not found');
}
```

### Validate Configuration

```javascript
const config = {
    phases: [
        { phaseNumber: 1, intervalHours: 2 },
        { phaseNumber: 2, intervalHours: 6 }
    ]
};

const result = configManager.validateConfiguration(config);

if (result.valid) {
    console.log('Configuration is valid');
} else {
    console.error('Validation errors:', result.errors);
}
```

### Get Configuration History

```javascript
const Campaign = mongoose.model('Campaign');
const history = await configManager.getConfigurationHistory(Campaign);

history.forEach(config => {
    console.log(`Version ${config.version}:`);
    console.log(`  Active: ${config.isActive}`);
    console.log(`  Active Campaigns: ${config.activeCampaigns}`);
    console.log(`  Phases: ${config.phases.length}`);
});
```

### Get Active Campaigns Count for a Version

```javascript
const Campaign = mongoose.model('Campaign');
const version = 5;
const count = await configManager.getActiveCampaignsCount(version, Campaign);

console.log(`Configuration version ${version} is used by ${count} active campaigns`);
```

## Validation Rules

The ConfigurationManager validates configurations according to these rules:

1. **Phase Count**: Must be between 0 and 5 phases
2. **Phase Numbers**: Must be sequential starting from 1 (1, 2, 3, ...)
3. **Interval Range**: Each interval must be between 1 and 48 hours
4. **Interval Type**: Intervals must be integers (whole numbers)
5. **Ascending Order**: Intervals must be in strictly ascending order

## Example Configurations

### Valid Configuration with 3 Phases

```javascript
{
    phases: [
        { phaseNumber: 1, intervalHours: 2 },
        { phaseNumber: 2, intervalHours: 6 },
        { phaseNumber: 3, intervalHours: 24 }
    ]
}
```

### Valid Configuration with 0 Phases (No Retries)

```javascript
{
    phases: []
}
```

### Invalid Configuration Examples

```javascript
// Too many phases (> 5)
{
    phases: [
        { phaseNumber: 1, intervalHours: 2 },
        { phaseNumber: 2, intervalHours: 4 },
        { phaseNumber: 3, intervalHours: 6 },
        { phaseNumber: 4, intervalHours: 12 },
        { phaseNumber: 5, intervalHours: 24 },
        { phaseNumber: 6, intervalHours: 48 }  // Invalid: > 5 phases
    ]
}

// Non-ascending intervals
{
    phases: [
        { phaseNumber: 1, intervalHours: 6 },
        { phaseNumber: 2, intervalHours: 2 }  // Invalid: not ascending
    ]
}

// Non-integer interval
{
    phases: [
        { phaseNumber: 1, intervalHours: 2.5 }  // Invalid: not an integer
    ]
}

// Interval out of range
{
    phases: [
        { phaseNumber: 1, intervalHours: 49 }  // Invalid: > 48 hours
    ]
}
```

## Error Handling

### Validation Errors

When validation fails, the error object includes a `validationErrors` array:

```javascript
try {
    await configManager.createConfiguration(invalidConfig);
} catch (error) {
    if (error.message === 'Configuration validation failed') {
        console.error('Validation failed:');
        error.validationErrors.forEach(err => console.error('  -', err));
    }
}
```

### Database Errors

Database errors are thrown as-is:

```javascript
try {
    await configManager.createConfiguration(config);
} catch (error) {
    console.error('Database error:', error.message);
}
```

## Integration with Campaign Creation

When creating a campaign, capture the active configuration:

```javascript
const configManager = new ConfigurationManager(RetryConfiguration);
const Campaign = mongoose.model('Campaign');

// Get active configuration
const activeConfig = await configManager.getActiveConfiguration();

if (!activeConfig) {
    throw new Error('No active retry configuration found');
}

// Create campaign with configuration snapshot
const campaign = await Campaign.create({
    tenantId: 'tenant123',
    id: 'campaign456',
    retryConfig: {
        version: activeConfig.version,
        phases: activeConfig.phases
    },
    status: 'initial',
    currentPhase: 1
    // ... other campaign fields
});

console.log(`Campaign created with retry config version ${activeConfig.version}`);
```

## Requirements Mapping

- **Requirement 1.4**: Configuration applies to campaigns created after modification timestamp
- **Requirement 8.1**: New configuration only affects new campaigns
- **Requirement 8.2**: Each campaign associated with configuration at creation time

## Notes

- Only one configuration can be active at a time
- Creating a new configuration automatically deactivates the previous one
- Configuration versions are auto-incremented starting from 1
- In production with MongoDB replica set, transactions should be used for atomicity
- Current implementation uses sequential operations for compatibility with standalone MongoDB
