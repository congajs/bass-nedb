const path = require('path');

const AdapterIntegrationSpec = require('../node_modules/bass/spec/AdapterIntegrationSpec');

describe('bass-nedb', AdapterIntegrationSpec('bass-nedb', {
    adapters: [
        path.resolve('')
    ]
}));
