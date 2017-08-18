const fs = require('fs-extra');
const path = require('path');

const AdapterIntegration = require('../node_modules/bass/spec/AdapterIntegration');

const dbPath = path.join(__dirname, 'tmp');

if (fs.existsSync(dbPath)) {
    fs.removeSync(dbPath);
}

describe('bass-nedb', AdapterIntegration('bass-nedb', {
    // connections: {
    //     default: {
    //         adapter: 'bass-nedb',
    //         directory: dbPath
    //     }
    // }

}));
