/*
 * This file is part of the bass-nedb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var Connection = require('./connection');

module.exports = {

	factory: function(config, logger, cb){
		var connection = new Connection(config.directory, logger);
		cb(null, connection);
	}
};