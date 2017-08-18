/*
 * This file is part of the bass-nedb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

module.exports = {
    name: "bass-nedb",
    annotations: [],
    client: require('./client'),
    connectionFactory: require('./connection-factory'),
    mapper: require('./mapper'),
    query: require('./query'),
    listeners: [
        {
            constructor: '',
            method: '',
            event: ''
        }
    ]
};
