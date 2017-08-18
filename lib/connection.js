/*
 * This file is part of the bass-nedb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

const path = require('path');

const async = require('async');
const Datastore = require('nedb');

module.exports = class Connection {

    constructor(dir, logger) {
        this.dir = dir;
        this.logger = logger;
        this.collections = {};
    }

    /**
     *
     * @param  {Metadata} metadata
     * @param  {Function} next
     * @return {void}
     */
    boot(metadataRegistry, next) {

        const calls = [];

        const that = this;
        let i, metadata;

        for (i in metadataRegistry.metas) {

            ((metadata) => {

                calls.push((cb) => {

                    that.collection(metadata.collection, (err, collection) => {

                        cb(err, collection);

                    });

                });

            })(metadataRegistry.metas[i])

        }

        async.series(calls, next);
    }

    /**
     * Get a collection by name
     *
     * @param  {String}   name
     * @param  {Function} cb
     * @return {void}
     */
    collection(name, cb) {

        const collections = this.collections;

        if (typeof collections[name] === 'undefined') {

            let collectionPath = null;
            let log = 'memory';

            if (typeof this.dir !== 'undefined' && this.dir !== null) {
                collectionPath = path.join(this.dir, name + '.db');
                log = collectionPath;
            }

            this.logger.debug('[bass-nedb] - loading collection "' + name + '" from: ' + log);

            const collection = new Datastore({ filename: collectionPath });

            collection.loadDatabase((err) => {

                if (err === null) {
                    collections[name] = collection;
                    cb(null, collection);

                } else {
                    cb(err);
                }

            });

        } else {

            cb(null, collections[name]);
        }
    }

    dereference(dbref, cb) {
        this.collection(dbref.ref, function(err, collection) {
            collection.findOne({ _id : dbref.id }, function(err, doc) {
                cb(err, doc);
            });
        });
    }

}
