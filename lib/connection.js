/*
 * This file is part of the bass-nedb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

const path = require('path');

const Datastore = require('nedb');

module.exports = class Connection {

	constructor(dir, logger) {
		this.dir = dir;
		this.logger = logger;
		this.collections = {};
	};

	/**
	 * 
	 * @param  {Metadata} metadata
	 * @param  {Function} cb
	 * @return {void}
	 */
	boot(metadataRegistry, cb) {

		const that = this;
		let i, metadata;

		for (i in metadataRegistry.metas) {

			metadata = metadataRegistry.metas[i];

			//metadata.indexes.single.forEach(function(index){

				that.collection(metadata.collection, function(err, collection) {

					//collection.ensureIndex({ fieldName : index.field, unique : index.isUnique, sparse : index.isSparse });

				});

			//});
		}

		cb(null);
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

			if (typeof this.dir !== 'undefined' && this.dir !== null) {
				collectionPath = path.join(this.dir, name + '.db');
			}

			this.logger.debug('[bass-nedb] - loading collection: ' + collectionPath);

			const collection = new Datastore({ filename: collectionPath });

			collection.loadDatabase(function(err) {

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
