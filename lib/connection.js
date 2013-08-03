/*
 * This file is part of the bass-nedb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var path = require('path');

var Datastore = require('nedb');

var Connection = function(dir){
	this.dir = dir;
	this.collections = {};
};

Connection.prototype = {

	/**
	 * 
	 * @param  {Metadata} metadata
	 * @param  {Function} cb
	 * @return {void}
	 */
	boot: function(metadataRegistry, cb){

		var that = this;

		for (var i in metadataRegistry.metas){

			var metadata = metadataRegistry.metas[i];

			metadata.indexes.single.forEach(function(index){

				that.collection(metadata.collection, function(err, collection){

					collection.ensureIndex({ fieldName : index.field, unique : index.isUnique, sparse : index.isSparse });

				});

			});
		}

		cb(null);
	},

	/**
	 * Get a collection by name
	 * 
	 * @param  {String}   name
	 * @param  {Function} cb
	 * @return {void}
	 */
	collection: function(name, cb){

		var collections = this.collections;

		if (typeof collections[name] === 'undefined'){

			var collectionPath = null;

			if (typeof this.dir !== 'undefined' && this.dir !== null){
				collectionPath = path.join(this.dir, name + '.db');
			}

			var collection = new Datastore({ filename: collectionPath });

			collection.loadDatabase(function(err){

				if (err === null){
					collections[name] = collection;
					cb(null, collection);

				} else {
					cb(err);
				}

			});

		} else {

			cb(null, collections[name]);
		}
	},

	dereference: function(dbref, cb){
		this.collection(dbref.ref, function(err, collection){
			collection.findOne({ _id : dbref.id }, function(err, doc){
				cb(err, doc);
			});
		});
	}

};

module.exports = Connection;