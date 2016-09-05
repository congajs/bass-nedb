/*
 * This file is part of the bass-nedb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var Client = function(db, logger){
	this.db = db;
	this.logger = logger;
};

Client.prototype = {

	/**
	 * Insert a new document
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	insert: function(metadata, collection, data, cb){

		this.logger.debug('[bass-nedb] - insert: ' + collection);
		this.logger.debug(data);

		this.db.collection(collection, function(err, coll) {
			coll.insert(data, function(err, docs){
				data[metadata.getIdFieldName()] = docs[metadata.getIdFieldName()];
				cb(err, data);
			});
		}); 
	},

	/**
	 * Update a document
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	update: function(metadata, collection, id, data, cb){

		this.db.collection(collection, function(err, coll) {

			var cond = {};
			cond[metadata.getIdFieldName()] = id;
			
			if (typeof metadata.versionProperty !== 'undefined'){
				cond['version'] = data['version']-1;
			}

			// need to remove the id from the update data
			delete data[metadata.getIdFieldName()];

			coll.update(cond, {'$set' : data }, function(err, docs){
				cb(err, docs);
			});
		}); 
	},

	/**
	 * Remove a document by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	remove: function(metadata, collection, id, cb){

		this.logger.debug('[bass-nedb] - remove: ' + id);

		this.db.collection(collection, function(err, coll){

			var cond = {};
			cond[metadata.getIdFieldName()] = id;

			coll.remove(cond, 1, function(err){
				cb(err);
			});
		});
	},

	/**
	 * Find a document by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	find: function(metadata, collection, id, cb){

		this.logger.debug('[bass-nedb] - find: ' + id + ' | ' + collection);

		this.db.collection(collection, function(err, coll){

			var cond = {};
			cond[metadata.getIdFieldName()] = id;

			coll.findOne(cond, function(err, item){
				cb(err, item);
			});
		});
	},

	/**
	 * Find documents based on a Query
	 * 
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findByQuery: function(metadata, collection, query, cb){

		this.logger.debug('[bass-nedb] - findByQuery');
		this.logger.debug(query);

		var criteria = this.convertQueryToCriteria(query);

		var that = this;
		var start = new Date();

		this.db.collection(collection, function(err, coll){

			var cursor = coll.find(criteria);

			// add sorting
			if (query.getSort() !== null && query.getSort() !== {}){
				cursor.sort(query.getSort());
			}

			// add skip
			if (typeof query.getSkip() !== 'undefined' && query.getSkip() !== null){
				cursor.skip(query.getSkip());
			}

			// add limit
			if (typeof query.getLimit() !== 'undefined' && query.getLimit() !== null){
				cursor.limit(query.getLimit());
			}

			// run the query
			cursor.exec(function(err, docs){

				var time = new Date() - start;
				that.logger.debug('took: ' + time);

				cb(err, docs);
			});
		});
	},

	/**
	 * Get a document count based on a Query
	 * 
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findCountByQuery: function(metadata, collection, query, cb){

		var mongoCriteria = this.convertQueryToCriteria(query);

		this.db.collection(collection, function(err, coll){

			cursor = coll.count(mongoCriteria, function(err, count){
				cb(err, count);
			});
		});
	},

	/**
	 * Find documents by simple criteria
	 * 
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Object}    criteria
	 * @param  {Object}    sort
	 * @param  {Number}    skip
	 * @param  {Number}    limit
	 * @param  {Function}  cb
	 * @return {void}
	 */
	findBy: function(metadata, collection, criteria, sort, skip, limit, cb){



		if (typeof criteria === 'undefined'){
			criteria = {};
		}

		this.logger.debug('[bass-nedb] - findBy');
		this.logger.debug(criteria);
		this.logger.debug('sort: ' + sort + ' | skip: ' + skip + ' limit: ' + limit);

		var that = this;

		var start = new Date();

		this.db.collection(collection, function(err, coll){

			coll.find(criteria, function(err, docs){

				var end = new Date() - start;

				that.logger.debug('took: ' + end);

				if (sort){
					sort(docs, sort);
				}				

				if (skip){
					docs = docs.slice(skip, -1);
				}

				if (limit){
					docs = docs.slice(0, limit);
				}

				cb(err, docs);

			});
		});
	},

	/**
	 * Find documents where a field has a value in an array of values
	 *
	 * @param {Metadata} metadata The metadata for the document type you are fetching
	 * @param {String} field The document's field to search by
	 * @param {Array.<(String|Number)>} values Array of values to search for
	 * @param {Object|null} sort Object hash of field names to sort by, -1 value means DESC, otherwise ASC
	 * @param {Number|null} limit The limit to restrict results
	 * @param {Function} cb Callback function
	 */
	findWhereIn: function(metadata, field, values, sort, limit, cb) {

		var criteria = {};
		criteria[field] = {'$in' : values};

		this.logger.debug('[bass-nedb] - findBy');
		this.logger.debug(criteria);
		this.logger.debug('sort: ' + sort + ' limit: ' + limit);

		this.findBy(
			metadata,
			metadata.collection,
			criteria ,
			sort,
			null,
			limit || undefined,
			cb
		);
	},

	/**
	 * Create a collection
	 * 
	 * @param  {[type]}   metadata   [description]
	 * @param  {[type]}   collection [description]
	 * @param  {Function} cb         [description]
	 * @return {[type]}              [description]
	 */
	create: function(metadata, collection, cb){

		this.db.createCollection(collection, cb);
	},

	/**
	 * Drop a collection
	 * 
	 * @param  {String}   collection
	 * @param  {Function} cb
	 * @return {void}
	 */
	drop: function(metadata, collection, cb){

		this.db.collection(collection, function(err, coll){
			coll.drop(cb);
		});
	},

	/**
	 * Rename a collection
	 * 
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {String}    newName
	 * @param  {Function}  cb
	 * @return {void}
	 */
	rename: function(metadata, collection, newName, cb){
		this.db.collection(collection, function(err, coll){
			coll.rename(newName, cb);
		});
	},

	/**
	 * Get a list of all of the collection names in the current database
	 * 
	 * @param  {Function} cb
	 * @return {void}
	 */
	listCollections: function(cb){
		this.db.collections(cb);
	},

	/**
	 * Convert a Bass Query to MongoDB criteria format
	 * 
	 * @param  {Query} query
	 * @return {Object}
	 */
	convertQueryToCriteria: function(query){

		var newQuery = {};

		var conditions = query.getConditions();

		for (var field in conditions){

			if (typeof conditions[field] === 'object'){

				var tmp = {};

				for (var i in conditions[field]){
					tmp['$' + i] = conditions[field][i];
				}

				newQuery[field] = tmp;

			} else {
				newQuery[field] = conditions[field];
			}
		}

		return newQuery;
	}

};

module.exports = Client;