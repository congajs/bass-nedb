/*
 * This file is part of the bass-nedb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// third-party modules
var async = require('async');

var DBRef = require('./dbref');

var Mapper = function(registry, client){
	this.registry = registry;
	this.client = client;
};

Mapper.prototype = {

	/**
	 * Convert a Javascript value to a db value
	 * 
	 * @param  {mixed} value
	 * @return {mixed}
	 */
	convertJavascriptToDb: function(type, value){

		var converted = value;

		// switch (type){
		// 	case 'objectid':
		// 		converted = new ObjectID(value);
		// 		break;
		// }

		return converted;
	},

	/**
	 * Convert a db value to a Javascript value
	 * 
	 * @param  {mixed} value
	 * @return {mixed}
	 */
	convertDbToJavascript: function(type, value){

		var converted = value;

		// if (value instanceof ObjectID){
		// 	converted = value.toHexString();
		// }

		return converted;
	},

	convertRelationsToData: function(metadata, model, data, cb){

		console.log(model);


		// one-to-one
		for (var i in metadata.relations['one-to-one']){
			var relation = metadata.relations['one-to-one'][i];
			var relationMetadata = this.registry.getMetadataByName(relation.document);
			data[relation.field] = new DBRef(relationMetadata.collection, model[i].id);
		}

		// one-to-many
		for (var i in metadata.relations['one-to-many']){
			var relation = metadata.relations['one-to-many'][i];
			var relationMetadata = this.registry.getMetadataByName(relation.document);

			data[relation.field] = [];

			model[i].forEach(function(oneToManyDoc){
				data[relation.field].push(new DBRef(relationMetadata.collection, oneToManyDoc.id));
			});
		}

		// @EmbedOne
		for (var i in metadata.embeds['one']){
			var relationMetadata = this.registry.getMetadataByName(metadata.embeds['one'][i].targetDocument);
			data[i] = mapper.mapModelToData(relationMetadata, model[i]);
		}

		// @EmbedMany
		for (var i in metadata.embeds['many']){
			var relationMetadata = this.registry.getMetadataByName(metadata.embeds['many'][i].targetDocument);

			if (Array.isArray(model[i])){
				data[i] = [];
				model[i].forEach(function(m){
					data[i].push(mapper.mapModelToData(relationMetadata, m));
				});
			}
		}

		cb();
	},

	convertDataRelationsToDocument: function(metadata, data, model, mapper, cb){

		var self = this;
		var calls = [];
		var registry = this.registry;

		// @EmbedOne
		for (var i in metadata.embeds['one']){

			(function(data, model, i){

				calls.push(
					function(callback){
						var relationMetadata = registry.getMetadataByName(metadata.embeds['one'][i].targetDocument);
						mapper.mapDataToModel(relationMetadata, data[i], function(err, m){
							model[i] = m;
							callback(model);
						});
					}
				);

			}(data, model, i));
		}

		// one-to-one
		for (var i in metadata.relations['one-to-one']){

			if(typeof data[i] !== 'undefined' && data[i] !== null){

				(function(data, model, i){

					calls.push(

						function(callback){
							var dbRef = new DBRef(data[i].ref, data[i].id);

							self.client.db.dereference(dbRef, function(err, item){
								model[i] = item;
								callback(model);
							});
						}
					);

				}(data, model, i));				
			}
		}

		// one-to-many
		for (var i in metadata.relations['one-to-many']){

			if (typeof data[i] !== 'undefined' && data[i] !== null){

				(function(data, model, i){

					calls.push(

						function(callback){

							var ids = [];
							var relation = metadata.getRelationByFieldName(i);
							var relationMetadata = self.registry.getMetadataByName(relation.document);

							for(var j in data[i]){
								ids.push(data[i][j].id);
							}

							self.client.findBy(relationMetadata,
											   relationMetadata.collection, 
											   { "_id" : { "$in" : ids }},
											   null,
											   null,
											   null,
											   function(err, docs){
											   		model[i] = docs;
											   		callback(model);
											   }
							);
						}
					);

				}(data, model, i));				
			}
		}

		if (calls.length > 0){

			// run all queries and process data
			async.parallel(calls, function(document){
				cb(null, document);
			});

		} else {
			cb(null, model);
		}
	},


	convertDataRelationToDocument: function(metadata, fieldName, data, model, mapper, cb) {

		var field = metadata.getFieldByProperty(fieldName);

		// if (!field || typeof data[field.name] === 'undefined' || data[field.name] === null) {
		// 	cb(null, model);
		// 	return;
		// }

		var relation = metadata.getRelationByFieldName(fieldName);

		if (typeof metadata.relations['one-to-one'][fieldName] !== 'undefined') {

			var dbRef = new DBRef(data[fieldName].ref, data[fieldName].id);

			this.client.db.dereference(dbRef, function(err, item){
				model[fieldName] = item;
				cb(err, model);
			});

		} else if (typeof metadata.relations['one-to-many'][fieldName] !== 'undefined') {

			// make sure we have an empty array
			model[fieldName] = [];

			var ids = [];
			for(var j in data[fieldName]){
				ids.push(data[fieldName][j].id);
			}

			// do not continue if we have nothing to map
			if (ids.length === 0) {
				cb(null, model);
				return;
			}

			var relation = metadata.getRelationByFieldName(fieldName);
			var relationMetadata = this.registry.getMetadataByName(relation.document);
			var annotation = metadata.relations['one-to-many'][fieldName];

			var sort = null;
			if (annotation.sort && annotation.direction) {
				sort = {};
				sort[annotation.sort] = annotation.direction.toString().toLowerCase() === 'desc' ? -1 : 1;
			}

			this.client.findWhereIn(relationMetadata, '_id', ids, sort, null, function(err, docs) {

				if (!err) {
					model[fieldName] = docs;
				}
				cb(err, model);
			});

		} else {
			cb(null, model);
		}
	}
};

module.exports = Mapper;