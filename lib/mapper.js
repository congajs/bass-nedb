/*
 * This file is part of the bass-nedb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// third-party modules
const _ = require('lodash');
const async = require('async');

// local mdoules
const DBRef = require('./dbref');

module.exports = class Mapper {

	constructor(registry, client) {
		this.registry = registry;
		this.client = client;
	}

	/**
	 * Convert a Javascript value to a db value
	 * 
	 * @param  {mixed} value
	 * @return {mixed}
	 */
	convertJavascriptToDb(type, value) {
		return value;
	}

	/**
	 * Convert a db value to a Javascript value
	 * 
	 * @param  {mixed} value
	 * @return {mixed}
	 */
	convertDbToJavascript(type, value) {
		return value;
	}

	convertRelationsToData(metadata, model, data, cb) {

		// one-to-one
		for (var i in metadata.relations['one-to-one']){
			var relation = metadata.relations['one-to-one'][i];
			var relationMetadata = this.registry.getMetadataByName(relation.document);

			if (model[i] !== null && typeof model[i] !== 'undefined') {
				data[relation.field] = new DBRef(relationMetadata.collection, model[i].id);
			}
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
	}

	convertDataRelationsToDocument(metadata, data, model, mapper, cb) {

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

			if (typeof data[i] !== 'undefined' && data[i] !== null){

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
	}


	convertDataRelationToDocument(metadata, fieldName, data, model, mapper, cb) {

		var field = metadata.getFieldByProperty(fieldName);

		// if (!field || typeof data[field.name] === 'undefined' || data[field.name] === null) {
		// 	cb(null, model);
		// 	return;
		// }

		var relation = metadata.getRelationByFieldName(fieldName);

		if (typeof metadata.relations['one-to-one'][fieldName] !== 'undefined') {

			var dbRef = new DBRef(data[fieldName].ref, data[fieldName].id);
			var start = new Date();

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

	/**
	 * Map raw data to a model using sparse information for any joins
	 * so that they can be grabbed later on in bulk and merged in
	 *
	 * @param  {Object}   model
	 * @param  {Metadata} metadata
	 * @param  {Object}   data
	 * @param  {Function} cb
	 * @return {void}
	 */
	mapPartialRelationsToModel(model, metadata, data, cb) {

		var relations = metadata.getRelations();

		var keys = Object.keys(metadata.relations['one-to-one']);
		for (var i = 0, j = keys.length; i < j; i++) {
			var relation = relations['one-to-one'][keys[i]];
			if (data[relation.field] !== null && typeof data[relation.field] !== 'undefined') {
				model[relation.field] = data[relation.field].id; // need to move this somewhere else
			}
		}

		var keys = Object.keys(metadata.relations['one-to-many']);
		for (var i = 0, j = keys.length; i < j; i++) {
			var relation = relations['one-to-many'][keys[i]];
			model[relation.field] = data[relation.field].map(function(el){ return el.id; }); // need to move this somewhere else
		}

		cb(null, model);
	}

	/**
	 * Run queries on a collection of partial models and merge the related
	 * models in to each model
	 * 
	 * @param  {Manager}  manager
	 * @param  {Metadata} metadata
	 * @param  {Object}   data
	 * @param  {Function} cb
	 * @return {void}
	 */
	mergeInRelations(manager, metadata, data, cb) {

		if (metadata.relations['one-to-one'].length === 0 && metadata.relations['one-to-many'] === 0) {
			cb(null, data);
			return;
		}

		var calls = [];
		var self = this;

		this.addOneToOneCalls(manager, metadata, data, calls);
		this.addOneToManyCalls(manager, metadata, data, calls);

		async.parallel(calls, function(err) {

			if (err) {
				cb(err);
			} else {
				cb(null, data);
			}

		});
	}

	addOneToOneCalls(manager, metadata, data, calls) {

		// var start = new Date();

		var self = this;

		var keys = Object.keys(metadata.relations['one-to-one']);
		for (var i = 0, j = keys.length; i < j; i++) {

			var relation = metadata.relations['one-to-one'][keys[i]];
			var relationMetadata = self.registry.getMetadataByName(relation.document);
			var idFieldName = relationMetadata.getIdFieldName();

			(function(data, relation, relationMetadata) {

				calls.push(function(cb){

					var ids = [];

					data.forEach(function(obj) {
						ids.push(obj[relation.field]);
					});

					ids = _.uniq(ids);

					if (ids.length > 0) {

						self.client.findWhereIn(relationMetadata, idFieldName, ids, null, null, function(err, relatedData) {

							if (err) {

								cb(err);

							} else {

								//var s = new Date();

								manager.mapDataToModels(relationMetadata, relatedData, function(err, documents) {

									// var e = new Date();
									// var t = e - s;

									// console.log('map data to models inside merge relations: ' + relationMetadata.name + ' - ' + t);

									if (err) {

										cb(err);

									} else {

										var docMap = {};
										var idPropertyName = relationMetadata.getIdPropertyName();
										var relationField = relation.field;

										documents.forEach(function(doc) {
											docMap[doc[idPropertyName]] = doc;
										});

										data.forEach(function(obj) {
											obj[relationField] = docMap[obj[relationField]];
										});

										cb(null);
									}
								});
							}
						});

					} else {
						cb(null);
					}
				});

			})(data, relation, relationMetadata);
		}

		// var end = new Date();
		// var time = end - start;

		// console.log('add one-to-one calls: ' + metadata.name + ' - ' + time);
	}

	addOneToManyCalls(manager, metadata, data, calls) {

		// var start = new Date();

		var self = this;

		var keys = Object.keys(metadata.relations['one-to-many']);
		for (var i = 0, j = keys.length; i < j; i++) {

			var relation = metadata.relations['one-to-many'][keys[i]];

			(function(data, relation) {

				calls.push(function(cb){

					var relationMetadata = self.registry.getMetadataByName(relation.document);
					var ids = [];

					data.forEach(function(obj) {
						obj[relation.field].forEach(function(rel) {
							ids.push(rel);
						});
					});

					ids = _.uniq(ids);

					if (ids.length > 0) {

						self.client.findWhereIn(relationMetadata, relationMetadata.getIdFieldName(), ids, null, null, function(err, relatedData) {

							if (err) {

								cb(err);

							} else {

								manager.mapDataToModels(relationMetadata, relatedData, function(err, documents) {

									if (err) {

										cb(err);

									} else {

										var docMap = {};

										documents.forEach(function(doc) {
											docMap[doc[relationMetadata.getIdPropertyName()]] = doc;
										});

										data.forEach(function(obj) {
											var tmp = [];
											obj[relation.field].forEach(function(id) {
												tmp.push(docMap[id]);
											});
											obj[relation.field] = tmp;
										});

										docMap = null;

										cb(null);
									}
								});
							}
						});
					} else {
						cb(null);
					}
				});
			})(data, relation);
		}

		// var end = new Date();
		// var time = end - start;

		// console.log('add one-to-many calls: ' + metadata.name + ' - ' + time);
	}

}
