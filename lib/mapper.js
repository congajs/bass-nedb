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

const AdapterMapper = require('../../bass').AdapterMapper;

// local modules
const DBRef = require('./dbref');

module.exports = class Mapper extends AdapterMapper {

	/**
	 * Convert relations on a model to data to insert
	 *
	 * @param  {MetaData} metadata
	 * @param  {Object}   model
	 * @param  {Object}   data
	 * @param  {Function} cb
	 * @return {void}
	 */
	convertModelRelationsToData(metadata, model, data, cb) {

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

			if (typeof data[relation.field] !== 'undefined') {
				model[relation.field] = data[relation.field].map(function(el){ return el.id; }); // need to move this somewhere else
			} else {
				model[relation.field] = [];
			}

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
