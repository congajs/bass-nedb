/*
 * This file is part of the bass-nedb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
const QueryResult = require('bass').QueryResult;

module.exports = class Client {

    constructor(db, logger) {
        this.db = db;
        this.logger = logger;
    }

    /**
     * Insert a new document
     *
     * @param  {Metadata}   metadata
     * @param  {String}     collection
     * @param  {Object}     data
     * @param  {Function}   cb
     * @return {void}
     */
    insert(metadata, collection, data, cb) {

        const start = new Date();

        this.db.collection(collection, (err, coll) => {
            coll.insert(data, (err, docs) => {

                const idFieldName = metadata.getIdFieldName();
                data[idFieldName] = docs[idFieldName];

                this.logger.debug(
                    '[bass-nedb] - insert [' + collection + ']: ' +
                        JSON.stringify(data) + ' : ' + ((new Date()) - start) + 'ms');

                cb(err, data);
            });
        });
    }

    /**
     * Update a document
     *
     * @param  {Metadata}   metadata
     * @param  {String}     collection
     * @param  {ObjectID}   id
     * @param  {Object}     data
     * @param  {Function}   cb
     * @return {void}
     */
    update(metadata, collection, id, data, cb) {

        const start = new Date();

        this.db.collection(collection, (err, coll) => {

            const idField = metadata.getIdFieldName();

            const cond = {[idField]: id};

            if (typeof metadata.versionProperty !== 'undefined') {
                cond['version'] = data['version']-1;
            }

            // need to remove the id from the update data
            if (idField in data) {
                delete data[idField];
            }

            coll.update(cond, {'$set' : data }, (err, docs) => {

                this.logger.debug(
                    '[bass-nedb] - update [' + collection + ']: ' +
                        JSON.stringify(data) + ' : ' + ((new Date()) - start) + 'ms');

                cb(err, docs);
            });
        });
    }

    /**
     * Update multiple records matching a criteria
     *
     * @param  {Metadata}   metadata
     * @param  {String}     collection
     * @param  {Object}   	criteria
     * @param  {Object}     data
     * @param  {Function}   cb
     * @return {void}
     */
    updateBy(metadata, collection, criteria, data, cb) {

        const start = new Date();

        this.db.collection(collection, (err, coll) => {

            // need to remove the id from the update data
            const idField = metadata.getIdFieldName();
            if (idField in data) {
                delete metadata[idField];
            }

            coll.update(criteria, {'$set' : data }, (err, docs) => {

                this.logger.debug(
                    '[bass-nedb] - updateBy [' + collection + ']: ' + JSON.stringify(criteria)
                        + ' : ' + JSON.stringify(data) + ' : ' + ((new Date()) - start) + 'ms');

                cb(err, docs);
            });
        });
    }

    /**
     * Remove a document by id
     *
     * @param  {Metadata}   metadata
     * @param  {String}     collection
     * @param  {ObjectID}   id
     * @param  {Object}     data
     * @param  {Function}   cb
     * @return {void}
     */
    remove(metadata, collection, id, cb) {

        const start = new Date();

        this.db.collection(collection, (err, coll) => {

            const cond = {};
            cond[metadata.getIdFieldName()] = id;

            coll.remove(cond, 1, err => {

                this.logger.debug(
                    '[bass-nedb] - remove [' + collection + ']: ' + id + ' : ' +
                        ((new Date()) - start) + 'ms');

                cb(err);
            });
        });
    }

    /**
     * Remove multiple records matching a criteria
     *
     * @param  {Metadata}   metadata
     * @param  {String}     collection
     * @param  {Object}   	criteria
     * @param  {Function}   cb
     * @return {void}
     */
    removeBy(metadata, collection, criteria, cb) {

        const start = new Date();

        this.db.collection(collection, (err, coll) => {
            coll.remove(criteria, { multi: true }, (err, numRemoved) => {

                this.logger.debug(
                    '[bass-nedb] - removeBy [' + collection + ']: ' +
                        JSON.stringify(criteria) + ' : ' + ((new Date()) - start) + 'ms');

                cb(err, numRemoved);
            });
        });
    }

    /**
     * Find a document by id
     *
     * @param  {Metadata}   metadata
     * @param  {String}     collection
     * @param  {ObjectID}   id
     * @param  {Object}     data
     * @param  {Function}   cb
     * @return {void}
     */
    find(metadata, collection, id, cb) {

        const start = new Date();

        const idFieldName = metadata.getIdFieldName();

        if (!idFieldName || idFieldName.length === 0) {
            cb(new Error('Could not find the Bass ID Field for ' + collection));
            return;
        }

        this.db.collection(collection, (err, coll) => {

            coll.findOne({[idFieldName]: id}, (err, item) => {

                this.logger.debug(
                    '[bass-nedb] - find [' + collection + ']: {"' + idFieldName +'":"' + id + '"} : ' +
                        ((new Date()) - start) + 'ms');

                cb(err, item);
            });
        });
    }

    /**
     * Find documents based on a Query
     *
     * @param  {Metadata} metadata
     * @param  {String}   collection
     * @param  {Query}    query
     * @param  {Function} cb
     * @return {void}
     */
    findByQuery(metadata, collection, query, cb) {

        const criteria = this.convertQueryToCriteria(query);

        const start = new Date();

        this.db.collection(collection, (err, coll) => {

            const cursor = coll.find(criteria);

            // add sorting
            if (query.getSort() !== null && query.getSort() !== {}) {
                cursor.sort(query.getSort());
            }

            // add skip
            if (typeof query.getSkip() !== 'undefined' && query.getSkip() !== null) {
                cursor.skip(query.getSkip());
            }

            // add limit
            if (typeof query.getLimit() !== 'undefined' && query.getLimit() !== null) {
                cursor.limit(query.getLimit());
            }

            cursor.exec((err, docs) => {

                // add documents to the query result
                queryResult.setData(docs);

                this.logger.debug(
                    '[bass-nedb] - findByQuery [' + collection + ']: ' +
                        JSON.stringify(query) + ' : ' + ((new Date()) - start) + 'ms');

                // if we are told to, fetch the total count
                if (query.getCountFoundRows()) {

                    that.findCountByQuery(metadata, collection, query, function(err, count) {

                        if (err) {
                            cb(err, null);
                        } else {
                            queryResult.totalRows = count;
                            cb(null, queryResult);
                        }
                    });

                } else {
                    cb(null, queryResult);
                }

            });
        });
    }

    /**
     * Get a document count based on a Query
     *
     * @param  {Metadata} metadata
     * @param  {String}   collection
     * @param  {Query}    query
     * @param  {Function} cb
     * @return {void}
     */
    findCountByQuery(metadata, collection, query, cb) {

        const start = new Date();

        const mongoCriteria = this.convertQueryToCriteria(query);

        this.db.collection(collection, (err, coll) => {
            const cursor = coll.count(mongoCriteria, (err, count) => {

                this.logger.debug(
                    '[bass-nedb] - findCountByQuery [' + collection + ']: ' +
                        JSON.stringify(query) + ' : ' + ((new Date()) - start) + 'ms');

                cb(err, count);
            });
        });
    }

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
    findBy(metadata, collection, criteria, sort, skip, limit, cb) {

        if (typeof criteria === 'undefined') {
            criteria = {};
        }

        const start = new Date();

        this.db.collection(collection, (err, coll) => {

            const cursor = coll.find(criteria);

            // add sorting
            if (typeof sort !== 'undefined' && sort !== null) {
                cursor.sort(sort);
            }

            // add skip
            if (typeof skip !== 'undefined' && skip !== null) {
                cursor.skip(skip);
            }

            // add limit
            if (typeof limit !== 'undefined' && limit !== null) {
                cursor.limit(limit);
            }

            // run the query
            cursor.exec((err, docs) => {

                this.logger.debug(
                    '[bass-nedb] - findBy [' + collection + ']: ' +
                        JSON.stringify(criteria) + ' : ' + ((new Date()) - start) + 'ms');

                cb(err, docs);
            });
        });
    }

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
    findWhereIn(metadata, field, values, sort, limit, cb) {

        const criteria = {};
        criteria[field] = {'$in' : values};

        this.findBy(
            metadata,
            metadata.collection,
            criteria ,
            sort,
            null,
            limit || undefined,
            cb
        );
    }

    /**
     * Create a collection
     *
     * @param  {Metadata} metadata
     * @param  {String}   collection
     * @param  {Function} cb
     * @return {void}
     */
    create(metadata, collection, cb) {

        const start = new Date();

        // attempt to create the collection
        this.db.collection(collection, (err, coll) => {

            this.logger.debug(
                '[bass-nedb] - create [' + collection + ']: ' +  ((new Date()) - start) + 'ms');

            cb(err);
        });

    }

    /**
     * Drop a collection
     *
     * @param  {String}   collection
     * @param  {Function} cb
     * @return {void}
     */
    drop(metadata, collection, cb) {

        const start = new Date();

        this.db.collection(collection, (err, coll) => {
            coll.remove({}, {multi: true}, (err, numRemoved) => {

                this.logger.debug(
                    '[bass-nedb] - drop [' + collection + ']: ' + ((new Date()) - start) + 'ms');

                cb(err);
            });

        });
    }

    /**
     * Rename a collection
     *
     * @param  {Metadata}  metadata
     * @param  {String}    collection
     * @param  {String}    newName
     * @param  {Function}  cb
     * @return {void}
     */
    rename(metadata, collection, newName, cb) {

        const start = new Date();

        this.db.collection(collection, (err, coll) => {

            this.logger.debug(
                '[bass-nedb] - rename [' + collection + ']: ' + newName + ' : ' +
                    ((new Date()) - start) + 'ms');

            coll.rename(newName, cb);
        });
    }

    /**
     * Get a list of all of the collection names in the current database
     *
     * @param  {Function} cb
     * @return {void}
     */
    listCollections(cb) {
        this.db.collections(cb);
    }

    /**
     * Convert a Bass Query to MongoDB criteria format
     *
     * @param  {Query} query
     * @return {Object}
     */
    convertQueryToCriteria(query) {

        const newQuery = {};
        const conditions = query.getConditions();

        let field, tmp, i;

        for (field in conditions){

            if (typeof conditions[field] === 'object'){

                tmp = {};

                for (i in conditions[field]){
                    tmp['$' + i] = conditions[field][i];
                }

                newQuery[field] = tmp;

            } else {
                newQuery[field] = conditions[field];
            }
        }

        return newQuery;
    }

}
