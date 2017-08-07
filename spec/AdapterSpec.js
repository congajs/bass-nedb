const path = require('path');

const { Bass } = require('bass');

const logger = require('log4js').getLogger();

process.on('unhandledRejection', (reason, p) => {
    console.error(p, reason);
    throw reason;
});

describe('Adapter', () => {

    const testData = [
        {email: 'test+1@foo.com', name: 'Test 1'},
        {email: 'test+2@foo.com', name: 'Test 2'},
        {email: 'test+3@foo.com', name: 'Test 3'},
        {email: 'test+4@foo.com', name: 'Test 4'}
    ];

    let bass;
    let session;
    let manager;

    beforeEach(done => {

        bass = new Bass({

            'adapters': [
                path.join(__dirname, '..', '..', 'bass-nedb')
            ],

            'logging': { logger },

            'connections': {
                'default': {
                    'adapter': 'bass-nedb'
                }
            },

            'managers': {
                'default': {
                    'adapter': 'bass-nedb',
                    'connection': 'default',
                    'documents': [
                        path.join(__dirname, '..', 'node_modules/bass/spec/data/model')
                    ]
                }
            }
        });

        bass.init().then(() => {

            // NOTE: needed until we can respond to on-connect
            setTimeout(() => {

                session = bass.createSession();
                manager = session.getManager('default');

                done();

            }, 1500);

        });

    });

    describe('create, read, update, delete', () => {

        beforeEach(done => {

            // insert the documents
            for (let data of testData) {
                const document = manager.createDocument('User', data);
                manager.persist(document);
            }

            manager.flush().then(() => {

                manager.findCountBy('User', {}).then(num => {

                    expect(num).toEqual(testData.length);

                    done();

                });

            });

        });

        afterAll(done => {

            // clean the collection
            manager.removeBy('User', {}).then(() => done());

        });

        it('should CRUD', done => {

            let promises = [];

            // validate the records one by one
            for (let [idx, data] of testData.entries()) {
                promises.push(
                    // find the document by criteria
                    manager.findOneBy('User', {
                        email: data.email,
                        name: data.name
                    }).then(document => {
                        expect(document).toEqual(jasmine.objectContaining({
                            email: data.email,
                            name: data.name
                        }));
                        // update the document
                        document.email = 'update+' + (idx + 1) + '@foo.com';
                        document.name = 'Update ' + (idx + 1);
                        manager.persist(document);
                        return manager.flush(document).then(() => {

                            // find the document by id
                            return manager.find('User', document.id).then(found => {
                                expect(found).toEqual(jasmine.objectContaining({
                                    email: document.email,
                                    name: document.name
                                }));

                                // remove the document
                                manager.remove(document);
                                return manager.flush(document);
                            })
                        })
                    })
                );
            }

            Promise.all(promises).then(data => {

                Promise.all([
                    manager.findBy('User', {}).then(documents => {
                        expect(documents.length).toEqual(0);
                    }),
                    manager.findCountBy('User', {}).then(num => {
                        expect(num).toEqual(0);
                    })
                ]).then(() => {

                    done();

                });

            });
        });

        it('should support remove by', done => {

            const data = testData[0];
            manager.removeBy('User', {email: data.email, name: data.name}).then(() => {

                Promise.all([
                    manager.findOneBy('User', {email: data.email, name: data.email}).then(document => {

                        expect(document).toBeFalsy();

                    }),
                    manager.findCountBy('User', {}).then(num => {

                        expect(num).toEqual(testData.length - 1);

                    })
                ]).then(data => {

                    manager.removeBy('User', {}).then(() => {

                        manager.findCountBy('User', {}).then(num => {

                            expect(num).toEqual(0);

                            done();

                        });

                    });

                });

            })

        });

        it('should support update by', done => {

            const data = testData[2];
            manager.updateBy(
                'User',
                {email: data.email, name: data.name},
                {email: 'update-by-user@foo.com', name: 'Update By User'}
            ).then(() => {

                Promise.all([
                    manager.findOneBy('User', {email: data.email, name: data.name}).then(document => {
                        expect(document).toBeFalsy();
                    }),

                    manager.findOneBy(
                        'User',
                        {email: 'update-by-user@foo.com', name: 'Update By User'}
                    ).then(document => {
                        expect(document).toEqual(jasmine.objectContaining({
                            email: 'update-by-user@foo.com',
                            name: 'Update By User'
                        }));
                    })
                ]).then(data => {

                    done();

                });

            })

        });

    });

});