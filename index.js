'use strict';

const path = require('path');
const util = require('util');
const Datastore = require('nedb');

module.exports = exports;
module.exports.__cmd = cmd;

/**
 * @param whaler
 */
async function exports (whaler) {
    if (!whaler.version) {
        throw new Error('unsupported version of `whaler` installed, require `whaler@>=0.7`');
    }
    const { default: storage } = await whaler.fetch('storage');
    storage.adapter = NeDbStorageAdapter;
}

/**
 * @param whaler
 */
async function cmd (whaler) {

    const cli = (await whaler.fetch('cli')).default;

    cli
        .command('nedb:import <name> [import-path]')
        .description('Import from FS to NeDB', {
            name: 'Storage name',
            'import-path': 'Import path',
        })
        .action(async (name, importPath, options) => {
            const { default: Storage } = await whaler.import('./lib/storage');
            const { default: storage } = await whaler.fetch('storage');

            const nedbAdapter = storage.create(name);
            const fsAdapter = new Storage.FileSystemAdapter(name);

            if (importPath) {
                fsAdapter.path = path.resolve(importPath, name);
            }

            const db = await fsAdapter.all();

            if (!Object.keys(db).length) {
                whaler.warn('No data in `%s`', name);
                return;
            }

            for (let key in db) {
                await nedbAdapter.set(key, db[key]);
            }

            whaler.info('Data from `%s` imported to `%s`', fsAdapter.path, name);
        })
    ;

    cli
        .command('nedb:export <name> [export-path]')
        .description('Export from NeDB to FS', {
            name: 'Storage name',
            'export-path': 'Export path',
        })
        .action(async (name, exportPath, options) => {
            const { default: Storage } = await whaler.import('./lib/storage');
            const { default: storage } = await whaler.fetch('storage');

            const nedbAdapter = storage.create(name);
            const fsAdapter = new Storage.FileSystemAdapter(name);

            if (exportPath) {
                fsAdapter.path = path.resolve(exportPath, name);
            }

            const db = await nedbAdapter.all();

            if (!Object.keys(db).length) {
                whaler.warn('No data in `%s`', name);
                return;
            }

            for (let key in db) {
                await fsAdapter.set(key, db[key]);
            }

            whaler.info('Data from `%s` exported to `%s`', name, fsAdapter.path);
        })
    ;

}

/**
 * @param {string} filename
 * @param {Function} callback
 */
function loadNeDB (filename, callback) {
    const db = new Datastore({ filename });
    db.loadDatabase(err => {
        if (err) {
            //callback(err);
            setTimeout(() => loadNeDB(filename, callback), 100);
        } else {
            callback(null, db);
        }
    });
}

class StorageError extends Error {
    /**
     * @param {string} message
     * @param {string} code
     */
    constructor(message, code) {
        super(message);
        this.name = 'StorageError';
        this.code = code;
    }
}

class NeDbStorageAdapter {
    /**
     * @param {string} name
     */
    constructor(name) {
        const filename = path.join('/var/lib/whaler/storage/nedb', name);

        const loadDb = util.promisify(loadNeDB);
        this.db = async (method, ...args) => {
            const db = await loadDb(filename);
            return new Promise((resolve, reject) => {
                db[method](...args, (err, result) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(result);
                    }
                });
            });
        };
    }

    async *[Symbol.asyncIterator]() {
        const db = await this.all();
        for (let key in db) {
            yield [key, db[key]];
        }
    }

    /**
     * @returns {Promise.<Object>}
     */
    async all() {
        const docs = await this.db('find', {});
        const data = {};
        for (let doc of docs) {
            const key = doc['_id'];
            delete doc['_id'];
            data[key] = doc;
        }
        return data;
    }

    /**
     * @param {string} key
     * @returns {Promise.<Object>}
     */
    async get(key) {
        const docs = await this.db('find', { _id: key });
        const data = docs[0] || null;
        if (!data || key !== data['_id']) {
            throw new StorageError(util.format('`%s` not found.', key), 'ERR_NOT_FOUND');
        }
        delete data['_id'];
        return data;
    }

    /**
     * @param {string} key
     * @param {Object} value
     * @returns {Promise.<Object>}
     */
    async set(key, value) {
        await this.db('update', { _id: key }, { _id: key, ...value }, { upsert: true });
        return this.get(key);
    }

    /**
     * @param {string} key
     * @param {Object} value
     * @returns {Promise.<Object>}
     */
    async insert(key, value) {
        try {
            const doc = await this.db('insert', { _id: key, ...value });
            delete doc['_id'];
            return doc;
        } catch (e) {
            throw new StorageError(util.format('`%s` already exists.', key), 'ERR_ALREADY_EXISTS');
        }
    }

    /**
     * @param {string} key
     * @param {Object} value
     * @returns {Promise.<Object>}
     */
    async update(key, value) {
        const numReplaced = await this.db('update', { _id: key }, { $set: value }, {});
        if (0 === numReplaced) {
            throw new StorageError(util.format('`%s` not found.', key), 'ERR_NOT_FOUND');
        }
        return this.get(key);
    }

    /**
     * @param {string} key
     */
    async remove(key) {
        const numRemoved = await this.db('remove', { _id: key }, {});
        if (0 === numRemoved) {
            throw new StorageError(util.format('`%s` not found.', key), 'ERR_NOT_FOUND');
        }
    }
}

module.exports.StorageError = StorageError;
module.exports.NeDbStorageAdapter = NeDbStorageAdapter;