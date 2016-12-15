import mysql from 'mysql';
import async from 'async';

export default class MySQL {
    /**
     * Constructor
     * @param config
     */
    constructor(config) {
        this._pool = false;
        this._className = 'MySQL';
        if (typeof config !== 'object') {
            throw new Error('A configuration object must be provided.');
        }

        /**
         * Handle MySQL connection disconnect.
         * @private
         */
        this._handleDisconnect = () => {
            this._pool = mysql.createPool(config);
            this._pool.on('error', (err) => {
                if (err.code === 'PROTOCOL_CONNECTION_LOST') {
                    this._handleDisconnect();
                } else {
                    console.error(err);
                }
            });
        };

        /**
         * Get a new MySQL connection from pool.
         * @param callback
         * @private
         */
        this._getConnection = (callback) => {
            this._pool.getConnection((err, connection) => {
                if (err) {
                    if (connection) {
                        connection.release();
                    }
                }

                callback(err, connection);
            });
        };

        /**
         * Handle MySQL errors for both transaction and non transaction connections.
         * @param err
         * @param trans
         * @param conn
         * @param callback
         * @private
         */
        this._handleError = (err, trans, conn, callback) => {
            if (trans && conn) {
                this.rollback(conn, err);
            }

            callback(err);
        };

        this._handleDisconnect();
    }

    /**
     * Return the class name
     */
    get className() {
        return this._className;
    }

    /**
     * Gets a new connection from the connection pool
     * @method getConnection
     * @param callback
     */
    getConnection(callback) {
        this._getConnection((err, conn) => {
            if (err) {
                callback(err);
                return;
            }

            if (!conn) {
                callback("Failed to obtain a MySQL connection.");
                return;
            }

            callback(null, conn);
        });
    };

    /**
     * Commit the current connection back to the pool
     * @method commitConnection
     * @param conn
     */
    commitConnection(conn) {
        conn.commit((err) => {
            if (err) {
                conn.rollback(() => {
                    console.error('Transaction failed, rolling back connection...');
                });
            }

            conn.release();
        });

        return false;
    };

    /**
     * Perform a rollback of the current transaction
     * @method rollback
     * @param conn
     * @param err
     */
    rollback(conn, err) {
        if (conn) {
            conn.rollback(() => {
                console.error('Transaction failed, rolling back connection...');
            });
        }

        return false;
    };

    /**
     * Get and begin a transaction connection
     * @method beginTransaction
     * @param callback
     */
    beginTransaction(callback) {
        this._getConnection((err, conn) => {
            if (err) {
                callback(err);
                return;
            }

            conn.beginTransaction((err) => {
                if (err) {
                    conn.release();
                }

                callback(err, conn);
            });
        });
    };

    /**
     * Perform a standard select statement
     * @method select
     * @param sql
     * @param params
     * @param conn
     * @param trans
     * @param callback
     */
    select(sql, params, conn, trans, callback) {
        async.waterfall([
            /**
             * If its not a transaction then we need to get a new connection
             */
            (next) => {
                if (conn) {
                    next(null, conn);
                    return;
                }

                this.getConnection(next);
            },

            /**
             * Perform the query, if its not a transaction then release the connection
             */
            (conn, next) => {
                conn.query(sql, params, (err, rows) => {
                    if (!trans) {
                        conn.release();
                    }

                    next(err, rows);
                });
            }
        ], (err, rows) => {
            if (err) {
                this._handleError(err, trans, conn, callback);
                return;
            }

            callback(null, rows);
        });
    };

    /**
     * Perform a standard insert statement
     * @method insert
     * @param sql
     * @param params
     * @param conn
     * @param trans
     * @param callback
     * @param includeAffectedRows (default: false)
     */
    insert(sql, params, conn, trans, callback, includeAffectedRows = false) {
        async.waterfall([
            /**
             * If its not a transaction then we need to get a new connection
             */
            (next) => {
                if (conn) {
                    next(null, conn);
                    return;
                }

                this.getConnection(next);
            },

            /**
             * Perform the query, if its not a transaction then release the connection
             * return the insertID and the number of affected rows
             */
            (conn, next) => {
                conn.query(sql, params, (err, data) => {
                    if (!trans) {
                        conn.release();
                    }

                    next(err, data ? data.insertId : false, data ? data.affectedRows : false);
                });
            }
        ], (err, insertID, affectedRows) => {
            if (err) {
                this._handleError(err, trans, conn, callback);
                return;
            }

            if (includeAffectedRows) {
                callback(null, insertID, affectedRows);
            } else {
                callback(null, insertID);
            }
        });
    };

    /**
     * Perform a standard update or delete query
     * @method update
     * @param sql
     * @param params
     * @param conn
     * @param trans
     * @param callback
     * @param includeAffectedRows (default: false)
     */
    update(sql, params, conn, trans, callback, includeAffectedRows = false) {
        async.waterfall([
            /**
             * If its not a transaction then we need to get a new connection
             */
            (next) => {
                if (conn) {
                    next(null, conn);
                    return;
                }

                this.getConnection(next);
            },

            /**
             * Perform the query, if its not a transaction then release the connection
             * return the number of affected rows
             */
            (conn, next) => {
                conn.query(sql, params, (err, data) => {
                    if (!trans) {
                        conn.release();
                    }

                    next(err, data ? data.affectedRows : false);
                });
            }
        ], (err, affectedRows) => {
            if (err) {
                this._handleError(err, trans, conn, callback);
                return;
            }

            if (includeAffectedRows) {
                callback(null, affectedRows);
            } else {
                callback(null);
            }
        });
    };
}