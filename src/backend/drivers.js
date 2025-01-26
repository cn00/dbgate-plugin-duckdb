const _ = require('lodash');
const stream = require('stream');

const driverBases = require('../frontend/drivers');
const Analyser = require('./Analyser');
const { splitQuery, postgreSplitterOptions } = require('dbgate-query-splitter');
const wkx = require('wkx');
const duckdb = require('duckdb-async');
const pgCopyStreams = require('pg-copy-streams');
const {
  getLogger,
  createBulkInsertStreamBase,
  makeUniqueColumnNames,
  extractDbNameFromComposite,
  extractErrorLogData,
} = global.DBGATE_PACKAGES['dbgate-tools'];

let authProxy;

const logger = getLogger('duckdbDriver');

function extractGeographyDate(value) {
  try {
    const buffer = Buffer.from(value, 'hex');
    const parsed = wkx.Geometry.parse(buffer).toWkt();

    return parsed;
  } catch (_err) {
    return value;
  }
}

function transformRow(row, columnsToTransform) {
  if (!columnsToTransform?.length) return row;

  for (const col of columnsToTransform) {
    const { columnName, dataTypeName } = col;
    if (dataTypeName == 'geography') {
      row[columnName] = extractGeographyDate(row[columnName]);
    }
  }

  return row;
}

function extractPostgresColumns(result, dbhan) {
  if (!result || !result.fields) return [];

  const { typeIdToName = {} } = dbhan;
  const res = result.fields.map(fld => ({
    columnName: fld.name,
    dataTypeId: fld.dataTypeID,
    dataTypeName: typeIdToName[fld.dataTypeID],
  }));
  makeUniqueColumnNames(res);
  return res;
}

function zipDataRow(rowArray, columns) {
  return _.zipObject(
    columns.map(x => x.columnName),
    rowArray
  );
}

function runStreamItem(dbhan, sql, options, rowCounter) {
  const stmt = dbhan.client.prepare(sql);
  if (stmt.reader) {
    const columns = stmt.columns();
    // const rows = stmt.all();

    options.recordset(
      columns.map((col) => ({
        columnName: col.name,
        dataType: col.type,
      }))
    );

    for (const row of stmt.iterate()) {
      options.row(row);
    }
  } else {
    const info = stmt.run();
    rowCounter.count += info.changes;
    if (!rowCounter.date) rowCounter.date = new Date().getTime();
    if (new Date().getTime() > rowCounter.date > 1000) {
      options.info({
        message: `${rowCounter.count} rows affected`,
        time: new Date(),
        severity: 'info',
      });
      rowCounter.count = 0;
      rowCounter.date = null;
    }
  }
}
  
/** @type {import('dbgate-types').EngineDriver} */
const drivers = driverBases.map(driverBase => ({
  ...driverBase,
  analyserClass: Analyser,

  async connect(props) {
    const {
      engine,
      server,
      port,
      user,
      password,
      database,
      databaseUrl,
      useDatabaseUrl,
      ssl,
      isReadOnly,
      authType,
      socketPath,
    } = props;
    /**
     * @type {import('pg').ClientConfig}
     */
    let options = null;

    let awsIamToken = null;
    if (authType == 'awsIam') {
      awsIamToken = await authProxy.getAwsIamToken(props);
    }

    if (engine == 'redshift@dbgate-plugin-duckdb') {
      let url = databaseUrl;
      if (url && url.startsWith('jdbc:redshift://')) {
        url = url.substring('jdbc:redshift://'.length);
      }
      if (user && password) {
        url = `postgres://${user}:${password}@${url}`;
      } else if (user) {
        url = `postgres://${user}@${url}`;
      } else {
        url = `postgres://${url}`;
      }

      options = {
        connectionString: url,
      };
    } else {
      options = useDatabaseUrl
        ? {
            connectionString: databaseUrl,
            application_name: 'DbGate',
          }
        : {
            host: authType == 'socket' ? socketPath || driverBase.defaultSocketPath : server,
            port: authType == 'socket' ? null : port,
            user,
            password: awsIamToken || password,
            database: extractDbNameFromComposite(database) || 'duckdb',
            ssl: authType == 'awsIam' ? ssl || { rejectUnauthorized: false } : ssl,
            application_name: 'DbGate duckdb',
          };
    }
    // const client = new pg.Client(options);
    let accessMode = isReadOnly ? duckdb.OPEN_READONLY : duckdb.OPEN_READWRITE;
    const client = await duckdb.Database.create(databaseUrl, accessMode);
    await client.connect();

    const dbhan = {
      client,
      database,
    };

    const datatypes = await this.query(dbhan, `SELECT oid::int, typname FROM pg_type WHERE typname in ('geography')`);
    const typeIdToName = _.fromPairs(datatypes.rows.map(cur => [cur.oid, cur.typname]));
    dbhan['typeIdToName'] = typeIdToName;

    // if (isReadOnly) {
    //   await this.query(dbhan, 'SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY');
    // }

    return dbhan;
  },
  async close(dbhan) {
    return dbhan.client.close();
  },
  async query(dbhan, sql) {
    const stmt = await dbhan.client.prepare(sql);
    logger.info(`prepare sql: ${sql}\nstmt.sql: ${stmt.stmt.sql}`);
    // stmt.raw();
    try {
      const columns = stmt.columns();
      const rows = await stmt.all();
      // bigint to Number
      rows.forEach(row => {
        columns.forEach(col => {
          if (col.type == 'bigint') {
            row[col.name] = Number(row[col.name]);
          }
        });
      })
      return {
        rows,
        columns: columns.map((col) => ({
          columnName: col.name,
          dataType: col.type,
        })),
      };
    } catch (error) {
      logger.error(extractErrorLogData(error), 'Query error');
      throw error;
    } finally {
    //   logger.info(`Finalizing sql: ${stmt.stmt.sql}`);
      await stmt.finalize();
    }
  },
  async stream(dbhan, sql, options) {
    const query = await dbhan.client.prepare(sql);
    const columns = query.columns();
    options.recordset(columns);
    const rows = await query.all();
    for (const row of rows) {
      options.row(row);
    }
    options.done();
  },
  async getVersion(dbhan) {
    const { rows } = await this.query(dbhan, 'select version() as version');
    const { version } = rows[0];

    return {
      version,
      versionText: `duckdb ${version}`,
    };
  },
  async readQueryTask(stmt, pass) {
    let sent = 0;
    for (const row of stmt.iterate()) {
      sent++;
      if (!pass.write(row)) {
        console.log('WAIT DRAIN', sent);
        await waitForDrain(pass);
      }
    }
    pass.end();
  },
  async readQuery(dbhan, sql, structure) {
    logger.debug('readQuery', sql);
    const pass = new stream.PassThrough({
      objectMode: true,
      highWaterMark: 100,
    });

    const stmt = await dbhan.client.prepare(sql);
    const columns = stmt.columns();

    pass.write({
      __isStreamHeader: true,
      ...(structure || {
        columns: columns.map((col) => ({
          columnName: col.name,
          dataType: col.type,
        })),
      }),
    });
    this.readQueryTask(stmt, pass);

    return pass;
  },
  async writeTable(dbhan, name, options) {
    // @ts-ignore
    return createBulkInsertStreamBase(this, stream, dbhan, name, options);
  },
//   async listDatabases(dbhan) {
//     const { rows } = await this.query(dbhan, "SELECT datname AS name FROM pg_database WHERE datname != 'temp'");
//     return rows;
//   },
  async listDatabases(dbhan) {
    const { rows } = await this.query(dbhan, 'show databases;');
    return rows.map(db => ({name: db.database_name}))
  },

  getAuthTypes() {
    const res = [
      {
        title: 'Host and port',
        name: 'hostPort',
      },
      {
        title: 'Socket',
        name: 'socket',
      },
    ];
    if (authProxy.supportsAwsIam()) {
      res.push({
        title: 'AWS IAM',
        name: 'awsIam',
      });
    }
    return res;
  },

  async listSchemas(dbhan) {
    const schemaRows = await this.query(
      dbhan,
      'select oid::int as "object_id", nspname as "schema_name" from pg_catalog.pg_namespace'
    );
    const defaultSchemaRows = await this.query(dbhan, 'SELECT current_schema');
    const defaultSchema = defaultSchemaRows.rows[0]?.current_schema?.trim();

    logger.debug(`Loaded ${schemaRows.rows.length} duckdb schemas`);

    const schemas = schemaRows.rows.map(x => ({
      schemaName: x.schema_name,
      objectId: x.object_id,
      isDefault: x.schema_name == defaultSchema,
    }));

    return schemas;
  },

  async writeQueryFromStream(dbhan, sql) {
    const stream = await dbhan.client.query(pgCopyStreams.from(sql));
    return stream;
  },
}));

drivers.initialize = dbgateEnv => {
  authProxy = dbgateEnv.authProxy;
};

module.exports = drivers;
