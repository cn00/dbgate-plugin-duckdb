const { driverBase } = require('dbgate-tools');
const Dumper = require('./Dumper');
const { duckdbSplitterOptions, noSplitSplitterOptions } = require('dbgate-query-splitter/lib/options');

function getDatabaseFileLabel(databaseFile) {
  if (!databaseFile) return databaseFile;
  const m = databaseFile.match(/[\/]([^\/]+)$/);
  if (m) return m[1];
  return databaseFile;
}

/** @type {import('dbgate-types').SqlDialect} */
const dialect = {
  limitSelect: true,
  rangeSelect: true,
  offsetFetchRangeSyntax: true,
  stringEscapeChar: "'",
  fallbackDataType: 'nvarchar(max)',
  quoteIdentifier(s) {
    return `[${s}]`;
  },
};

/** @type {import('dbgate-types').EngineDriver} */
const driver = {
  ...driverBase,
  dumperClass: Dumper,
  dialect,
  engine: 'duckdb@dbgate-plugin-duckdb',
  title: 'DuckDb Lite',
  showConnectionField: (field, values) => {
    return ['server', 'isReadOnly'].includes(field);
  },
  databaseUrlPlaceholder: 'e.g. server=:memory:',
};

module.exports = driver;
