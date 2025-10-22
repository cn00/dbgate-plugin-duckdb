const drivers = require('./drivers');

module.exports = {
  packageName: 'dbgate-plugin-duckdb2',
  drivers,
  initialize(dbgateEnv) {
    drivers.initialize(dbgateEnv);
  },
};
