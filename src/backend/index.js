const driver = require('./driver');

module.exports = {
  packageName: 'dbgate-plugin-duckdb',
  drivers: [driver],
  initialize(dbgateEnv) {
    driver.initialize(dbgateEnv);
  },
};
