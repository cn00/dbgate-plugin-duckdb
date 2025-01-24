const driver = require('./driver');

module.exports = {
  packageName: 'dbgate-plugin-duckdblite',
  drivers: [driver],
  initialize(dbgateEnv) {
    driver.initialize(dbgateEnv);
  },
};
