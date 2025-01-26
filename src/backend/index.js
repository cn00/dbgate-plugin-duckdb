const drivers = require('./drivers');

module.exports = {
  packageName: 'dbgate-plugin-duckdbpg',
  drivers,
  initialize(dbgateEnv) {
    drivers.initialize(dbgateEnv);
  },
};
