// ***********************************************************
// This example plugins/index.js can be used to load plugins
//
// You can change the location of this file or turn off loading
// the plugins file with the 'pluginsFile' configuration option.
//
// You can read more here:
// https://on.cypress.io/plugins-guide
// ***********************************************************

// This function is called when a project is opened or re-opened (e.g. due to
// the project's config changing)

module.exports = (on, config) => {
  // Add in the public key from the parent environment to cypress'
  config.env = config.env || {};
  config.env.PUBLIC_JWT_SIGNING_KEY = process.env.PUBLIC_JWT_SIGNING_KEY;
  console.log("extended config.env with process.env.{PUBLIC_JWT_SIGNING_KEY}");
  return config;
};
