// ***********************************************
// This example commands.js shows you how to
// create various custom commands and overwrite
// existing commands.
//
// For more comprehensive examples of custom
// commands please read more here:
// https://on.cypress.io/custom-commands
// ***********************************************
//
//
// -- This is a parent command --
// Cypress.Commands.add("login", (email, password) => { ... })
//
//
// -- This is a child command --
// Cypress.Commands.add("drag", { prevSubject: 'element'}, (subject, options) => { ... })
//
//
// -- This is a dual command --
// Cypress.Commands.add("dismiss", { prevSubject: 'optional'}, (subject, options) => { ... })
//
//
// -- This is will overwrite an existing command --
// Cypress.Commands.overwrite("visit", (originalFn, url, options) => { ... })

const { dateTimePickerDefaultProps } = require("@material-ui/pickers/constants/prop-types");

// Regex developed and explained at https://regex101.com/r/oBJsrs/1
const pep440_regex = new RegExp("(?<version>[0-9]+(?:\.[0-9]+)+)-(?<dev>[0-9]*)-(?<revision>\w*)(-(?<dirty>dirty))?", "mg")

function getCookieValue(a) {
  var b = document.cookie.match("(^|;)\\s*" + a + "\\s*=\\s*([^;]+)");
  return b ? b.pop() : "";
}

Cypress.Commands.add("build_version_string", () => {
  cy.exec('git describe --tags --dirty --always').then((result) =>{
    console.debug(pep440_regex)
    const {version, dev, revision, dirty} = result.stdout.matchAll(pep440_regex).next().value.groups
    const outstring =  `${version}.post0.dev${dev}` 
    console.debug(outstring)
    return outstring
  })}
)

Cypress.Commands.add("login", () =>
  cy.goto("/").request("POST", "/signin", {
    username: "TEST_USER",
    password: "DUMMY_PASSWORD",
  })
);
Cypress.Commands.add("create_two_factor_user", (username, password) =>
  cy.login_admin().then((response) => {
    return cy
      .request({
        method: "POST",
        url: "/admin/users",
        body: {
          username: username,
          password: password,
          require_two_factor: true,
        },
        headers: { "X-CSRF-Token": getCookieValue("X-CSRF") },
      })
      .its("body");
  })
);
  Cypress.Commands.add("create_user", (username, password) =>
  cy.login_admin().then((response) =>
    cy.request({
      method: "POST",
      url: "/admin/users",
      body: {
        username: username,
        password: password,
      },
      headers: { "X-CSRF-Token": getCookieValue("X-CSRF") },
    })
  )
);
Cypress.Commands.add("create_role", (role_name) =>
  cy.login_admin().then((response) =>
    cy
      .request({
        method: "POST",
        url: "/roles/",
        body: {
          name: role_name,
          scopes: [0],
          latest_token_expiry: "2121-12-31T00:00:00.000000Z",
          server_id: 1,
          longest_token_life_minutes: 2*24*60
        },
        headers: { "X-CSRF-Token": getCookieValue("X-CSRF") },
      })
      .its("body")
  )
);
Cypress.Commands.add("create_user_and_log_in", (username, password) =>
  cy
    .create_user(username, password)
    .its("body")
    .then((response) => {
      cy.clearCookies();
      cy.getCookies().should("be.empty");
      cy.request("POST", "/signin", {
        username: username,
        password: password,
      }).its("body");
    })
);
Cypress.Commands.add("login_admin", () =>
  cy.request("POST", "/signin", {
    username: "TEST_ADMIN",
    password: "DUMMY_PASSWORD",
  })
);

/*
Workaround for cypress holding onto old cookies
https://github.com/cypress-io/cypress/issues/3438#issuecomment-478660605
 */

Cypress.Commands.add("goto", { prevSubject: false }, (url, options = {}) => {
  if (options.log !== false) {
    Cypress.log({
      name: "goto",
      message: `Goto ${url}`,
    });
  }

  const target = new URL("http://localhost" + url);
  const params = new URLSearchParams(target.search);
  params.append("cypressBuffferFix", Date.now());
  const adjusted = target.pathname + "?" + params.toString() + target.hash;
  cy.visit(adjusted, {
    log: false,
  });
});

/*
Workaround for uploading files in Cypress
https://github.com/cypress-io/cypress/issues/170#issuecomment-533519550
 */

Cypress.Commands.add(
  "uploadFile",
  { prevSubject: true },
  (subject, fileName) => {
    cy.fixture(fileName).then((content) => {
      const el = subject[0];
      const testFile = new File([JSON.stringify(content)], fileName);
      const dataTransfer = new DataTransfer();

      dataTransfer.items.add(testFile);
      el.files = dataTransfer.files;
      cy.wrap(subject).trigger("change", { force: true });
    });
  }
);
