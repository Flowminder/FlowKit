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

function getCookieValue(a) {
  var b = document.cookie.match("(^|;)\\s*" + a + "\\s*=\\s*([^;]+)");
  return b ? b.pop() : "";
}

Cypress.Commands.add("login", () =>
	cy.request("POST", "/signin", {
		username: "TEST_USER",
		password: "DUMMY_PASSWORD"
	})
);
Cypress.Commands.add("create_user", (username, password) =>

		cy.login_admin().then((response) =>
		cy.request({method: "POST", url: "/admin/users",
			body:{
			username: username,
			password: password}
		, headers: {"X-CSRF-Token":getCookieValue("X-CSRF")}}))

);
Cypress.Commands.add("create_group", (group_name) =>

		cy.login_admin().then((response) =>
		cy.request({method: "POST", url: "/admin/groups",
			body:{
			name: group_name}
		, headers: {"X-CSRF-Token":getCookieValue("X-CSRF")}}).its('body')
		)

);
Cypress.Commands.add("create_user_and_log_in", (username, password) =>

		cy.create_user(username, password).its('body').then((response) =>
			{cy.request("/signout");
		cy.request("POST", "/signin", {
			username: username,
			password: password
		}).its('body')})

);
Cypress.Commands.add("login_admin", () =>
	cy.request("POST", "/signin", {
		username: "TEST_ADMIN",
		password: "DUMMY_PASSWORD"
	})
);
