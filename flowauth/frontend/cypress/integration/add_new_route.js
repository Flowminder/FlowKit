/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Server management", function() {
  Cypress.Cookies.debug(true);

  beforeEach(function() {
    // Log in and navigate to user details screen
    cy.login_admin()
      .goto("/")
      .get("#api_routes")
      .click();
  });
  it("Add a new route", function() {
    // adding username with space
    const route_name = Math.random()
      .toString(36)
      .substring(2, 15);
    cy.get("#name").type(route_name, {
      force: true
    });
    cy.get("#new").click();
    cy.get("#name").should("be.empty");
    cy.contains(route_name).should("exist");
    cy.get("#server_list")
      .click()
      .get("#edit_1")
      .click()
      .get("#api-exp")
      .click();
    cy.get("#permissions").should("have.attr", "data-indeterminate", "true");
    cy.get("[data-permission-id='" + route_name + "_get_result']")
      .children(".MuiIconButton-label")
      .children("#permission")
      .should("not.be.checked")
      .should("not.be.disabled");
    cy.get("[data-permission-id='" + route_name + "_poll']")
      .children(".MuiIconButton-label")
      .children("#permission")
      .should("not.be.checked")
      .should("not.be.disabled");
    cy.get("[data-permission-id='" + route_name + "_run']")
      .children(".MuiIconButton-label")
      .children("#permission")
      .should("not.be.checked")
      .should("not.be.disabled");
    cy.get("#unit-exp").click();
    cy.get('[data-aggregation-id="' + route_name + '_admin0"]')
      .children(".MuiIconButton-label")
      .children("#unit")
      .should("not.be.disabled");
    cy.get("#api_routes").click();
    cy.contains(route_name)
      .parent()
      .parent()
      .children()
      .last()
      .click({
        force: true
      });
    cy.contains(route_name).should("not.exist");
  });
});
