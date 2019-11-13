/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Server management", function() {
  Cypress.Cookies.debug(true);

  beforeEach(function() {
    // Log in and navigate to user details screen
    cy.login_admin()
      .goto("/")
      .get("#server_list")
      .click();
  });

  it("Add blank maximum lifetime minutes", function() {
    cy.get("#new").click();
    //Add blank maximum lifetime minutes
    cy.get("#max-life")
      .type(" ", { force: true })
      .clear({ force: true });
    cy.get("#max-life-helper-text").should(
      "have.text",
      "Maximum lifetime minutes can not be blank."
    );
    cy.get("#max-life").type("1234", { force: true });
    cy.get("#max-life-helper-text").should("not.exist");
  });
  it("Add duplicate Server name", function() {
    cy.get("#new").click();
    //adding existing server name and new secret key
    cy.get("#name").type("TEST_SERVER", { force: true });
    cy.contains("Save").click();
    //checking error dialogue text
    cy.get("#error-dialog-description").should(
      "have.text",
      "Server with this name already exists."
    );
    cy.contains("OK").click();
    cy.get("#error-dialog-description").should("not.exist");
  });
  it("Add server", function() {
    cy.get("#new").click();
    //Add new user with password
    const server_name = Math.random()
      .toString(36)
      .substring(2, 15);
    cy.get("#name").type(server_name, {
      force: true
    });
    cy.get("#max-life").type("1234", {
      force: true
    });
    cy.contains("Save").click();
    cy.contains(server_name).should("be.visible");
  });
});
