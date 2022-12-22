/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Token generation", function () {
  Cypress.Cookies.debug(true);
  beforeEach(function () {
    // Log in and navigate to user details screen
    cy.login();
    //adding wait time to load permissions
    cy.wait(250);
    cy.goto("/");
    cy.get("#servers").click();
  });

  it("Add token name with space", function () {
    cy.get("#new").click();
    // adding token name with space
    //{force:true} added for test to skip scrooling issue.
    cy.get("#name").type("Token ", {
      force: true,
    });
    //checking validation text
    cy.get("#name-helper-text").should(
      "have.text",
      "Token name may only contain letters, numbers and underscores."
    );
    cy.get("#name").clear({
      force: true,
    });

    cy.get("#name").type("TEXT_INPUT_TEST", {
      force: true,
    });
    cy.contains("#name-helper-text").should("not.exist");
  });

  it("Add blank token name", function () {
    cy.get("#new").click();
    //adding blank token name
    cy.get("#name").type(" ", {
      force: true,
    });
    cy.get("#name").clear({
      force: true,
    });
    //checking validation text
    cy.get("#name-helper-text").should(
      "have.text",
      "Token name cannot be blank."
    );
    cy.get("#name").clear({
      force: true,
    });
    cy.get("#name").type("TOKEN_TEST01", {
      force: true,
    });
    cy.contains("#name-helper-text").should("not.exist");
  });

  it("Submit without any permissions checked", function () {
    cy.get("#new").click();
    //add token name
    cy.get("#name").type("TOKEN_TEST02", {
      force: true,
    });
    cy.contains("Save").click();
    cy.get(".MuiDialog-container").should(
      "have.text",
      "ErrorAt least one role must be selectedClose"
    );
  });

  it("Add new token", function () {
    cy.get("#new").click();
    //Add new token
    cy.get("#name").type("auto_test_token", {
      force: true,
    });
    cy.get(".MuiGrid-grid-xs-8 > .MuiButton-root").click();
    cy.contains("Save").click();
    cy.contains("auto_test_token").should("be.visible");
  });
});
