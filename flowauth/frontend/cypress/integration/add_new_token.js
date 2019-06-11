/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Token generation", function() {
  Cypress.Cookies.debug(true);

  beforeEach(function() {
    // Log in and navigate to user details screen
    cy.login();
    cy.goto("/");
    cy.get("#servers").click();
  });
  it("Add token name with space", function() {
    cy.get("#new").click();
    // adding token name with space
    //{force:true} added for test to skip scrooling issue.
    cy.get("#name").type("Token ", {
      force: true
    });
    //checking validation text
    cy.get("#name-helper-text").should(
      "have.text",
      "Token name may only contain letters, numbers and underscores."
    );
    cy.get("#name").clear({
      force: true
    });

    cy.get("#name").type("TOKEN_TEST01", {
      force: true
    });
    cy.contains("#name-helper-text").should("not.exist");
  });
  it("Add blank token name", function() {
    cy.get("#new").click();
    //adding blank token name
    cy.get("#name").type(" ", {
      force: true
    });
    cy.get("#name").clear({
      force: true
    });
    //checking validation text
    cy.get("#name-helper-text").should(
      "have.text",
      "Token name cannot be blank."
    );
    cy.get("#name").clear({
      force: true
    });
    cy.get("#name").type("TOKEN_TEST01", {
      force: true
    });
    cy.contains("#name-helper-text").should("not.exist");
  });
  it("API permision checkbox checked", function() {
    cy.get("#new").click();
    cy.get("#permissions").check();
    cy.get("#api-exp").click();
    cy.get("#permission")
      .first()
      .check();
  });
  it("API permision checkboxs unchecked", function() {
    cy.get("#new").click();
    cy.get("#api-exp").click();
    cy.get("#permission").uncheck();
    cy.get("#permissions").uncheck();
  });
  it("Aggrigation unit checkbox checked", function() {
    cy.get("#new").click();
    cy.get("#unit-exp").click();
    cy.get("#units").check();
    cy.get("#unit")
      .first()
      .check();
  });
  // it("Aggrigation unit checkboxs unchecked", function () {
  //   cy.get("#new").click();
  //   cy.get("#unit-exp").click();
  //   cy.get("#unit").uncheck();
  //   cy.get("#units").uncheck();
  // });
  it("Add new token", function() {
    cy.get("#new").click();
    //Add new token
    cy.get("#name").type("TOKEN_TEST01", {
      force: true
    });
    cy.contains("Save").click();
    cy.contains("TOKEN_TEST01").should("be.visible");
  });
});
