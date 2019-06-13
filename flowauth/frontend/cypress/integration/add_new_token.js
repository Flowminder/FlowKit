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
  it("Submit without any permissions checked", function() {
    cy.get("#new").click();
    //add token name
    cy.get("#name").type("TOKEN_TEST02", {
      force: true
    });
    //unchecking permission top level checkbox
    cy.get("#permissions").click();
    //uncheck aggrigation unit top-level checkbox
    cy.get("#units").click();
    cy.contains("Save").click();
    cy.get("#warning-dialog-description").should(
      "have.text",
      "Warning: no permissions will be granted by this token. Are you sure?"
    );
    cy.get("#warning-dialog-yes").click();
    cy.contains("TOKEN_TEST02").should("be.visible");
  });

  it("API permisions sub-level checkboxs checked", function() {
    cy.get("#new").click();
    //unchecking permission top level checkbox
    cy.get("#permissions").click();
    //checking permission top level checkbox
    cy.get("#permissions").click();
    cy.get("#api-exp").click();
    //checking first sub-level permission checkbox
    cy.get("#permission")
      .first()
      .should("be.checked");
  });

  it("API permision sub-level checkboxs unchecked", function() {
    cy.get("#new").click();

    //unchecking permission top level checkbox
    cy.get("#permissions").click();
    cy.get("#api-exp").click();
    //checking first sub-level permission checkbox
    cy.get("#permission")
      .first()
      .should("not.be.checked");
  });
  it("Top-level API permisions checkbox checked", function() {
    cy.get("#new").click();
    cy.get("#api-exp").click();
    //unchecking first sub-level checkbox
    cy.get("#permission")
      .first()
      .click({
        force: true
      });
    //unchecking first sub-level checkbox
    cy.get("#permission")
      .first()
      .click({
        force: true
      });
    //checking top-level permission checkbox
    cy.get("#permissions").should("be.checked");
  });
  it("Top-level API permisions checkbox unchecked", function() {
    cy.get("#new").click();
    //unchecking permission top level checkbox
    cy.get("#permissions").click();
    cy.get("#api-exp").click();
    //checking first sub-level checkbox
    cy.get("#permission")
      .first()
      .click({
        force: true
      });
    //checking first sub-level checkbox
    cy.get("#permission")
      .first()
      .click({
        force: true
      });

    cy.get("#permissions").should("not.be.checked");
  });
  it("Top-level API permisions checkbox intermidiate", function() {
    cy.get("#new").click();
    cy.get("#api-exp").click();
    //checking first sub-level permission checkbox
    cy.get("#permission")
      .first()
      .click({
        force: true
      });
    cy.get("#permissions").should("have.attr", "data-indeterminate", "true");
  });
  it("Aggrigation unit sub-level checkboxs checked", function() {
    cy.get("#new").click();
    //uncheck aggrigation unit top-level checkbox
    cy.get("#units").click();
    //check aggrigation unit top-level checkbox
    cy.get("#units").click();
    cy.get("#unit-exp").click();
    cy.get("#unit")
      .first()
      .should("be.checked");
  });
  it("Aggrigation unit sub-level checkboxs unchecked", function() {
    cy.get("#new").click();
    //uncheck aggrigation unit top-level checkbox
    cy.get("#units").click();
    cy.get("#unit-exp").click();
    cy.get("#unit")
      .first()
      .should("not.be.checked");
  });
  it("Top-level Aggrigation unit checkbox checked", function() {
    cy.get("#new").click();
    cy.get("#unit-exp").click();
    //uncheck first sub-level checkbox
    cy.get("#unit")
      .first()
      .click({
        force: true
      });
    //check first sub-level checkbox
    cy.get("#unit")
      .first()
      .click({
        force: true
      });
    cy.get("#units").should("be.checked");
  });
  it("Top-level Aggrigation unit checkbox unchecked", function() {
    cy.get("#new").click();
    //uncheck aggrigation unit top-level checkbox
    cy.get("#units").click();
    cy.get("#unit-exp").click();
    //check first sub-level checkbox
    cy.get("#unit")
      .first()
      .click({
        force: true
      });
    //uncheck first sub-level checkbox
    cy.get("#unit")
      .first()
      .click({
        force: true
      });
    cy.get("#units").should("not.be.checked");
  });
  it("Top-level Aggrigation unit checkbox intermidiate", function() {
    cy.get("#new").click();
    cy.get("#unit-exp").click();
    //uncheck first sub-level checkbox
    cy.get("#unit")
      .first()
      .click({
        force: true
      });
    cy.get("#units").should("have.attr", "data-indeterminate", "true");
  });
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
