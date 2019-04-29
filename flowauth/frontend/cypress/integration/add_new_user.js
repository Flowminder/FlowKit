/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Login screen", function () {
  Cypress.Cookies.debug(true);

  beforeEach(function () {
    // Log in and navigate to user details screen
    cy.login_admin();
    cy.visit("/");
    cy.get("#user_list").click();
  });

  it("Add Username with space", function () {
    cy.get("#new").click();
    // adding username with space
    cy.get("#username").type("USER ");
    //checking validation text
    cy.get("#username-helper-text").should(
      "have.text",
      "Username may on contain letters, numbers and underscores."
    );
    cy.get("#username")
      .type(" ")
      .clear();
    cy.get("#username").type("USER_TEST01");
    cy.contains("#username-helper-text").should("not.exist");
  });
  it("Add blank Username", function () {
    cy.get("#new").click();
    //adding blank username
    cy.get("#username")
      .type(" ")
      .clear();
    //checking validation text
    cy.get("#username-helper-text").should(
      "have.text",
      "Username can not be blank."
    );
    cy.get("#username").type("USER_TEST01");
    cy.contains("#username-helper-text").should("not.exist");
  });
  it("Add blank Password", function () {
    cy.get("#new").click();
    //Add blank password
    cy.get("#password")
      .type(" ")
      .clear();
    cy.get("#password-helper-text").should(
      "have.text",
      "Use a few words, avoid common phrases"
    );
    cy.get("#password").type("C>K,7|~44]44:ibK");
    cy.get("#password-helper-text").should("not.exist");
  });
  it("Add password with less strength", function () {
    cy.get("#new").click();
    //Add password with less strength
    cy.get("#password").type("USER_TEST");
    cy.get("#password-helper-text").should(
      "have.text",
      "Add another word or two. Uncommon words are better."
    );
    cy.get("#password")
      .type(" ")
      .clear();
    cy.get("#password").type("C>K,7|~44]44:ibK");
    cy.get("#password-helper-text").should("not.exist");
  });
  it("Add User", function () {
    cy.get("#new").click();
    //Add new user with password
    cy.get("#username").type("USER_TEST01");
    cy.get("#password").type("C>K,7|~44]44:ibK");
    cy.contains("Save").click();
    cy.contains("USER_TEST01").should("be.visible");
  });
});