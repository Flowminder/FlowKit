/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("User management", function() {
  Cypress.Cookies.debug(true);

  beforeEach(function() {
    // Log in and navigate to user details screen
    cy.login_admin();
    cy.goto("/");
    cy.get("#user_list").click();
  });
  it("Edit existing user password", function() {
    cy.get("#new").click();
    //Add new user with password
    cy.get("#username").type("USER_TEST");
    cy.get("#password").type("DUMMY_ORIGINAL_PASSWORD");
    cy.contains("Save").click();
    cy.get("#edit_3").click();
    cy.get("#password").type("DUMMY_UPDATED_PASSWORD");
    cy.contains("Save").click();
    cy.request("/signout");
    cy.request("POST", "/signin", {
      username: "USER_TEST",
      password: "DUMMY_UPDATED_PASSWORD"
    });
  });
});
