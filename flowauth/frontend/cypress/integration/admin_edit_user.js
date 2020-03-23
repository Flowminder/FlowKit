/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("User management", function () {
  Cypress.Cookies.debug(true);

  beforeEach(function () {
    // Log in and navigate to user details screen
    cy.login_admin();
    cy.goto("/");
    cy.get("#user_list").click();
  });
  it("Edit existing user password", function () {
    const user_name = Math.random().toString(36).substring(2, 15);
    cy.get("#new").click();
    //Add new user with password
    cy.get("#username").type(user_name);
    cy.get("#password").type("DUMMY_ORIGINAL_PASSWORD");
    cy.contains("Save").click();
    cy.get("[data-action=edit][data-item-name=" + user_name + "]").click();
    cy.get("#password").type("DUMMY_UPDATED_PASSWORD");
    cy.contains("Save").click();
    cy.get("[data-action=edit][data-item-name=" + user_name + "]").should(
      "exist"
    );
    cy.request("/signout");
    cy.request("POST", "/signin", {
      username: user_name,
      password: "DUMMY_UPDATED_PASSWORD",
    });
  });
});
