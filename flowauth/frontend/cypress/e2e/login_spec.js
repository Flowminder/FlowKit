/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Login screen", function () {
  beforeEach(function () {
    // Go to login screen
    cy.goto("/");
  });

  it("Log in as a user", function () {
    cy.get("#username").type("TEST_USER");
    cy.get("#password").type("DUMMY_PASSWORD");
    cy.get("button").click();
    cy.contains("viewer").should("exist");
    cy.contains("runner").should("exist");
    cy.getCookie("session").should("exist");
    cy.getCookie("X-CSRF").should("exist");
  });

  it("Fail to log in with incorrect username", function () {
    // Attempt to log in with incorrect username
    cy.get("#username").type("WRONG_USER");
    cy.get("#password").type("DUMMY_PASSWORD");
    cy.get("button").click();
    // Check that dialog appears with correct title and description
    cy.get("#error-dialog-title").should("contain", "Error");
    cy.get("#error-dialog-description").should(
      "contain",
      "Incorrect username or password."
    );
  });

  it("Fail to log in with incorrect password", function () {
    // Attempt to log in with incorrect password
    cy.get("#username").type("TEST_USER");
    cy.get("#password").type("WRONG_PASSWORD");
    cy.get("button").click();
    // Check that dialog appears with correct title and description
    cy.get("#error-dialog-title").should("contain", "Error");
    cy.get("#error-dialog-description").should(
      "contain",
      "Incorrect username or password."
    );
  });

  it("Error dialog re-appears after closing if password wasn't changed", function () {
    // Attempt to log in with incorrect username
    cy.get("#username").type("WRONG_USER");
    cy.get("#password").type("DUMMY_PASSWORD");
    cy.get("button").click();
    // Close dialog for the first time
    cy.contains("OK").click().should("not.exist");
    // Check that dialog reappears if we try logging in again
    cy.get("button").click();
    cy.get("#error-dialog");
  });

  it("Error dialog does not reappear when typing in input", function () {
    // Attempt to log in with incorrect username
    cy.get("#username").type("WRONG_USER");
    cy.get("#password").type("DUMMY_PASSWORD");
    cy.get("button").click();
    // Close dialog
    cy.contains("OK").click().should("not.exist");
    // Check that dialog doesn't reappear when we type in the username input
    cy.get("#username").type("_CHANGED");
    cy.get("#error-dialog").should("not.exist");
  });

  it("Render correct component according to login status", function () {
    // Check that login screen renders initially
    cy.goto("/");
    cy.contains("Sign in");
    // Log in, reload, and check that dashboard renders
    cy.login();
    cy.goto("/");
    cy.contains("My Servers");
    // Log out, reload, and check that login screen renders again
    cy.request("/signout");
    cy.goto("/");
    cy.contains("Sign in");
  });
});
