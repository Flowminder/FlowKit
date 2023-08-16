/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
Cypress.Cookies.debug(true);
describe("User details screen", function () {
  var username, password;

  beforeEach(function () {
    // Log in and navigate to user details screen
    username = Math.random().toString(36).substring(2, 15);
    password = "ORIGINAL_DUMMY_PASSWORD";
    cy.create_user_and_log_in(username, password)
      .log("Created user: " + username + ":" + password)
      .goto("/")
      .get("#user_details")
      .click();
  });

  it("Change user password", function () {
    // Change password
    cy.get("#oldPassword").type(password);
    cy.get("#newPasswordA").type("ANOTHER_DUMMY_PASSWORD");
    cy.get("#newPasswordB").type("ANOTHER_DUMMY_PASSWORD");
    cy.contains("Save").click();
    // Check that snackbar appears and inputs are cleared
    cy.contains("Password changed").should("be.visible");
    cy.get("#oldPassword").should("have.value", "");
    cy.get("#newPasswordA").should("have.value", "");
    cy.get("#newPasswordB").should("have.value", "");
    // Check that we can log in with new password
    cy.get("#logout").click();
    cy.get("#username").type(username);
    cy.get("#password").type("ANOTHER_DUMMY_PASSWORD");
    cy.get("button").click();
    cy.contains("My Servers");
  });

  it("Display error when old password is incorrect", function () {
    // Attempt to change password with incorrect old password
    cy.get("#oldPassword").type("WRONG_PASSWORD");
    cy.get("#newPasswordA").type("ANOTHER_DUMMY_PASSWORD");
    cy.get("#newPasswordB").type("ANOTHER_DUMMY_PASSWORD");
    cy.contains("Save").click();
    // Check that dialog appears with correct title and description
    cy.get("#error-dialog-title").should("contain", "Error");
    cy.get("#error-dialog-description").should(
      "contain",
      "Password incorrect.",
    );
  });

  it("Display error when passwords do not match", function () {
    // Attempt to change password with non-matching new passwords
    cy.get("#oldPassword").type(password);
    cy.get("#newPasswordA").type("ANOTHER_DUMMY_PASSWORD");
    cy.get("#newPasswordB").type("DIFFERENT_DUMMY_PASSWORD");
    cy.contains("Save").click();
    // Check that dialog appears with correct title and description
    cy.get("#error-dialog-title").should("contain", "Error");
    cy.get("#error-dialog-description").should(
      "contain",
      "Passwords do not match.",
    );
  });

  it("Display error when password is not complex enough", function () {
    // Attempt to change password to something too simple
    cy.get("#oldPassword").type(password);
    cy.get("#newPasswordA").type("PASSWORD");
    cy.get("#newPasswordB").type("PASSWORD");
    cy.contains("Save").click();
    // Check that dialog appears with correct title and description
    cy.get("#error-dialog-title").should("contain", "Error");
    cy.get("#error-dialog-description").should(
      "contain",
      "Password not complex enough.",
    );
  });

  it("Error dialog re-appears after closing if password wasn't changed", function () {
    // Attempt to change password with incorrect old password
    cy.get("#oldPassword").type("WRONG_PASSWORD");
    cy.get("#newPasswordA").type("ANOTHER_DUMMY_PASSWORD");
    cy.get("#newPasswordB").type("ANOTHER_DUMMY_PASSWORD");
    cy.contains("Save").click();
    // Close dialog for the first time
    cy.contains("OK").click().should("not.exist");
    // Check that dialog reappears if we try logging in again
    cy.contains("Save").click();
    cy.get("#error-dialog");
  });

  it("Error dialog does not reappear when typing in input", function () {
    // Attempt to change password with incorrect old password
    cy.get("#oldPassword").type("WRONG_PASSWORD");
    cy.get("#newPasswordA").type("ANOTHER_DUMMY_PASSWORD");
    cy.get("#newPasswordB").type("ANOTHER_DUMMY_PASSWORD");
    cy.contains("Save").click();
    // Close dialog
    cy.contains("OK").click().should("not.exist");
    // Check that dialog doesn't reappear when we type in the username input
    cy.get("#newPasswordA").type("_CHANGED");
    cy.get("#error-dialog").should("not.exist");
  });
});
