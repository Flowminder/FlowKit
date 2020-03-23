/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
Cypress.Cookies.debug(true);
describe("Two-factor setup", function () {
  var username, password;

  beforeEach(function () {
    // Log in and navigate to user details screen
    username = Math.random().toString(36).substring(2, 15);
    password = "ORIGINAL_DUMMY_PASSWORD";
    cy.create_two_factor_user(username, password).clearCookies().goto("/");
  });

  it("Two factor setup flow.", function () {
    cy.get("#username")
      .type(username)
      .get("#password")
      .type(password)
      .get("button")
      .click()
      .get("[data-button-id=submit]")
      .should("be.disabled");
    cy.get("[data-cy=backup_code]") // Get a backup code for use later
      .first()
      .invoke("text")
      .then((text) => {
        cy.get("[data-button-id=copy]")
          .click()
          .get("[data-button-id=submit]")
          .should("not.be.disabled")
          .click()
          .get("[data-id=qr_code]")
          .invoke("attr", "data-secret")
          .then((secret) => {
            const mfasecret = secret.split(".")[0];
            cy.exec("npx otp-cli totp generate -k " + mfasecret).then(
              (mfaCode) => {
                cy.get("input")
                  .type(mfaCode.stdout)
                  .get("[data-button-id=submit]")
                  .click();
                cy.contains("My Servers").should("be.visible");
                cy.get("#logout").click();
                cy.get("#username")
                  .type(username)
                  .get("#password")
                  .type(password)
                  .get("button")
                  .click()
                  .get("#two_factor_code")
                  .type(mfaCode.stdout) // Log in using backup code because we don't want to wait for a new mfa code
                  .get("button")
                  .click()
                  .get("#error-dialog-description")
                  .should("have.text", "Code not valid.");
                cy.get("#error-dialog-ok").click();
              }
            );
          });
        cy.get("#signin-button")
          .click()
          .get("#two_factor_code")
          .type(text) // Log in using backup code because we don't want to wait for a new mfa code
          .get("button")
          .click();
        cy.contains("My Servers").should("be.visible");
      });
  });

  it("Two factor setup as part of login must be completed to continue.", function () {
    cy.get("#username")
      .type(username)
      .get("#password")
      .type(password)
      .get("button")
      .click()
      .get("[data-button-id=submit]")
      .should("be.disabled")
      .get("[data-button-id=copy]")
      .click()
      .get("[data-button-id=submit]")
      .should("not.be.disabled")
      .click();
    cy.goto("/");
    cy.get("[data-cy=backup_code]").should("be.visible");
  });

  it("A user can enable two-factor authentication themselves", function () {
    username = Math.random().toString(36).substring(2, 15);
    cy.create_user_and_log_in(username, password)
      .goto("/")
      .get("#user_details")
      .click()
      .get("#enable_two_factor")
      .click();
    cy.get("[data-button-id=copy]")
      .click()
      .get("[data-button-id=submit]")
      .should("not.be.disabled")
      .click()
      .get("[data-id=qr_code]")
      .invoke("attr", "data-secret")
      .then((secret) => {
        const mfasecret = secret.split(".")[0];
        cy.exec("npx otp-cli totp generate -k " + mfasecret).then((mfaCode) => {
          cy.get("input")
            .type(mfaCode.stdout)
            .get("[data-button-id=submit]")
            .click();
          cy.get("#reset_two_factor").should("be.visible");
        });
      });
  });
});
