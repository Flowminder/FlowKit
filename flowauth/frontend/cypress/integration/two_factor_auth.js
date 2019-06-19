/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
Cypress.Cookies.debug(true);
describe("Two-factor setup", function() {
  var username, password;

  beforeEach(function() {
    // Log in and navigate to user details screen
    username = Math.random()
      .toString(36)
      .substring(2, 15);
    password = "ORIGINAL_DUMMY_PASSWORD";
    cy.create_two_factor_user(username, password)
      .clearCookies()
      .goto("/");
  });

  it("Two factor setup as part of login can be successfully completed.", function() {
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
      .click()
      .get("[data-id=qr_code]")
      .invoke("attr", "data-secret")
      .then(secret => {
        cy.exec("npx otp-cli totp generate -k " + secret.split(".")[0]).then(
          mfaCode => {
            cy.get("input")
              .type(mfaCode.stdout)
              .get("[data-button-id=submit")
              .click();
            cy.contains("My Servers").should("be.visible");
          }
        );
      });
  });

  it("Two factor setup as part of login must be completed to continue.", function() {
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
});
