/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Public key viewing", function () {
  let public_key;
  beforeEach(function () {
    const blob = Cypress.Blob.base64StringToBlob(
      Cypress.env("PUBLIC_JWT_SIGNING_KEY")
    );
    Cypress.Blob.blobToBinaryString(blob).then((key) => {
      public_key = key;
    });
    // Log in and navigate to user details screen
    cy.login_admin();
    cy.goto("/");
    cy.get("#public_key").click();
    cy.wait(50);
  });
  it("Public key is shown", function () {
    cy.get("#public_key_body").should(
      "have.text",
      public_key.replace(/\n|\r/g, "")
    );
  });
  it("Public key can be downloaded", function () {
    // Ideally we'd test the download works, but this isn't possible headless in cypress (https://github.com/cypress-io/cypress/issues/949)
    cy.get("#download").click();
    cy.get("a[download]").should("exist");
  });
});
