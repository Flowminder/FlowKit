/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Public key viewing", function () {
    let public_key;
    beforeEach(function () {
        Cypress.Blob.base64StringToBlob(Cypress.env('PUBLIC_JWT_SIGNING_KEY')).then((blob) => Cypress.Blob.blobToBinaryString(blob).then((key) => {public_key = key}));
        // Log in and navigate to user details screen
        cy.login_admin();
        cy.goto("/");
        cy.get("#public_key").click();

    });
    it("Public key is shown", function () {
        cy.wait(50)
        cy.get("#public_key_body").should("to.have.text", public_key.replace(/\n|\r/g, ""))

    });
    it("Public key can be downloaded", function() {
        cy.get("#download").click();
        cy.get('a[download]')
      .then((anchor) => (
        new Cypress.Promise((resolve, reject) => {
          // Use XHR to get the blob that corresponds to the object URL.
          const xhr = new XMLHttpRequest();
          xhr.open('GET', anchor.prop('href'), true);
          xhr.responseType = 'blob';

          // Once loaded, use FileReader to get the string back from the blob.
          xhr.onload = () => {
            if (xhr.status === 200) {
              const blob = xhr.response;
              const reader = new FileReader();
              reader.onload = () => {
                // Once we have a string, resolve the promise to let
                // the Cypress chain continue, e.g. to assert on the result.
                resolve(reader.result);
              };
              reader.readAsText(blob);
            }
          };
          xhr.send();
        })
      ))// Now the regular Cypress assertions should work.
      .should('equal', public_key.replace(/\r/g, ""));
    })


});