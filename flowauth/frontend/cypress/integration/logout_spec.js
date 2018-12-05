/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Logout", function () {
    Cypress.Cookies.debug(true)

    it("Logout actually logs out", function () {
        cy.login();
        cy.visit("/");
        cy.get("#logout").click();
        cy.wait(500);
        cy.contains("Sign in");
    });
});