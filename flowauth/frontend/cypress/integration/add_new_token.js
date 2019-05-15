/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Login screen", function () {
    Cypress.Cookies.debug(true);

    beforeEach(function () {
        // Log in and navigate to user details screen
        cy.login();
        cy.visit("/");
        cy.get("#servers").click();
    });
    it("Add server name with space", function () {
        cy.get("#new").click();
        // adding username with space
        cy.get("#tokenname").type("Token ");
        //checking validation text
        cy.get("#name-helper-text").should(
            "have.text",
            "Token name may only contain letters, numbers and underscores."
        );
        // cy.get("#tokenname")
        //     .type(" ")
        //     .clear();
        cy.get("#tokenname").type("TOKEN_TEST01");
        cy.contains("#name-helper-text").should("not.exist");
    });
    it("Add blank token name", function () {
        cy.get("#new").click();
        //adding blank token name
        cy.get("#tokenname")
            .type(" ")
            .clear();
        //checking validation text
        cy.get("#name-helper-text").should(
            "have.text",
            "Token name can not be blank."
        );
        cy.get("#tokenname").type("TOKEN_TEST01");
        cy.contains("#name-helper-text").should("not.exist");
    });
});