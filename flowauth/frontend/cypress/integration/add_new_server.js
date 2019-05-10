/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Login screen", function () {
    Cypress.Cookies.debug(true);

    beforeEach(function () {
        // Log in and navigate to user details screen
        cy.login_admin();
        cy.visit("/");
        cy.get("#server_list").click();
    });
    it("Add server name with space", function () {
        cy.get("#new").click();
        // adding username with space
        cy.get("#name").type("Server ");
        //checking validation text
        cy.get("#name-helper-text").should(
            "have.text",
            "Server name may only contain letters, numbers and underscores."
        );
        cy.get("#name")
            .type(" ")
            .clear();
        cy.get("#name").type("SERVER_TEST01");
        cy.contains("#name-helper-text").should("not.exist");
    });
    it("Add blank server name", function () {
        cy.get("#new").click();
        //adding blank username
        cy.get("#name")
            .type(" ")
            .clear();
        //checking validation text
        cy.get("#name-helper-text").should(
            "have.text",
            "Server name can not be blank."
        );
        cy.get("#name").type("SERVER_TEST01");
        cy.contains("#name-helper-text").should("not.exist");
    });
    it("Add blank secret key", function () {
        cy.get("#new").click();
        //Add blank secret key
        cy.get("#secret-key")
            .type(" ")
            .clear();
        cy.get("#secret-key-helper-text").should(
            "have.text",
            "Secret key can not be blank."
        );
        cy.get("#secret-key").type("C>K,7|~44]44:ibK");
        cy.get("#secret-key-helper-text").should("not.exist");
    });
    it("Add secret key with space", function () {
        cy.get("#new").click();
        //Add secret key with space
        cy.get("#secret-key")
            .type("C>K,7 |~44]44:ibK");
        cy.get("#secret-key-helper-text").should(
            "have.text",
            "Secret key can not contain space."
        );
        cy.get("#secret-key")
            .type(" ")
            .clear();
        cy.get("#secret-key").type("C>K,7|~44]44:ibK");
        cy.get("#secret-key-helper-text").should("not.exist");
    });

    it("Add blank maximum liftime minutes", function () {
        cy.get("#new").click();
        //Add blank maximum lifetime minutes
        cy.get("#max-life")
            .type(" ")
            .clear();
        cy.get("#max-life-helper-text").should(
            "have.text",
            "Maximum lifetime minutes can not be blank."
        );
        cy.get("#max-life").type("1234");
        cy.get("#max-life-helper-text").should("not.exist");
    });
    it("Add duplicate Server name", function () {
        cy.get("#new").click();
        //adding existing server name and new secret key
        cy.get("#name").type("TEST_SERVER");
        cy.get("#secret-key").type("C>K,7|~44]44:ibK");
        cy.contains("Save").click();
        //checking error dialogue text
        cy.get("#error-dialog-description").should(
            "have.text",
            "Server with this name already exists."
        );
        cy.contains("OK").click();
        cy.get("#name")
            .type(" ")
            .clear();
        cy.get("#name").type("TEST_SERVER2");
        cy.contains("Save").click();
        cy.contains("#error-dialog").should("not.exist");
        cy.contains("TEST_SERVER2").should("be.visible");
    });
    it("Add server", function () {
        cy.get("#new").click();
        //Add new user with password
        cy.get("#name").type("Server_Test01");
        cy.get("#secret-key").type("C>K,7|~44]44:ibK");
        cy.get("#max-life").type("1234");
        cy.contains("Save").click();
        cy.contains("Server_Test01").should("be.visible");
    });

});