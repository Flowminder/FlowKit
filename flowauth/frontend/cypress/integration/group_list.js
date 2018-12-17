/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Group list screen", function() {
    Cypress.Cookies.debug(true);

    beforeEach(function() {
        // Log in and navigate to user details screen
        cy.login_admin();
        cy.visit("/");
        cy.get("#group_admin").click();
    });

    it("Add group", function() {
        // Add a new group
        cy.get("#new").click();
        cy.get("#group_name").type("DUMMY_GROUP");
        cy.contains("Save").click();
        // Check that new group appears
        cy.contains("DUMMY_GROUP").should("be.visible");
    });

    it("Delete group", function() {
        cy.get("#rm_2").click();
        // Check that new group appears
        cy.contains("Test Group").should("not.be.visible");
    });
});
