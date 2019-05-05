/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Group list screen", function () {
    Cypress.Cookies.debug(true);

    beforeEach(function () {
        // Log in and navigate to user details screen
        cy.login_admin();
        cy.visit("/");
        cy.get("#group_admin").click();
    });
    it("Add blank group", function () {
        cy.get("#new").click();
        //adding blank groupname
        cy.get("#name")
            .type(" ")
            .clear();
        //checking validation text
        cy.get("#name-helper-text").should(
            "have.text",
            "Group name can not be blank."
        );
        cy.get("#name").type("TEST GROUP");
        cy.contains("#name-helper-text").should("not.exist");
    });
    it("Add group name with underscore", function () {
        cy.get("#new").click();
        //adding groupname with underscore 
        cy.get("#name")
            .type("Group_");
        //checking validation text
        cy.get("#name-helper-text").should(
            "have.text",
            "Group name may only contain letters, numbers and space."
        );
        cy.get("#name")
            .type(" ")
            .clear();
        cy.get("#name").type("TEST GROUP");
        cy.contains("#name-helper-text").should("not.exist");
    });


    it("Add group", function () {
        // Add a new group
        cy.contains("DUMMY GROUP").should('not.exist')
        cy.get("#new").click();
        cy.get("#name").type("DUMMY GROUP");
        cy.contains("Save").click();
        // Check that new group appears
        cy.contains("DUMMY GROUP").should("be.visible");
    });

    it("Delete group", function () {
        cy.contains("Test Group").should("be.visible");
        cy.get("#rm_2").click();
        // Check that the group is gone
        cy.contains("Test Group").should("not.be.visible");
    });

    it("Edit group", function () {
        cy.get("#edit_2").click();
        // Check that group is populated and window title is edit
        cy.contains("Edit Group").should("be.visible");
        cy.get("#name").should("have.value", "Test Group");
        cy.get("#name").type("{selectall}DUMMY GROUP");
        cy.contains("Save").click();
        // Check that group is renamed
        cy.contains("DUMMY GROUP").should("be.visible");
        cy.contains("Test Group").should("not.be.visible");
    });
});