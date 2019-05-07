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
        cy.get("#name").type("TEST_GROUP");
        cy.contains("#name-helper-text").should("not.exist");
    });
    it("Add group name with space", function () {
        cy.get("#new").click();
        //adding groupname with space 
        cy.get("#name")
            .type("Group ");
        //checking validation text
        cy.get("#name-helper-text").should(
            "have.text",
            "Group name may only contain letters, numbers and underscores."
        );
        cy.get("#name")
            .type(" ")
            .clear();
        cy.get("#name").type("TEST_GROUP");
        cy.contains("#name-helper-text").should("not.exist");
    });
    it("Add duplicate group name", function () {
        cy.get("#new").click();
        //adding existing username and new password
        cy.get("#name")
            .type("Test_Group");
        cy.contains("Save").click();
        //checking error dialogue text
        cy.get("#error-dialog-description").should(
            "have.text",
            "Group name already exists."
        );
        cy.contains("OK").click();
        cy.get("#name")
            .type(" ")
            .clear();
        cy.get("#name").type("Group_TEST");
        cy.contains("Save").click();
        cy.contains("#error-dialog").should("not.exist");
    });

    it("Add group", function () {
        // Add a new group
        cy.contains("DUMMY_GROUP").should('not.exist')
        cy.get("#new").click();
        cy.get("#name").type("DUMMY_GROUP");
        cy.contains("Save").click();
        // Check that new group appears
        cy.contains("DUMMY_GROUP").should("be.visible");
    });

    it("Delete group", function () {
        cy.contains("Test_Group").should("be.visible");
        cy.get("#rm_2").click();
        // Check that the group is gone
        cy.contains("DUMMY_GROUP").should("not.be.visible");
    });

    it("Edit group", function () {
        cy.get("#edit_2").click();
        // Check that group is populated and window title is edit
        cy.contains("Edit Group").should("be.visible");
        cy.get("#name").should("have.value", "Test_Group");
        cy.get("#name").type("{selectall}DUMMY_GROUP");
        cy.contains("Save").click();
        // Check that group is renamed
        cy.contains("DUMMY_GROUP").should("be.visible");
        cy.contains("Test_Group").should("not.be.visible");
    });
});