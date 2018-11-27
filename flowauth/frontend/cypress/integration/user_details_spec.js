/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Change_password", function () {
    beforeEach(function () {
        // Reset demo data
        cy.exec("pipenv run flask demodata")
        // Log in and navigate to user details screen.
        // Note: Cypress advise not doing it this way, but the alternative is to call
        // react component methods using Cypress, which I haven't succeeded in doing.
        cy.visit("/");
        cy.get("#username").type("TEST_USER");
        cy.get("#password").type("DUMMY_PASSWORD");
        cy.get("button").click();
        cy.get("#user_details").click();
    });

    it("Change user password", function () {
        // Change password
        cy.get("#oldPassword").type("DUMMY_PASSWORD");
        cy.get("#newPasswordA").type("ANOTHER_DUMMY_PASSWORD");
        cy.get("#newPasswordB").type("ANOTHER_DUMMY_PASSWORD");
        cy.contains("Save").click();
        // Check that snackbar appears and inputs are cleared
        cy.contains("Password changed").should("be.visible");
        cy.get("#oldPassword").should("have.value", "");
        cy.get("#newPasswordA").should("have.value", "");
        cy.get("#newPasswordB").should("have.value", "");
        // Check that we can log in with new password
        cy.get("#logout").click();
        cy.get("#username").type("TEST_USER");
        cy.get("#password").type("ANOTHER_DUMMY_PASSWORD");
        cy.get("button").click();
        cy.contains("Aruba").should("exist");
    });
});