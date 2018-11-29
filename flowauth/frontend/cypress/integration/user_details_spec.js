/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Password change screen", function () {
    beforeEach(function () {
        // Reset demo data
        cy.resetDB();
        // Log in
        cy.login();
    });

    it("Change user password", function () {
        cy.visit("/");
        cy.get("#user_details").click();
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