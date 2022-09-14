/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("role list screen", function () {
  Cypress.Cookies.debug(true);

  beforeEach(function () {
    // Log in and navigate to user details screen
    cy.login_admin();
    cy.goto("/");
    cy.get("#role_admin").click();
  });
  it("Add blank role", function () {
    cy.get("#new").click();
    //adding blank rolename
    cy.get("#name").type(" ").clear();
    //checking validation text
    cy.get("#name-helper-text").should(
      "have.text",
      "Role name can not be blank."
    );
    cy.get("#name").type("TEST_ROLE");
    cy.contains("#name-helper-text").should("not.exist");
  });
  it("Add role name with space", function () {
    cy.get("#new").click();
    //adding groupname with space
    cy.get("#name").type("Role ");
    //checking validation text
    cy.get("#name-helper-text").should(
      "have.text",
      "Role name may only contain letters, numbers and underscores."
    );
    cy.get("#name").type(" ").clear();
    cy.get("#name").type("TEST_ROLE");
    cy.contains("#name-helper-text").should("not.exist");
  });
  it("Add duplicate role name", function () {
    cy.get("#new").click();
    //adding existing username and new password
    cy.get("#name").type("Test_Role");
    cy.contains("Save").click();
    //checking error dialogue text
    cy.get("#error-dialog-description").should(
      "have.text",
      "Role name already exists."
    );
    cy.contains("OK").click();
    cy.contains("#error-dialog").should("not.exist");
  });

  it("Add role", function () {
    // Add a new group
    const role_name = Math.random().toString(36).substring(2, 15);
    cy.contains(role_name).should("not.exist");
    cy.get("#new").click();
    cy.get("#name").type(role_name);
    cy.contains("Save").click();
    // Check that new group appears
    cy.contains(role_name).should("be.visible");
  });

  it("Delete role", function () {
    // Create the role
    const role_name =
      Math.random().toString(36).substring(2, 15) + "_TO_DELETE";
    cy.create_role(role_name).then((role) => {
      console.log("Role " + role);
      // Reload the roles page
      cy.goto("/");
      cy.get("#role_admin").click();
      cy.contains(role_name).should("be.visible");
      cy.get("#rm_" + role.id).click();
      // Check that the role is gone
      cy.contains(role_name).should("not.exist");
    });
  });

  it("Edit role", function () {
    const role_name =
      Math.random().toString(36).substring(2, 15) + "_TO_BE_EDITED";
    cy.create_role(role_name).then((role) => {
      console.log("Role " + role);

      // Reload the roles page
      cy.goto("/");
      cy.get("#role_admin").click();
      cy.contains(role_name).should("be.visible");
      cy.get("#edit_" + role.id).click();
      // Check that role is populated and window title is edit
      cy.contains("Edit Role").should("be.visible");
      cy.get("#name").should("have.value", role_name);
      cy.get("#name").type("{selectall}" + role_name + "_edited");
      cy.contains("Save").click();
      // Check that role is renamed
      cy.contains(role_name + "_edited").should("be.visible");
      cy.contains("/^" + role_name + "$/").should("not.exist");
    });
  });
});
