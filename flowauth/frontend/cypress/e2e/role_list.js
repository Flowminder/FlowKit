/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("role list screen", function () {
  Cypress.Cookies.debug(true);
  const race_timeout = 3000;

  beforeEach(function () {
    // Log in and navigate to user details screen
    cy.login_admin();
    cy.goto("/");
    cy.get("#role_admin").click();
    cy.wait(race_timeout); // This needed to mitigate race condition

    //set up aliases
    cy.get("[data-cy=new]").click().as("nav_new_role_view");
    cy.get('[data-cy="scope-item-get_available_dates"]')
      .should("exist")
      .as("wait_for_new_role_load");
  });

  it("Add blank role", function () {
    this.nav_new_role_view;
    this.wait_for_new_role_load;
    //adding blank rolename
    cy.get("#name").type(" ").clear();
    //checking validation text
    cy.get("#name-helper-text").should(
      "have.text",
      "Role name cannot be blank.",
    );
    cy.get("#name").type("TEST_ROLE");
    cy.contains("#name-helper-text").should("not.exist");
  });

  it("Add role name with space", function () {
    this.nav_new_role_view;
    this.wait_for_new_role_load;
    //adding groupname with space
    cy.get("#name").type("Role ");
    //checking validation text
    cy.get("#name-helper-text").should(
      "have.text",
      "Role name may only contain letters, numbers and underscores.",
    );
    cy.get("#name").type(" ").clear();
    cy.get("#name").type("TEST_ROLE");
    cy.contains("#name-helper-text").should("not.exist");
  });

  it("Add duplicate role name", function () {
    const role_name = Math.random().toString(36).substring(2, 15);
    cy.create_role(role_name).then((role) => {
      this.nav_new_role_view;
      this.wait_for_new_role_load;
      cy.get("#name").type(role_name);
      cy.get('[data-cy="scope-item-get_available_dates"]')
        .find("[data-cy=checkbox]")
        .first()
        .click();
      cy.contains("Save").click();
      cy.get("#error-dialog-description").contains(`Name already exists`);
    });
  });

  it("Add role", function () {
    // Add a new group
    const role_name = Math.random().toString(36).substring(2, 15);
    cy.contains(role_name).should("not.exist");
    this.nav_new_role_view;
    this.wait_for_new_role_load;

    cy.get("#name").type(role_name);
    cy.get('[data-cy="scope-item-get_available_dates"]')
      .find("[data-cy=checkbox]")
      .first()
      .click();
    cy.contains("Save").click();
    // Check that new group appears
    cy.contains(role_name).should("be.visible");
  });

  it("Role scopes are reflected", function () {
    const role_name = Math.random().toString(36).substring(2, 15);
    cy.create_role(role_name).then((role) => {
      cy.goto("/").get("#role_admin").click();
      cy.get(`[data-cy=edit-${role_name}]`).click();
      cy.get('[data-cy="nested-admin0"]').within(() => {
        cy.get("[data-cy=checkbox]").invoke("prop", "indeterminate", true);
        cy.get("[data-cy=chevron]").click();
      });
      cy.get("[data-cy=nested-consecutive_trips_od_matrix]")
        .find("[data-cy=checkbox]")
        .invoke("prop", "checked", true);
    });
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
      cy.wait(race_timeout);
      cy.contains(role_name).should("be.visible");
      cy.get(`[data-cy=delete-${role_name}]`).click();
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
      cy.goto("/").get("#role_admin").click();

      cy.contains(role_name)
        .should("be.visible")
        .get(`[data-cy=edit-${role_name}]`)
        .scrollIntoView()
        .click();
      // Check that role is populated and window title is edit
      cy.contains("Edit Role")
        .should("be.visible")
        .get("#name")
        .should("have.value", role_name);
      cy.get("#name").type("{selectall}" + role_name + "_edited");
      cy.contains("Save").click();
      // Check that role is renamed
      cy.contains(role_name + "_edited")
        .should("exist")
        .contains("/^" + role_name + "$/")
        .should("not.exist");
    });
  });
});
