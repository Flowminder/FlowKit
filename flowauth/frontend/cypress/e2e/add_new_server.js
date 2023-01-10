/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Server management", function () {
  Cypress.Cookies.debug(true);

  beforeEach(function () {
    // Log in and navigate to user details screen
    cy.login_admin().goto("/").get("#server_list").click();
  });

  it("Add blank maximum lifetime minutes", function () {
    cy.get("#new").click();
    //Add blank maximum lifetime minutes
    cy.get("#max-life").type(" ", { force: true }).clear({ force: true });
    cy.get("#max-life-helper-text").should(
      "have.text",
      "Maximum lifetime minutes can not be blank."
    );
    cy.get("#max-life").type("1234", { force: true });
    cy.get("#max-life-helper-text").should("not.exist");
  });
  it("Add duplicate Server name", function () {
    cy.get("#new").click();
    cy.get("#spec-upload-button").uploadFile("api_spec.json");
    cy.get("#max-life").type("1234", {
      force: true,
    });
    cy.contains("Save").click();
    //checking error dialogue text
    cy.get("#error-dialog-description").should(
      "have.text",
      "Server with this name already exists."
    );
    cy.contains("OK").click();
    cy.get("#error-dialog-description").should("not.exist");
  });
  it("Add server", function () {
    cy.get("#new").click();
    const server_name = Math.random().toString(36).substring(2, 15);
    cy.get("#spec-upload-button").then((subject) => {
      cy.fixture("api_spec.json").then((content) => {
        const el = subject[0];
        content["components"]["securitySchemes"]["token"]["x-audience"] =
          server_name;
        const testFile = new File([JSON.stringify(content)], "api_spec.json");
        const dataTransfer = new DataTransfer();

        dataTransfer.items.add(testFile);
        el.files = dataTransfer.files;
        cy.wrap(subject).trigger("change", { force: true });
      });
    });
    cy.get("#max-life").type("1234", {
      force: true,
    });
    cy.contains("Save").click().wait(1);
    /* Edit the server */
    cy.get("[data-action=edit][data-item-name=" + server_name + "]").click();
    cy.get(".rs-picker-toggle-value").click().wait(1);
    cy.get(
      ":nth-child(1) > .rs-checkbox > .rs-checkbox-checker > label > .rs-checkbox-wrapper"
    ).click({ force: true });
    cy.contains("Save").click({ force: true }).wait(1);
    /* Check the edit happened */
    cy.get("[data-action=edit][data-item-name=" + server_name + "]")
      .click()
      .wait(1);
    cy.get(".rs-picker-toggle-value").should("have.text", "run (All)");
    cy.contains("Save").click({ force: true }).wait(1);
    cy.get("[data-action=edit][data-item-name=" + server_name + "]")
      .click()
      .wait(1);
    /* Supply an updated spec */
    cy.get("#spec-upload-button")
      .then((subject) => {
        cy.fixture("api_spec.json").then((content) => {
          const el = subject[0];
          content["components"]["securitySchemes"]["token"]["x-audience"] =
            server_name;
          content["components"]["securitySchemes"]["token"][
            "x-security-scopes"
          ] = ["get_result&test_scope"];
          const testFile = new File([JSON.stringify(content)], "api_spec.json");
          const dataTransfer = new DataTransfer();

          dataTransfer.items.add(testFile);
          el.files = dataTransfer.files;
          cy.wrap(subject).trigger("change", { force: true });
        });
      })
      .wait(1);
    cy.get(".rs-picker-toggle-value").should("have.text", "get_result (All)");
    cy.contains("Save").click({ force: true }).wait(1);
    cy.get("[data-action=edit][data-item-name=" + server_name + "]")
      .click()
      .wait(1);
    cy.get(".rs-picker-toggle-value").should("have.text", "get_result (All)");
    cy.contains("Save").click({ force: true }).wait(1);
    /* Delete it again */
    cy.get("[data-action=rm][data-item-name=" + server_name + "]").click();
    cy.get("[data-action=rm][data-item-name=" + server_name + "]").should(
      "not.exist"
    );
  });
});
