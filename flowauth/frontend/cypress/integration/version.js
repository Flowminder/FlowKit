/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

describe("Login screen", function () {
  beforeEach(function () {
    // Go to login screen
    cy.goto("/");
  });

  it("Should show the version on the login screen", function () {
    cy.exec(
      'git describe --tags --dirty --always | sed s/"-"/"+"/ | sed s/"-"/"."/g'
    ).then((result) => {
      cy.get("#flowauth_version").should(
        "contain",
        "FlowAuth v" + result.stdout
      );
    });
  });

  it("Should show the version after logging in", function () {
    cy.exec(
      'git describe --tags --dirty --always | sed s/"-"/"+"/ | sed s/"-"/"."/g'
    ).then((result) => {
      cy.login_admin()
        .goto("/")
        .get("#flowauth_version")
        .should("contain", "FlowAuth v" + result.stdout);
    });
  });
});
