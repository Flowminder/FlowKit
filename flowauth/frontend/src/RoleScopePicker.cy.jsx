/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/* eslint-disable quotes */
/* eslint-disable no-undef */
/* eslint-disable react/react-in-jsx-scope */
import RoleScopePicker from "./RoleScopePicker"

//Need a dummy set of server scopes
//and a dummy set of existing role scopes
//


describe("<RoleScopePicker>", () => {
  beforeEach(() => {
    cy.intercept({
      method: "GET",
      url: "/admin/servers/1/scopes", 
    },
    [
      {
        "enabled": true,
        "id": 1,
        "name": "admin0:dummy_query:dummy_nested_query:silly_query"
      },
      {
        "enabled": true,
        "id": 3,
        "name": "admin0:dummy_query:inner_dummy_query"
      },
      {
        "enabled": true,
        "id": 6,
        "name": "admin0:other_query:inner_dummy_query"
      },
      {
        "enabled": true,
        "id": 4,
        "name": "admin1:dummy_query:inner_dummy_query"
      },
      {
        "enabled": true,
        "id": 7,
        "name": "get_results"
      }
    ]
    ).as("getServerScopes");

    cy.intercept({
      method: "GET",
      url:"/roles/1/scopes"
    },
    [{
      "id": 3,
      "name": "admin0:dummy_query:inner_dummy_query",
    },
    {
      "id":1,
      "name":"admin0:dummy_query:dummy_nested_query"
    },
    {
      "id":7,
      "name":"get_results"
    }
    ]
    ).as("getRoleScopes");
  })
	
  it("mounts", () => {
    cy.mount(<RoleScopePicker
      updateScopes={()=>{}}
      server_id = {1}
      role_id = {1}
    />);
    cy.wait("@getServerScopes")
    cy.wait("@getRoleScopes")
  })

  it("expands", () => {
    cy.mount(<RoleScopePicker
      updateScopes={()=>{}}
      server_id = {1}
      role_id = {1}
    />);
    cy.get("[data-cy=nested-admin0]").within(() => {
      cy.get("[data-cy=chevron]")
        .click()
    }).contains("dummy_query")
  })

  it("cascades correctly", () => {
    cy.mount(<RoleScopePicker
      updateScopes={()=>{}}
      server_id = {1}
      role_id = {1}
    />);
    // Check admin0 starts indeterminant
    cy.get("[data-cy=nested-admin0")
      .find("[data-cy=checkbox]").first()
      .invoke("prop", "indeterminate", true)

    // Expand child scopes and check click on checkbox works
    cy.get("[data-cy=nested-admin0]").within(() => {
      cy.get("[data-cy=chevron]")
        .click()
      cy.get("[data-cy=nested-dummy_query]")
        .find("[data-cy=checkbox]").first()
        .invoke("prop", "checked", true)
        .click()
        .invoke("prop", "checked", false)
    })

    // Check both child scopes of admin0 change on click
    cy.get("[data-cy=nested-other_query]")
      .find("[data-cy=checkbox]").first()
      .click()

    //Check both children being true cause admin0 to become true
    cy.get("[data-cy=nested-admin0")
      .find("[data-cy=checkbox]").first()
      .invoke("prop", "checked", true)

    //Click on admin0's checkbox
    cy.get("[data-cy=nested-admin0")
      .find("[data-cy=checkbox]").first()
      .click()

    // Check both children are now false
    cy.get("[data-cy=nested-admin0]")
      .find("[data-cy=nested-dummy_query]")
      .find("[data-cy=checkbox]").first()
      .invoke("prop", "checked", false)
    cy.get("[data-cy=nested-admin0]")
      .find("[data-cy=nested-other_query]")
      .find("[data-cy=checkbox]").first()
      .invoke("prop", "checked", false)
  })

})