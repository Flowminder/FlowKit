/* eslint-disable no-undef */
/* eslint-disable react/react-in-jsx-scope */
import RoleScopePicker from "./RoleScopePicker"

//Need a dummy set of server scopes
//and a dummy set of existing role scopes
//


describe("<RoleScopePicker>", () => {
  it("mounts", () => {
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
				
    cy.mount(<RoleScopePicker
      updateScopes={()=>{}}
      server_id = {1}
      role_id = {1}
    />);
    cy.wait("@getServerScopes")
    cy.wait("@getRoleScopes")
    cy.get(".rs-picker-toggle-value > span").should("include.text", "admin0 (All), get_results")
  })

  // it("respects deeply nested loading differences", () => {
  //   cy.intercept({
  //     method: "GET",
  //     url: "/admin/servers/1/scopes", 
  //   },
  //   [
  //     {
  //       "enabled": true,
  //       "id": 1,
  //       "name": "admin0:dummy_query:other_query:silly_query"
  //     },
  //     {
  //       "enabled": true,
  //       "id": 3,
  //       "name": "admin0:dummy_query:dummy_query"
  //     }
  //   ]
  //   ).as("getServerScopes");

  //   cy.intercept({
  //     method: "GET",
  //     url:"/roles/1/scopes"
  //   },
  //   [{
  //     "id": 3,
  //     "name": "admin0:dummy_query:dummy_query",
  //   },
  //   ]
  //   ).as("getRoleScopes");
				
  //   cy.mount(<RoleScopePicker
  //     updateScopes={()=>{}}
  //     server_id = {1}
  //     role_id = {1}
  //   />);
  //   cy.wait("@getServerScopes")
  //   cy.wait("@getRoleScopes")
  //   cy.get(".rs-picker-toggle-value > span").should("include.text", "admin0:dummy_query:dummy_query")
  // })
}
)

