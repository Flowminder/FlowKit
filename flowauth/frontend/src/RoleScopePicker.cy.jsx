import RoleScopePicker from './RoleScopePicker'

//Need a dummy set of server scopes
//and a dummy set of existing role scopes
//



describe('<RoleScopePicker>', () => {
	it('mounts', () => {
		cy.intercept({
				method: 'GET',
				url: '/admin/servers/1/scopes', 
			},
			[
				{
					"enabled": true,
					"id": 1,
					"name": "admin0:dummy_query:dummy_nested_query"
				},
				{
					"enabled": true,
					"id": 3,
					"name": "admin0:dummy_query:inner_dummy_query"
				}
			]
		).as('getServerScopes');

		cy.intercept({
			method: 'GET',
			url:'/roles/1/scopes'
		},
		[{
			"id": 3,
			"name": "admin0:dummy_query:inner_dummy_query",
		},
		{
			"id":3,
			"name":"admin0:dummy_query:dummy_nested_query"
		}
	]
		).as('getRoleScopes');
				
		cy.mount(<RoleScopePicker
				updateScopes={()=>{}}
				server_id = {1}
				role_id = {1}
			/>);
		cy.wait("@getServerScopes")
		cy.wait("@getRoleScopes")
		cy.get('.rs-picker-toggle-value > span').should("include.text", "admin0 (All)")
	}),

	it('respects deeply nested loading differences', () => {
		cy.intercept({
				method: 'GET',
				url: '/admin/servers/1/scopes', 
			},
			[
				{
					"enabled": true,
					"id": 1,
					"name": "admin0:dummy_query:other_query"
				},
				{
					"enabled": true,
					"id": 3,
					"name": "admin0:dummy_query:dummy_query"
				}
			]
		).as('getServerScopes');

		cy.intercept({
			method: 'GET',
			url:'/roles/1/scopes'
		},
		[
			{
				"id": 3,
				"name": "admin0:dummy_query:dummy_query"
			},
		]
		).as('getRoleScopes');
				
		cy.mount(<RoleScopePicker
				updateScopes={()=>{}}
				server_id = {1}
				role_id = {1}
			/>);
		cy.get('.rs-picker-toggle-caret').click();
		cy.get('.rs-checkbox-wrapper').should('be.checked')
		cy.get('[data-layer="0"] > ul > .rs-picker-cascader-menu-has-children > .rs-check-item > .rs-checkbox-checker > label').should('have.text','admin0').find('input').should('be.checked');
		cy.get('[data-layer="1"] > ul > .rs-picker-cascader-menu-has-children > .rs-check-item > .rs-checkbox-checker > label > .rs-checkbox-wrapper').should('not.be.checked');
	})
})

