describe("Login", function() {
	it("Log in as a user", function() {
		cy.visit("/");
		cy.get("#username").type("TEST_USER");
		cy.get("#password").type("DUMMY_PASSWORD");
		cy.get("button").click();
		cy.contains("Aruba").should("exist");
		cy.getCookie("session").should("exist");
		cy.getCookie("X-CSRF").should("exist");
	});
});
