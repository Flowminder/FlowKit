describe("Login", function () {
	it("Log in as a user", function () {
		cy.visit("/");
		cy.get("#username").type("TEST_USER");
		cy.get("#password").type("DUMMY_PASSWORD");
		cy.get("button").click();
		cy.contains("Aruba").should("exist");
		cy.getCookie("session").should("exist");
		cy.getCookie("X-CSRF").should("exist");
	});
});

describe("Login_error_username", function () {
	it("Fail to log in with incorrect username", function () {
		cy.visit("/");
		// Attempt to log in with incorrect username
		cy.get("#username").type("WRONG_USER");
		cy.get("#password").type("DUMMY_PASSWORD");
		cy.get("button").click();
		// Check that dialog appears with correct title and description
		cy.get("#error-dialog-title").should("contain", "Error");
		cy.get("#error-dialog-description").should("contain", "Incorrect username or password.");
	});
});

describe("Login_error_password", function () {
	it("Fail to log in with incorrect password", function () {
		cy.visit("/");
		// Attempt to log in with incorrect password
		cy.get("#username").type("TEST_USER");
		cy.get("#password").type("WRONG_PASSWORD");
		cy.get("button").click();
		// Check that dialog appears with correct title and description
		cy.get("#error-dialog-title").should("contain", "Error");
		cy.get("#error-dialog-description").should("contain", "Incorrect username or password.");
	});
});

describe("Login_error_twice", function () {
	it("Error dialog appears more than once", function () {
		cy.visit("/");
		// Attempt to log in with incorrect username
		cy.get("#username").type("WRONG_USER");
		cy.get("#password").type("DUMMY_PASSWORD");
		cy.get("button").click();
		// Close dialog for the first time
		cy.contains("OK").click().should("not.exist");
		// Check that dialog reappears if we try logging in again
		cy.get("button").click();
		cy.get("#error-dialog");
	});
});

describe("Login_error_typing", function () {
	it("Error dialog does not reappear when typing in input", function () {
		cy.visit("/");
		// Attempt to log in with incorrect username
		cy.get("#username").type("WRONG_USER");
		cy.get("#password").type("DUMMY_PASSWORD");
		cy.get("button").click();
		// Close dialog
		cy.contains("OK").click().should("not.exist");
		// Check that dialog doesn't reappear when we type in the username input
		cy.get("#username").type("_CHANGED");
		cy.get("#error-dialog").should("not.exist");
	});
});
