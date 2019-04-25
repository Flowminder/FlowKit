describe("Login screen", function() {
  Cypress.Cookies.debug(true);

  beforeEach(function() {
    // Log in and navigate to user details screen
    cy.login_admin();
    cy.visit("/");
    cy.get("#user_list").click();
  });

  // it("Log in as an admin", function () {
  //     cy.get("#username").type("TEST_ADMIN");
  //     cy.get("#password").type("DUMMY_PASSWORD");
  //     cy.get("button").click();
  //     cy.getCookie("session").should("exist");
  //     cy.getCookie("X-CSRF").should("exist");
  //     //cy.get("#user_list").click();
  // });
  it("Add Username with space", function() {
    cy.get("#new").click();
    // checking with sapce in username
    cy.get("#username").type("USER ");
    //checking with less then 5 char
    cy.get("#username").type("USER");
    cy.get("#username").type("{selectall}USER_TEST");
  });
  it("Add Username with 4 character", function() {
    cy.get("#new").click();
    //checking with less then 4 char
    cy.get("#username").type("USER");
    cy.get("#username").type("{selectall}USER_TEST");
  });
  it("Add Password with less strength", function() {
    cy.get("#new").click();
    //checking with less then 4 char
    cy.get("#password").type("TEST_PASSWORD");
    cy.get("#password").type("{selectall}C>K,7|~44]44:ibK");
  });
  it("Add User", function() {
    cy.get("#new").click();
    //checking with less then 4 char
    cy.get("#username").type("USER_TEST");
    cy.get("#password").type("C>K,7|~44]44:ibK");
    cy.contains("Save").click();
  });
});
