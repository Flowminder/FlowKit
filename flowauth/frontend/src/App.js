import React, { Component } from "react";
import Login from "./Login";
import Dashboard from "./Dashboard";
import { logout } from "./util/api";

class App extends Component {
  constructor(props) {
    super(props);

    this.login = this.login.bind(this);
    this.logout = this.logout.bind(this);
    this.state = {
      loggedIn: false,
      is_admin: false
    };
  }
  login(is_admin) {
    this.setState({
      loggedIn: true,
      is_admin: is_admin
    });
  }
  componentDidCatch(error, info) {
    console.log(error);
    this.setState({
      loggedIn: false,
      is_admin: false
    });
  }
  async logout() {
    logout();
    this.setState({
      loggedIn: false,
      is_admin: false
    });
  }
  render() {
    if (this.state.hasError) throw this.state.error;

    const { loggedIn, is_admin } = this.state;
    if (loggedIn) {
      return <Dashboard logout={this.logout} is_admin={is_admin} />;
    } else {
      return <Login login={this.login} />;
    }
  }
}

export default App;
