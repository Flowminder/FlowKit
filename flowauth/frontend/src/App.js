import React, { Component } from "react";
import Login from "./Login";
import Dashboard from "./Dashboard";
import { isLoggedIn, logout } from "./util/api";

class App extends Component {
  constructor(props) {
    super(props);
    this.state = {
      loggedIn: false,
      is_admin: false
    };
  }
  setLoggedIn = (is_admin) => {
    this.setState({
      loggedIn: true,
      is_admin: is_admin
    });
  }
  componentDidCatch(error, info) {
    console.log(error);
    logout();
    this.setState({
      loggedIn: false,
      is_admin: false
    });
  }
  logout = async () => {
    logout();
    this.setState({
      loggedIn: false,
      is_admin: false
    });
  }
  componentDidMount() {
    isLoggedIn().then(json => {
      this.setState({
        loggedIn: json.logged_in,
        is_admin: json.is_admin
      });
    });
  }
  render() {
    if (this.state.hasError) throw this.state.error;

    const { loggedIn, is_admin } = this.state;
    if (loggedIn) {
      return <Dashboard logout={this.logout} is_admin={is_admin} />;
    } else {
      return <Login setLoggedIn={this.setLoggedIn} />;
    }
  }
}

export default App;
