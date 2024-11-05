/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React, { Component } from "react";
import Login from "./Login";
import Dashboard from "./Dashboard";
import { logout } from "./util/api";
import Version from "./Version";

class App extends Component {
  constructor(props) {
    super(props);
    this.state = {
      loggedIn: false,
      is_admin: false,
    };
  }
  setLoggedIn = (is_admin) => {
    this.setState({
      loggedIn: true,
      is_admin: is_admin,
    });
  };
  componentDidCatch(error, info) {
    console.log(error);
    logout().then((json) => {
      this.setLoggedOut();
    });
  }
  setLoggedOut = () => {
    this.setState({
      loggedIn: false,
      is_admin: false,
    });
  };
  render() {
    if (this.state.hasError) throw this.state.error;

    const { loggedIn, is_admin } = this.state;
    let component;
    if (loggedIn) {
      component = (
        <Dashboard setLoggedOut={this.setLoggedOut} is_admin={is_admin} />
      );
    } else {
      component = (
        <Login
          setLoggedIn={this.setLoggedIn}
          setLoggedOut={this.setLoggedOut}
        />
      );
    }
    return (
      <>
        {component}
        <Version />
      </>
    );
  }
}

export default App;
