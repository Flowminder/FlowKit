/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import { getServerScopes, getRoleScopes } from "./util/api";
import Picker from "./Picker";

// Component for picking scopes for a role
class RoleScopePicker extends React.Component {
  state = {
    role_scopes: [],
    all_scopes: [],
  };

  handleChange = (event) => {
    this.setState({ role_scopes: event.target.value });
    this.props.updateScopes(event.target.value);
  };

  componentDidUpdate() {
    var role_scopes;
    var all_scopes;
    getRoleScopes(this.props.role_id)
      .then((json) => {
        role_scopes = json;
      })
      .catch((err) => {
        if (err.code === 404) {
          role_scopes = [];
        } else {
          throw err;
        }
      })
      .then(() => {
        return getServerScopes(this.props.server_id);
      })
      .then((json) => {
        all_scopes = json;
      })
      .catch((err) => {
        if (err.code === 404) {
          all_scopes = [];
        } else {
          throw err;
        }
      })
      .then(() => {
        this.setState({
          role_scopes: role_scopes.map(
            (member) =>
              all_scopes[all_scopes.map((scope) => scope.id).indexOf(member.id)]
          ),
          all_scopes: all_scopes,
        });
      })
      .catch((err) => {
        this.setState({ hasError: true, error: err });
      });
  }

  render() {
    const { role_scopes, all_scopes, hasError, error } = this.state;
    return (
      <Picker
        objs={role_scopes}
        all_objs={all_scopes}
        hasError={hasError}
        error={error}
        handleChange={this.handleChange}
        label={"Scopes"}
      />
    );
  }
}

export default RoleScopePicker;
