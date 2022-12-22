/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import { getUsers, getRoleMembers } from "./util/api";
import Picker from "./Picker";

// Component for picking members for a role
class RoleMembersPicker extends React.Component {
  state = {
    members: [],
    all_users: [],
  };

  handleChange = (event) => {
    this.setState({ members: event.target.value });
    this.props.updateMembers(event.target.value);
  };

  componentDidMount() {
    var members;
    var all_users;
    getRoleMembers(this.props.role_id)
      .then((json) => {
        members = json;
      })
      .catch((err) => {
        if (err.code === 404) {
          members = [];
        } else {
          throw err;
        }
      })
      .then(() => {
        return getUsers();
      })
      .then((json) => {
        all_users = json;
      })
      .catch((err) => {
        if (err.code === 404) {
          all_users = [];
        } else {
          throw err;
        }
      })
      .then(() => {
        this.setState({
          members: members.map(
            (member) =>
              all_users[all_users.map((user) => user.id).indexOf(member.id)]
          ),
          all_users: all_users,
        });
      })
      .catch((err) => {
        this.setState({ hasError: true, error: err });
      });
  }

  render() {
    const { members, all_users, hasError, error } = this.state;
    return (
      <Picker
        objs={members}
        all_objs={all_users}
        hasError={hasError}
        error={error}
        handleChange={this.handleChange}
        label={"Members"}
      />
    );
  }
}

export default RoleMembersPicker;
