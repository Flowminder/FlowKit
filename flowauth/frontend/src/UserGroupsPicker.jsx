/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import { getGroups, getGroupsForUser } from "./util/api";
import Picker from "./Picker";

class UserGroupsPicker extends React.Component {
  state = {
    groups: [],
    all_groups: [],
  };

  handleChange = (event) => {
    this.setState({ groups: event.target.value });
    this.props.updateGroups(event.target.value);
  };

  getData = () => {
    var all_groups;
    getGroups()
      .then((json) => {
        all_groups = json;
        return getGroupsForUser(this.props.user_id);
      })
      .then((json) => {
        this.setState({
          groups: json.map(
            (member) =>
              all_groups[all_groups.map((user) => user.id).indexOf(member.id)],
          ),
          all_groups: all_groups,
        });
      })
      .catch((err) => {
        if (err.code === 404) {
          this.setState({
            groups: [],
            all_groups: all_groups,
          });
        } else {
          this.setState({ hasError: true, error: err });
        }
      });
  };

  componentDidMount() {
    this.getData();
  }

  componentDidUpdate(prevProps) {
    const { user_id } = this.props;
    if (user_id !== prevProps.user_id) {
      this.getData();
    }
  }

  render() {
    const { groups, all_groups, hasError, error } = this.state;
    return (
      <Picker
        objs={groups}
        all_objs={all_groups}
        hasError={hasError}
        error={error}
        handleChange={this.handleChange}
        label={"Groups"}
      />
    );
  }
}

export default UserGroupsPicker;
