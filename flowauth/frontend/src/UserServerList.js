/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import List from "@material-ui/core/List";
import ListSubheader from "@material-ui/core/ListSubheader";
import ServerButton from "./ServerButton";
import { getMyServers } from "./util/api";

class UserServerList extends React.Component {
  state = {
    servers: []
  };

  componentDidMount() {
    getMyServers()
      .then(servers => {
        this.setState({ servers: servers });
      })
      .catch(err => {
        if (err.code === 401 || err.code === 405) {
          this.setState({ hasError: true, error: err });
        }
      });
  }

  render() {
    if (this.state.hasError) throw this.state.error;

    const { onClick } = this.props;

    return (
      <React.Fragment>
        <List>
          <ListSubheader inset>My Servers</ListSubheader>
          {this.state.servers.map((object, i) => (
            <ServerButton
              key={object.id}
              name={object.server_name}
              id={object.id}
              onClick={onClick}
            />
          ))}
        </List>
      </React.Fragment>
    );
  }
}

export default UserServerList;
