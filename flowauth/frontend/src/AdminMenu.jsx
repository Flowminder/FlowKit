/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import List from "@material-ui/core/List";
import ListSubheader from "@material-ui/core/ListSubheader";
import ListItem from "@material-ui/core/ListItem";
import ListItemText from "@material-ui/core/ListItemText";

class AdminMenu extends React.Component {
  render() {
    const { onClick } = this.props;

    return (
      <React.Fragment>
        <List>
          <ListSubheader inset>Admin</ListSubheader>
          <ListItem button onClick={() => onClick("user_admin")} id="user_list">
            <ListItemText primary="Users" />
          </ListItem>
          <ListItem
            button
            onClick={() => onClick("server_admin")}
            id="server_list"
          >
            <ListItemText primary="Servers" />
          </ListItem>
          <ListItem
            button
            onClick={() => onClick("role_admin")}
            id="role_admin"
          >
            <ListItemText primary="Roles" />
          </ListItem>

          <ListItem
            button
            onClick={() => onClick("public_key_admin")}
            id="public_key"
          >
            <ListItemText primary="Public Key" />
          </ListItem>
        </List>
      </React.Fragment>
    );
  }
}

export default AdminMenu;
