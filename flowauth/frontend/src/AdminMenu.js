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
          <ListItem button onClick={() => onClick("server_admin")}>
            <ListItemText primary="Servers" />
          </ListItem>
          <ListItem
            button
            onClick={() => onClick("group_admin")}
            id="group_admin"
          >
            <ListItemText primary="Groups" />
          </ListItem>
          <ListItem button onClick={() => onClick("capability_admin")}>
            <ListItemText primary="API Routes" />
          </ListItem>
          <ListItem button onClick={() => onClick("aggregation_unit_admin")}>
            <ListItemText primary="Aggregation Units" />
          </ListItem>
        </List>
      </React.Fragment>
    );
  }
}

export default AdminMenu;
