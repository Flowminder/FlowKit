/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import { withStyles } from "@material-ui/core/styles";
import ScopeDetails from "./ScopeDetails";
import { getUserRoles } from "./util/api";
import { Fragment, useEffect, useState } from "react";
import {
  List,
  Checkbox,
  ListSubheader,
  ListItem,
  Button,
  Typography,
} from "@material-ui/core";
import { TypeChecker } from "rsuite/esm/utils";
import { Divider } from "@material-ui/core";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2,
  },
});

function RoleString(props) {
  const { role } = props;

  return (
    <Fragment>
      <List>
        {role.scopes.map((scope) => (
          <ListItem>
            <Typography variant="body1" gutterTop>
              {scope}
            </Typography>
            <Divider />
          </ListItem>
        ))}
      </List>
    </Fragment>
  );
}

function TokenRolesPicker(props) {
  const { roles, checkAll, handleToggle, checked } = props;

  return (
    <Fragment>
      <Button onClick={checkAll}>Select all</Button>
      <List>
        <ListSubheader inset>Roles</ListSubheader>
        {roles.map((role, i) => (
          <ListItem key={role.name}>
            <Typography>{role.name}</Typography>
            <Checkbox
              onChange={handleToggle(i)}
              checked={checked.indexOf(i) !== -1}
            />
          </ListItem>
        ))}
      </List>
    </Fragment>
  );
}

export default withStyles(styles)(TokenRolesPicker);
