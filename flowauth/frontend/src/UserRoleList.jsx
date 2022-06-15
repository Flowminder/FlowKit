/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
 
import { withStyles } from "@material-ui/core/styles";
import RoleDetails from "./RoleAdmin";
import Lister from "./Lister";
import { getRoles, deleteRole } from "./util/api";
import { Fragment, useEffect, useState } from "react";
import { Checkbox, ListSubheader } from "@material-ui/core";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2,
  },
});


function UserRoleList(props){
  const {user, server} = props

  const [roles, setRoleState] = useState(new {})

  useEffect(() => {
    getRoles(server).then((roles) => setRoleState(roles))
  }, [])

  return (
    <Fragment>
      <List>
        <ListSubheader inset>Roles</ListSubheader>
        {roles.map((this_role, i) => (
          <RoleDetails
            name={this_role.name}
            scopes={this_role.scopes}
          />
        ))}
      </List>
    </Fragment>
  )

}

export default withStyles(styles)(UserRoleList);