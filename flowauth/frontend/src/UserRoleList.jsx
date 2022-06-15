/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
 
import { withStyles } from "@material-ui/core/styles";
import RoleDetails from "./RoleDetails";
import { getUserRoles} from "./util/api";
import { Fragment, useEffect, useState } from "react";
import {List, Checkbox, ListSubheader, ListItem } from "@material-ui/core";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2,
  },
});


function UserRoleList(props){
  const {user, server} = props

  const [roles, setRoleState] = useState([])
  const [checked, setChecked] = useState([])

  const handleToggle = (value) => () => {
    const currentIndex = checked.indexOf(value);
    const newChecked = [...checked]
    console.log("box checked")
    if (currentIndex === -1) {
      newChecked.push(value)
    } else {
      newChecked.splice(currentIndex, 1)
    }
    setChecked(newChecked)
  }

  useEffect(() => {
    getUserRoles(server)
    .then((roles) => {setRoleState(roles); console.log(roles)},
     (err) => console.log(err))
  }, [])

  return (
    <Fragment>
      <List>
        <ListSubheader inset>Roles</ListSubheader>
        {roles.map((this_role, i) => (
          <ListItem key={i}>
          <RoleDetails role={this_role}/>
          <Checkbox
                onChange={handleToggle(i)}
                checked={checked.indexOf(i) !== -1}
              />
          </ListItem>
        ))}
      </List>
    </Fragment>
  )

}

export default withStyles(styles)(UserRoleList);