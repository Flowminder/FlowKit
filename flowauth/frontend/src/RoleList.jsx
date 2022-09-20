/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
 
import { withStyles } from "@material-ui/core/styles";
import RoleDetails from "./RoleDetails";
import Lister from "./Lister";
import {deleteRole, getServer, getServers, getRole } from "./util/api";
import React, { useEffect, useState } from "react";
import { Typography } from "@material-ui/core";
import { Button, Stack } from "rsuite";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2,
  },
});
// const RoleList = Lister(
//   RoleDetails,
//   "Roles",
//   getRoles,
//   deleteRole
// // );


function RoleItem(props) {
  // Properties:
  // role_id

  const {role_id} = props

  const [role, setRole] = useState({})

  useEffect(
    () => {
      const fetch_role = (async(role_id) => {
        const role = await getRole(role_id)
        setRole(role)
      })

      fetch_role(role_id).catch(console.error)
    }, []
  )

  return(
    <Grid>
      <Item>{role.name}</Item>
      <Item><Button>Edit</Button></Item>
      <Item><Button>Delete</Button></Item>
    </Grid>
  )
}

function ServerRoleList(props) {
  const {role_ids} = props

  return(
    <Stack>
      {role_ids.map(
        (role_id) => <Item><RoleItem role_id = {role_id}/></Item>)
      }
    </Stack>
  )
}

function ServerHeader(props) {
  const {server} = props

  return(
    <Grid>
      <Item>{server.name}</Item>
      <Item><Button>Edit</Button></Item>
    </Grid>
  )
}

function ServerRoleView(props) {
  const {server_id} = props

  const [server, setServer] = useState({})

  useEffect(
    () => {
      const fetch_server=(async(server_id) => {
        const server = await getServer(server_id)
        console.debug("Got server", server)
        setServer(server)
      })

      fetch_server(server_id).catch(console.error)
    }, []
  )
  
  return(
    <Stack>
      <Item><ServerHeader server = {server} /></Item>
      <Item><ServerRoleList role_ids = {server.roles}/></Item>
    </Stack>
  )
}


function RoleList(props) {

  const [server_list, setServerList] = useState([])

  // On load, get list of servers.
  useEffect(
    () => {
      const fetch_servers = async () => {
        const servers = await getServers()
        setServerList(servers)
        console.debug(server_list)
      }

      fetch_servers().catch(console.error)
    }
  , []);


  return (
    <React.Fragment>
      <Stack>
      {server_list.map((server) => (
        <Item><ServerRoleView server_id = {server.id}/></Item>
      ))}
      </Stack>
    </React.Fragment>
  )
}


export default withStyles(styles)(RoleList);

