/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
 
import { withStyles } from "@material-ui/core/styles";
import RoleDetails from "./RoleDetails";
import Lister from "./Lister";
import {deleteRole, getServer, getServers, getRole, getRolesOnServer } from "./util/api";
import React, { useEffect, useState } from "react";
import { Typography, Grid, Paper, Button } from "@material-ui/core";
import { EditLocation } from "@material-ui/icons";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2,
  },
});


function RoleItem(props) {
  // Properties:
  // role_id

  const {role} = props
  const edit_action = () => console.debug(role.id)
  const delete_action = () => deleteRole(role.id)

  return(
    <Grid container spacing = {3}>
      <Grid item xs={8}>{role.name}</Grid>
      <Grid item xs={1}><Button onClick={edit_action}>Edit</Button></Grid>
      <Grid item xs={1}r><Button onClick={delete_action}>Delete</Button></Grid>
    </Grid>
  )
}

function ServerRoleList(props) {
  const {roles} = props

  return(
    <Grid direction='column'>
      {roles.map(
        (role) => <Paper><RoleItem role = {role}/></Paper>)
      }
    </Grid>
  )
}

function ServerHeader(props) {
  const {server} = props

  return(
    <Grid direction='column'>
      <Paper>{server.name}</Paper>
      <Paper><Button>Add New</Button></Paper>
    </Grid>
  )
}

function ServerRoleView(props) {
  const {server_id, edit_action, delete_action} = props

  const [server, setServer] = useState({})
  const [roles, setRoleList] = useState([])

  useEffect(
    () => {
      const fetch_server=(async(server_id) => {
        const server = await getServer(server_id)
        const role_list = await getRolesOnServer(server_id)
        console.debug("Got server", server)
        console.debug("Got roles", roles)
        setServer(server)
        setRoleList(role_list)
      })

      fetch_server(server_id).catch((err) => console.error(err))
    }, []
  )

  return(
    <Grid direction='column'>
      <Paper><ServerHeader server = {server} /></Paper>
      <Paper><ServerRoleList roles = {roles}/></Paper>
    </Grid>
  )
}


function RoleList(props) {

  const {editAction, deleteAction} = props

  const [server_list, setServerList] = useState([])

  // On load, get list of servers.
  useEffect(
    () => {
      const fetch_servers = (async () => {
        const servers = await getServers()
        setServerList(servers)
        console.debug(server_list)
      })

      fetch_servers().catch((err) => console.error(err))
    }
  , []);


  return (
    <React.Fragment>
      <Grid direction='column'>
      {server_list.map((server) => (
        <Paper><ServerRoleView
          server_id = {server.id}
          edit_action = {editAction}
          delete_action = {deleteAction}
        /></Paper>
      ))}
      </Grid>
    </React.Fragment>
  )
}




export default withStyles(styles)(RoleList);

