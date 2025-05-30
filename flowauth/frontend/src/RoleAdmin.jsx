/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import { withStyles } from "@material-ui/core/styles";
import RoleDetails from "./RoleDetails";
import Lister from "./Lister";
import {
  deleteRole as requestDeleteRole,
  getServer,
  getServers,
  getRole,
  getRolesOnServer,
} from "./util/api";
import React, { useCallback, useEffect, useState } from "react";
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

  const { role, server, onClick, deleteRole } = props;

  const edit_role = () => onClick(role.id, server.id);
  const delete_role = () => deleteRole(role.id);

  return (
    <Grid container spacing={3}>
      <Grid item xs={8}>
        <Typography component="h3">{role.name}</Typography>
      </Grid>
      <Grid item xs={1}>
        <Button onClick={edit_role} data-cy={`edit-${role.name}`}>
          Edit
        </Button>
      </Grid>
      <Grid item xs={1} r>
        <Button onClick={delete_role} data-cy={`delete-${role.name}`}>
          Delete
        </Button>
      </Grid>
    </Grid>
  );
}

function ServerRoleList(props) {
  const { roles, server, onClick, deleteRole } = props;

  return (
    <Grid direction="column">
      {roles.map((role) => (
        <Grid item>
          <RoleItem
            role={role}
            server={server}
            onClick={onClick}
            deleteRole={deleteRole}
          />
        </Grid>
      ))}
    </Grid>
  );
}

function ServerHeader(props) {
  const { server, onClick } = props;
  const new_role_on_server = () => onClick(-1, server.id);

  const [new_role_enabled, setNewRoleEnabled] = useState(false);

  useEffect(() => setNewRoleEnabled(typeof server !== "undefined"), [server]);

  return (
    <Grid container direction="row">
      <Grid item xs={9}>
        <Typography variant="h5" component="h1">
          {server.name}
        </Typography>
      </Grid>
      <Grid item xs={3}>
        <Button
          onClick={new_role_on_server}
          data-cy={"new"}
          enabled={new_role_enabled}
        >
          New Role
        </Button>
      </Grid>
    </Grid>
  );
}

function ServerRoleView(props) {
  const { server_id, onClick, deleteRole } = props;

  const [server, setServer] = useState({});
  const [roles, setRoleList] = useState([]);
  const [dataLoaded, setDataLoaded] = useState(false);

  const deleteRoleWithEdit = (role_id) => {
    deleteRole(role_id);
    setRoleList(roles.filter((x) => x.id !== role_id));
  };

  useEffect(() => {
    const fetch_server = async (server_id) => {
      const server = await getServer(server_id);
      const role_list = await getRolesOnServer(server_id);
      console.debug("Got server", server);
      console.debug("Got roles", roles);
      setServer(server);
      setRoleList(role_list);
    };

    fetch_server(server_id).catch((err) => console.error(err));
  }, []);

  useEffect(() => {
    if (server !== "undefined" && roles !== "undefined") {
      setDataLoaded(true);
    }
  }, [server, roles]);

  if (dataLoaded)
    return (
      <Grid container direction="column">
        <Grid item>
          <ServerHeader server={server} onClick={onClick} />
        </Grid>
        <Grid item>
          <ServerRoleList
            roles={roles}
            server={server}
            onClick={onClick}
            deleteRole={deleteRoleWithEdit}
          />
        </Grid>
      </Grid>
    );
  else return null;
}

function RoleList(props) {
  const { onClick, deleteRole } = props;
  const [server_list, setServerList] = useState([]);

  // On load, get list of servers.
  useEffect(() => {
    const fetch_servers = async () => {
      const servers = await getServers();
      setServerList(servers);
      console.debug(server_list);
    };

    fetch_servers().catch((err) => console.error(err));
  }, []);

  return (
    <React.Fragment>
      <Grid direction="column">
        {server_list.map((server) => (
          <Grid item>
            <ServerRoleView
              server_id={server.id}
              onClick={onClick}
              deleteRole={deleteRole}
            />
          </Grid>
        ))}
      </Grid>
    </React.Fragment>
  );
}

function RoleAdmin(props) {
  const [is_editing, setIsEditing] = useState(false);
  const [active_role, setActiveRole] = useState(-1);
  const [active_server, setActiveServer] = useState(-1);

  const edit_role = (this_role, this_server) => {
    setActiveRole(this_role);
    setActiveServer(this_server);
    setIsEditing(true);
  };

  const delete_role = async (this_role) => {
    await requestDeleteRole(this_role);
  };

  const return_to_list = () => {
    setActiveRole(-1);
    setActiveServer(-1);
    setIsEditing(false);
  };

  if (is_editing) {
    return (
      <RoleDetails
        role_id={active_role}
        onClick={return_to_list}
        server_id={active_server}
      />
    );
  } else {
    return <RoleList onClick={edit_role} deleteRole={delete_role} />;
  }
}

export default withStyles(styles)(RoleAdmin);
