/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import SubmitButtons from "./SubmitButtons";
import ErrorDialog from "./ErrorDialog";
import {
    renameRole,
    createRole,
    getRole,
} from "./util/api";

class RoleDetails extends React.Component {
  state = {
    name: "",
    scopes: [],
    server: "",
    users: [],
    edit_mode: false,
    name_helper_text: "",
    pageError: false,
    errors: { message: "" },
  };

  async componentDidMount() {
    try {
      const role = await getRole(
        this.props.server_id,
        this.props.item_id);
      this.setState({ ...role, edit_mode: true });
    } catch (err) {
      if (err.code !== 404) {
        this.setState({ hasError: true, error: err });
      }
    }
  }

  handleChange = (name) => (event) => {
    this.setState({
      pageError: false,
      errors: "",
    });
    this.setState({
      [name]: event.target.value,
    });
    if (name === "name") {
      var letters = /^[A-Za-z0-9_]+$/;
      let rolename = event.target.value;
      if (rolename.match(letters)) {
        this.setState({ name_helper_text: "" });
      } else if (rolename.length === 0) {
        this.setState({ name_helper_text: "Role name can not be blank." });
      } else {
        this.setState({
          name_helper_text:
            "Role name may only contain letters, numbers and underscores.",
        });
      }
    }
  };

  updateScopes = (scopes) => {
    this.setState({ scopes:scopes});
  };

  updateServer = (server) => {
    this.setState({ server: server });
  };

  updateUsers = (users) => {
    this.setState({ users: users})
  }
  handleSubmit = async () => {
    const { name_helper_text, scopes, server, users, edit_mode, name } = this.state;
    const { item_id, onClick } = this.props;

    if (name_helper_text === "") {
      const role = edit_mode
        ? renameRole(item_id, name)
        : createRole(name, []);
      try {
        await editRoleUsers((await role).id, users);
        await editRoleServer((await role).id, server);
        await editRoleScope((await role).id, scopes)
        onClick();
      } catch (err) {
        if (err.code === 400) {
          this.setState({ pageError: true, errors: err });
        } else {
          this.setState({ hasError: true, error: err });
        }
      }
    }
  };
  render() {
    if (this.state.hasError) throw this.state.error;

    const { classes, onClick, item_id } = this.props;
    const { name, server } = this.state;

    return (
      <React.Fragment>
        <Grid xs={12}>
          <Typography variant="h5" component="h1">
            {(this.state.edit_mode && "Edit Role") || "New Role"}
          </Typography>
        </Grid>
        <Grid xs={12}>
          <TextField
            id="name"
            label="Name"
            className={classes.textField}
            required={true}
            value={name}
            onChange={this.handleChange("name")}
            margin="normal"
            error={this.state.name_helper_text}
            helperText={this.state.name_helper_text}
          />
        </Grid>

        <Grid xs={12}>
          <Typography variant="h5" component="h1">
            Members
          </Typography>
        </Grid>
        <Grid xs={12}>
          <ScopePicker
            group_id={item_id}
            updateMembers={this.updateScopes}
          />
        </Grid>
        <ErrorDialog
          open={this.state.pageError}
          message={this.state.errors.message}
        />
        <Grid item xs={12} />
        <SubmitButtons handleSubmit={this.handleSubmit} onClick={onClick} />
      </React.Fragment>
    );
  }
}

export default RoleDetails;
