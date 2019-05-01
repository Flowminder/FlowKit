/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import { createUser } from "./util/api";
import { generate } from "generate-password";
import Typography from "@material-ui/core/Typography";
import UserGroupsPicker from "./UserGroupsPicker";
import RefreshIcon from "@material-ui/icons/Refresh";
import IconButton from "@material-ui/core/IconButton";
import InputAdornment from "@material-ui/core/InputAdornment";
import GroupServerPermissions from "./GroupServerPermissions";
import FormControlLabel from "@material-ui/core/FormControlLabel";
import Switch from "@material-ui/core/Switch";
import LockIcon from "@material-ui/icons/Lock";
import LockOpenIcon from "@material-ui/icons/LockOpen";
import SubmitButtons from "./SubmitButtons";
import ErrorDialog from "./ErrorDialog";
import {
  getUser,
  editGroupMemberships,
  editUser,
  editGroupServers
} from "./util/api";
var zxcvbn = require("zxcvbn");

class UserAdminDetails extends React.Component {
  state = {
    name: "",
    username_helper_text: "",
    password: "",
    password_helper_text: "",
    edit_mode: false,
    groups: [],
    servers: [],
    group_id: null,
    is_admin: false,
    password_strength: null,
    pageError: false,
    errors: { message: "" }
  };
  componentDidMount() {
    getUser(this.props.item_id)
      .then(json => {
        this.setState(Object.assign(json || {}, json && { edit_mode: true }));
      })
      .catch(err => {
        if (err.code !== 404) {
          this.setState({ hasError: true, error: err });
        }
      });
  }

  generatePassword = event => {
    var pass = generate({ length: 16, numbers: true, symbols: true });
    var passStrength = zxcvbn(pass);
    this.setState({
      password: pass,
      password_strength: passStrength.score
    });
  };

  setAdmin = event => {
    this.setState({ is_admin: event.target.checked });
  };
  handleChange = name => event => {
    this.setState({
      pageError: false,
      errors: ""
    });
    var state = {
      [name]: event.target.value
    };
    if (name === "name") {
      var letters = /^[A-Za-z0-9_]+$/;
      let username = event.target.value;
      if (username.match(letters)) {
        state = Object.assign(state, {
          username_helper_text: ""
        });
      } else if (username.length == 0) {
        state = Object.assign(state, {
          username_helper_text: "Username can not be blank."
        });
      } else {
        state = Object.assign(state, {
          username_helper_text:
            "Username may only contain letters, numbers and underscores."
        });
      }
    }
    if (name === "password") {
      var passStrength = zxcvbn(event.target.value);
      console.log(passStrength.feedback.warning);
      state = Object.assign(state, {
        password_strength: passStrength.score,
        password_helper_text: passStrength.feedback.suggestions
      });
    }
    this.setState(state);
  };
  updateGroups = groups => {
    this.setState({ groups: groups });
  };
  updateServers = servers => {
    this.setState({ servers: servers });
  };
  handleSubmit = () => {
    const { item_id, onClick } = this.props;
    const {
      edit_mode,
      name,
      password,
      servers,
      groups,
      is_admin,
      username_helper_text,
      password_strength
    } = this.state;
    if (username_helper_text === "" && password_strength > 3) {
      var task;
      var uid;
      if (edit_mode) {
        task = editUser(item_id, name, password, is_admin);
      } else {
        task = createUser(name, password, is_admin);
      }
      task
        .then(json => {
          uid = json.id;
          return editGroupServers(json.group_id, servers);
        })
        .then(json => {
          return editGroupMemberships(uid, groups);
        })
        .then(json => {
          onClick();
        })
        .catch(err => {
          if (err.code === 400) {
            this.setState({ pageError: true, errors: err });
          }
        });
    }
  };

  render() {
    if (this.state.hasError) throw this.state.error;

    const { classes, onClick, item_id } = this.props;
    const {
      name,
      password,
      group_id,
      servers,
      edit_mode,
      password_strength
    } = this.state;
    return (
      <React.Fragment>
        <Grid xs={12}>
          <Typography variant="h5" component="h1">
            {(edit_mode && "Edit User") || "New User"}
          </Typography>
        </Grid>
        <Grid xs={6}>
          <TextField
            id="username"
            label="Username"
            className={classes.textField}
            required={true}
            value={name}
            onChange={this.handleChange("name")}
            margin="normal"
            InputLabelProps={{ shrink: true }}
            error={this.state.username_helper_text}
            helperText={this.state.username_helper_text}
          />
        </Grid>
        <Grid xs={6}>
          <TextField
            id="password"
            className={classes.textField}
            value={password}
            required={true}
            label="Reset Password"
            onChange={this.handleChange("password")}
            margin="normal"
            error={this.state.password_helper_text[0]}
            helperText={this.state.password_helper_text[0]}
            InputProps={{
              endAdornment: (
                <InputAdornment position="end">
                  <IconButton
                    color="inherit"
                    className={classes.button}
                    aria-label="New password"
                    onClick={this.generatePassword}
                  >
                    <RefreshIcon />
                  </IconButton>
                  {(password_strength || password_strength === 0) &&
                    ((password_strength > 3 && <LockIcon />) || (
                      <LockOpenIcon color="secondary" />
                    ))}
                </InputAdornment>
              )
            }}
          />
        </Grid>
        <Grid xs={12}>
          <Typography variant="h5" component="h1">
            Administrator Rights
          </Typography>
        </Grid>
        <Grid xs={2}>
          <FormControlLabel
            control={
              <Switch
                checked={this.state.is_admin}
                onChange={this.setAdmin}
                value="is_admin"
              />
            }
            label={
              (this.state.is_admin && "User is admin") || "User is not admin"
            }
          />
        </Grid>
        <Grid xs={12}>
          <Typography variant="h5" component="h1">
            Group Memberships
          </Typography>
        </Grid>
        <Grid xs={12}>
          <UserGroupsPicker
            user_id={item_id}
            updateGroups={this.updateGroups}
          />
        </Grid>
        <GroupServerPermissions
          group_id={group_id}
          updateServers={this.updateServers}
          servers={servers}
          classes={classes}
        />
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

export default UserAdminDetails;
