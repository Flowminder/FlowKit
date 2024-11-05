/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import { generate } from "generate-password";
import Typography from "@material-ui/core/Typography";
import UserRolesPicker from "./UserRolesPicker";
import RefreshIcon from "@material-ui/icons/Refresh";
import IconButton from "@material-ui/core/IconButton";
import InputAdornment from "@material-ui/core/InputAdornment";
import FormControlLabel from "@material-ui/core/FormControlLabel";
import Switch from "@material-ui/core/Switch";
import LockIcon from "@material-ui/icons/Lock";
import LockOpenIcon from "@material-ui/icons/LockOpen";
import SubmitButtons from "./SubmitButtons";
import ErrorDialog from "./ErrorDialog";
import { getUser, createUser, editUser } from "./util/api";
var zxcvbn = require("zxcvbn");

class UserAdminDetails extends React.Component {
  state = {
    name: "",
    username_helper_text: "",
    password: "",
    password_helper_text: "",
    require_two_factor: false,
    edit_mode: false,
    roles: [],
    is_admin: false,
    password_strength: null,
    has_two_factor: false,
    two_factor_can_be_disabled: false,
    pageError: false,
    error: { message: "" },
  };
  async componentDidMount() {
    const json = getUser(this.props.item_id);
    try {
      this.setState(await json);
      this.setState({ edit_mode: true });
      if ((await json)["has_two_factor"]) {
        this.setState({ two_factor_can_be_disabled: true });
      }
    } catch (err) {
      if (err.code !== 404) {
        this.setState({ hasError: true, error: err });
      }
    }
  }

  generatePassword = (event) => {
    var pass = generate({ length: 16, numbers: true, symbols: true });
    var passStrength = zxcvbn(pass);
    this.setState({
      password: pass,
      password_strength: passStrength.score,
    });
  };

  setTwoFactorRequired = (event) => {
    this.setState({ require_two_factor: event.target.checked });
  };

  setHasTwoFactor = (event) => {
    this.setState({ has_two_factor: event.target.checked });
  };

  setAdmin = (event) => {
    this.setState({ is_admin: event.target.checked });
  };

  handleChange = (name) => (event) => {
    this.setState({
      pageError: false,
      error: "",
    });
    this.setState({
      [name]: event.target.value,
    });
    if (name === "name") {
      var letters = /^[A-Za-z0-9_]+$/;
      let username = event.target.value;
      if (username.match(letters)) {
        this.setState({
          username_helper_text: "",
        });
      } else if (username.length === 0) {
        this.setState({
          username_helper_text: "Username can not be blank.",
        });
      } else {
        this.setState({
          username_helper_text:
            "Username may only contain letters, numbers and underscores.",
        });
      }
    }
    if (name === "password") {
      var passStrength = zxcvbn(event.target.value);
      this.setState({
        password_strength: passStrength.score,
        password_helper_text: passStrength.feedback.suggestions,
      });
    }
  };

  updateRoles = (roles) => {
    this.setState({ roles: roles });
  };

  handleSubmit = async () => {
    const { item_id, onClick } = this.props;
    const {
      edit_mode,
      name,
      password,
      roles,
      is_admin,
      username_helper_text,
      password_strength,
      require_two_factor,
      has_two_factor,
    } = this.state;
    if (
      username_helper_text === "" &&
      (password.length === 0 || password_strength > 3)
    ) {
      try {
        const user = await (edit_mode
          ? editUser(
              item_id,
              name,
              password.length > 0 ? password : undefined,
              is_admin,
              require_two_factor,
              has_two_factor,
              roles.map((r) => r.id),
            )
          : createUser(
              name,
              password,
              is_admin,
              require_two_factor,
              roles.map((r) => r.id),
            ));
        onClick();
      } catch (err) {
        if (err.code === 400) {
          this.setState({ pageError: true, error: err });
        } else {
          this.setState({ hasError: true, error: err });
        }
      }
    }
  };

  render() {
    if (this.state.hasError) throw this.state.error;

    const { classes, item_id, onClick } = this.props;
    const {
      name,
      password,
      edit_mode,
      password_strength,
      require_two_factor,
      has_two_factor,
      two_factor_can_be_disabled,
      is_admin,
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
              ),
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
                checked={is_admin}
                onChange={this.setAdmin}
                value="is_admin"
              />
            }
            label={(is_admin && "User is admin") || "User is not admin"}
          />
        </Grid>
        <Grid xs={12}>
          <Typography variant="h5" component="h1">
            Two-Factor Authentication
          </Typography>
        </Grid>
        <Grid xs={2}>
          <FormControlLabel
            control={
              <Switch
                checked={require_two_factor}
                onChange={this.setTwoFactorRequired}
                value="require_two_factor"
              />
            }
            label={
              (require_two_factor && "Two-factor authentication required") ||
              "Two-factor authentication not required"
            }
          />
        </Grid>
        <Grid xs={2}>
          <FormControlLabel
            control={
              <Switch
                checked={has_two_factor}
                onChange={this.setHasTwoFactor}
                value="has_two_factor"
                disabled={!two_factor_can_be_disabled}
              />
            }
            label={
              (has_two_factor && "Two-factor authentication enabled") ||
              "Two-factor authentication not enabled"
            }
          />
        </Grid>
        <Grid xs={12}>
          <Typography variant="h5" component="h1">
            Roles
          </Typography>
        </Grid>
        <Grid xs={12}>
          <UserRolesPicker user_id={item_id} updateRoles={this.updateRoles} />
        </Grid>
        <ErrorDialog
          open={this.state.pageError}
          message={this.state.error.message}
        />
        <Grid item xs={12} />
        <SubmitButtons handleSubmit={this.handleSubmit} onClick={onClick} />
      </React.Fragment>
    );
  }
}

export default UserAdminDetails;
