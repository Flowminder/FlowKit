/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Button from "@material-ui/core/Button";
import Paper from "@material-ui/core/Paper";
import Grid from "@material-ui/core/Grid";
import FormControl from "@material-ui/core/FormControl";
import Input from "@material-ui/core/Input";
import InputLabel from "@material-ui/core/InputLabel";
import PropTypes from "prop-types";
import LockIcon from "@material-ui/icons/Lock";
import LockOpenIcon from "@material-ui/icons/LockOpen";
import { withStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import InputAdornment from "@material-ui/core/InputAdornment";
import {
  editPassword,
  isTwoFactorActive,
  isTwoFactorRequired,
  disableTwoFactor
} from "./util/api";
import ErrorDialog from "./ErrorDialog";
import MessageSnackbar from "./MessageSnackbar";
import TwoFactorConfirm from "./TwoFactorConfirm";
import GenerateBackupCodes from "./GenerateBackupCodes";
var zxcvbn = require("zxcvbn");

const styles = theme => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  },
  button: {
    margin: theme.spacing.unit
  }
});

class UserDetails extends React.Component {
  state = {
    oldPassword: "",
    newPasswordA: "",
    newPasswordB: "",
    password_strength: null,
    passwordChanged: false,
    two_factor_enabled: false,
    require_two_factor: false,
    two_factor_setup: false,
    hasError: false,
    error: { message: "" }
  };

  handleSubmit = async event => {
    event.preventDefault();
    if (this.state.newPasswordA === this.state.newPasswordB) {
      editPassword(this.state.oldPassword, this.state.newPasswordA)
        .then(() => {
          this.setState({
            passwordChanged: true,
            oldPassword: "",
            newPasswordA: "",
            newPasswordB: "",
            password_strength: null,
            hasError: false,
            error: { message: "" }
          });
        })
        .catch(err => {
          this.setState({ passwordChanged: false, hasError: true, error: err });
        });
    } else {
      this.setState({
        passwordChanged: false,
        hasError: true,
        error: { message: "Passwords do not match." }
      });
    }
  };

  handleTextChange = name => event => {
    var passStrength = zxcvbn(event.target.value);
    var state = {
      [name]: event.target.value,
      passwordChanged: false,
      hasError: false,
      error: { message: "" }
    };
    if (name === "newPasswordA") {
      state = Object.assign(state, {
        password_strength: passStrength.score
      });
    }
    this.setState(state);
  };
  editTwoFactor = () => this.setState({ two_factor_setup: true });
  newBackups = () => this.setState({ new_backups: true });
  finishNewBackups = () => this.setState({ new_backups: false });
  finishEditTwoFactor = () =>
    this.setState({ two_factor_setup: false, two_factor_enabled: true });
  cancelEditTwoFactor = () => this.setState({ two_factor_setup: false });
  disableTwoFactor = async () => this.setState(await disableTwoFactor());

  async componentDidMount() {
    this.setState(await isTwoFactorActive());
    this.setState(await isTwoFactorRequired());
  }

  render() {
    const { classes } = this.props;
    const {
      oldPassword,
      newPasswordA,
      newPasswordB,
      password_strength,
      require_two_factor,
      new_backups,
      two_factor_enabled,
      two_factor_setup
    } = this.state;

    if (!two_factor_setup && !new_backups) {
      return (
        <Paper className={classes.root}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12}>
              <Typography variant="h5" component="h1">
                Reset password
              </Typography>
            </Grid>
            <Grid item xs={12}>
              <form className={classes.form} onSubmit={this.handleSubmit}>
                <Grid item xs={3}>
                  <FormControl margin="normal" required fullWidth>
                    <InputLabel htmlFor="oldPassword">Old Password</InputLabel>
                    <Input
                      id="oldPassword"
                      name="oldPassword"
                      type="password"
                      value={oldPassword}
                      onChange={this.handleTextChange("oldPassword")}
                    />
                  </FormControl>
                </Grid>
                <Grid item xs={9} />
                <Grid item xs={3}>
                  <FormControl margin="normal" required fullWidth>
                    <InputLabel htmlFor="newPasswordA">New Password</InputLabel>
                    <Input
                      id="newPasswordA"
                      name="newPasswordA"
                      type="password"
                      value={newPasswordA}
                      onChange={this.handleTextChange("newPasswordA")}
                      endAdornment={
                        <InputAdornment position="end">
                          {(password_strength || password_strength === 0) &&
                            ((password_strength > 3 && <LockIcon />) || (
                              <LockOpenIcon color="secondary" />
                            ))}
                        </InputAdornment>
                      }
                    />
                  </FormControl>
                </Grid>
                <Grid item xs={9} />
                <Grid item xs={3}>
                  <FormControl
                    margin="normal"
                    required
                    fullWidth
                    error={newPasswordA !== newPasswordB}
                  >
                    <InputLabel htmlFor="newPasswordB">New Password</InputLabel>
                    <Input
                      id="newPasswordB"
                      name="newPasswordB"
                      type="password"
                      value={newPasswordB}
                      onChange={this.handleTextChange("newPasswordB")}
                    />
                  </FormControl>
                </Grid>
                <Grid item xs={9} />
                <Grid item xs={12} container direction="row-reverse">
                  <Grid item>
                    <Button
                      type="submit"
                      fullWidth
                      variant="contained"
                      color="primary"
                      className={classes.submit}
                    >
                      Save
                    </Button>
                  </Grid>
                </Grid>
              </form>
            </Grid>
            <Grid item xs={12}>
              <Typography variant="h5" component="h1">
                Two-factor authentication
              </Typography>
            </Grid>
            <Grid item xs={12}>
              <Button
                type="submit"
                variant="contained"
                color="primary"
                className={classes.button}
                onClick={this.editTwoFactor}
                id={
                  two_factor_enabled ? "reset_two_factor" : "enable_two_factor"
                }
              >
                {two_factor_enabled ? "Reset" : "Enable"}
              </Button>
              <Button
                disabled={!two_factor_enabled}
                type="submit"
                variant="contained"
                color="primary"
                className={classes.button}
                onClick={this.newBackups}
              >
                Regenerate backup codes
              </Button>
              <Button
                disabled={require_two_factor || !two_factor_enabled}
                type="submit"
                variant="contained"
                color="primary"
                className={classes.button}
                onClick={this.disableTwoFactor}
              >
                Disable
              </Button>
            </Grid>
          </Grid>
          <ErrorDialog
            open={this.state.hasError}
            message={this.state.error.message}
          />
          <MessageSnackbar
            open={this.state.passwordChanged}
            variant="success"
            message="Password changed"
          />
        </Paper>
      );
    } else if (!new_backups) {
      return (
        <TwoFactorConfirm
          classes={classes}
          finish={this.finishEditTwoFactor}
          cancel={this.cancelEditTwoFactor}
        />
      );
    } else {
      return (
        <GenerateBackupCodes
          finish={this.finishNewBackups}
          cancel={this.finishNewBackups}
        />
      );
    }
  }
}
UserDetails.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(UserDetails);
