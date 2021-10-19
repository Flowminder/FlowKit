/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import { QRCode } from "react-qr-svg";
import ErrorDialog from "./ErrorDialog";
import { confirmTwoFactor } from "./util/api";
import Button from "@material-ui/core/Button";
import PropTypes from "prop-types";
import CircularProgress from "@material-ui/core/CircularProgress";
import LockIcon from "@material-ui/icons/Lock";
import LockOpenIcon from "@material-ui/icons/LockOpen";
import { withStyles } from "@material-ui/core/styles";
import { Typography } from "@material-ui/core";

const styles = (theme) => ({
  button: {
    margin: theme.spacing.unit,
  },
});

class TwoFactorActivate extends React.Component {
  state = {
    hasError: false,
    pageError: false,
    errors: {},
    activating: false,
  };

  handleChange = (name) => (event) => {
    this.setState({ two_factor_code: event.target.value, pageError: false });
  };

  confirm = async () => {
    const { finish, secret, backup_codes_signature } = this.props;
    this.setState({ activating: true, pageError: false });
    const json = confirmTwoFactor(
      this.state.two_factor_code,
      secret,
      backup_codes_signature
    );
    try {
      this.setState(await json);
      if ((await json).two_factor_enabled && finish) {
        finish();
      }
    } catch (err) {
      this.setState({ errors: err });
      this.setState({ pageError: true });
    }
    this.setState({ activating: false });
  };

  render() {
    const { provisioning_url, secret, two_factor_code, cancel, classes } =
      this.props;
    if (this.state.hasError) throw this.state.error;

    const { two_factor_enabled, activating } = this.state;
    return (
      <Grid
        container
        spacing={2}
        direction="column"
        justify="center"
        alignItems="center"
      >
        <Grid item xs={8}>
          <Typography>
            Scan the code below using your authenticator app, and enter the code
            it generates in the box below to complete setup.
          </Typography>
        </Grid>
        <Grid item xs={8}>
          <QRCode
            bgColor="#FFFFFF"
            fgColor="#000000"
            level="Q"
            style={{ width: 256 }}
            value={provisioning_url}
            data-id="qr_code"
            data-secret={secret}
          />
        </Grid>

        <Grid item xs={8}>
          <TextField
            id="auth_code"
            label="Authorisation Code"
            value={two_factor_code}
            onChange={this.handleChange("two_factor_code")}
            margin="normal"
            variant="outlined"
          />
        </Grid>
        <Grid item xs={8} container justify="space-between">
          <Grid item xs={2}>
            <div className={classes.wrapper}>
              <Button
                type="submit"
                variant="contained"
                color="primary"
                className={classes.button}
                onClick={this.confirm}
                disabled={activating || two_factor_enabled}
                data-button-id="submit"
              >
                {activating && <CircularProgress size={24} />}
                {!activating && !two_factor_enabled && (
                  <LockOpenIcon color="secondary" />
                )}
                {two_factor_enabled && <LockIcon />} Activate
              </Button>
            </div>
          </Grid>
          <Grid item xs={2}>
            <Button
              data-button-id="cancel"
              type="submit"
              variant="contained"
              className={classes.button}
              onClick={cancel}
              disabled={this.two_factor_enabled}
            >
              Cancel
            </Button>
          </Grid>
        </Grid>
        <ErrorDialog
          open={this.state.pageError}
          message={this.state.errors.message}
        />
      </Grid>
    );
  }
}

TwoFactorActivate.propTypes = {
  classes: PropTypes.object.isRequired,
};
export default withStyles(styles)(TwoFactorActivate);
