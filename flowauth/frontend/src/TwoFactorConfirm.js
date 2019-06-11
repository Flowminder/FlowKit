/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import ErrorDialog from "./ErrorDialog";
import { startTwoFactorSetup } from "./util/api";
import Paper from "@material-ui/core/Paper";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import BackupCodes from "./BackupCodes";
import TwoFactorActivate from "./TwoFactorActivate";
import { Typography } from "@material-ui/core";

const styles = theme => ({
  button: {
    margin: theme.spacing.unit
  },
  codeBlock: {
    fontFamily: "Consolas, Monaco, 'Andale Mono', 'Ubuntu Mono', monospace"
  }
});

class TwoFactorConfirm extends React.Component {
  state = {
    provisioning_url: "",
    two_factor_code: "",
    backup_codes: [],
    hasError: false,
    pageError: false,
    errors: {},
    backupsCollected: false,
    confirming: false,
    activating: false
  };
  async componentDidMount() {
    try {
      const setup_json = await startTwoFactorSetup();
      this.setState(setup_json);
    } catch (err) {
      if (err.code !== 404) {
        this.setState({ hasError: true, error: err });
      }
    }
  }

  handleChange = name => event => {
    this.setState({ two_factor_code: event.target.value });
  };

  advance = () =>
    this.setState({
      confirming: true
    });
  backstep = () => {
    const { confirming } = this.state;
    if (confirming) {
      this.setState({ confirming: false });
    }
  };

  render() {
    const { classes, finish, cancel } = this.props;
    if (this.state.hasError) throw this.state.error;

    const { provisioning_url, confirming } = this.state;
    return (
      <Paper className={classes.root}>
        <Typography variant="h5" component="h1">
          Two-factor authentication setup
        </Typography>
        <br />
        {provisioning_url !== "" && (
          <>
            {confirming && (
              <TwoFactorActivate
                cancel={this.backstep}
                finish={finish}
                provisioning_url={provisioning_url}
              />
            )}
            {!confirming && (
              <BackupCodes advance={this.advance} cancel={cancel} />
            )}{" "}
          </>
        )}
        <ErrorDialog
          open={this.state.pageError}
          message={this.state.errors.message}
        />
      </Paper>
    );
  }
}

TwoFactorConfirm.propTypes = {
  classes: PropTypes.object.isRequired
};
export default withStyles(styles)(TwoFactorConfirm);
