/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Grid from "@material-ui/core/Grid";
import { getTwoFactorBackups } from "./util/api";
import Button from "@material-ui/core/Button";
import ErrorDialog from "./ErrorDialog";
import PropTypes from "prop-types";
import BackupCodesBlock from "./BackupCodesBlock";
import { withStyles } from "@material-ui/core/styles";
import CircularProgress from "@material-ui/core/CircularProgress";
import Typography from "@material-ui/core/Typography";

const styles = theme => ({
  button: {
    margin: theme.spacing.unit
  },
  codeBlock: {
    fontFamily: "Consolas, Monaco, 'Andale Mono', 'Ubuntu Mono', monospace"
  }
});

class BackupCodes extends React.Component {
  state = {
    backup_codes: [],
    hasError: false,
    pageError: false,
    errors: {},
    backupsCollected: false
  };
  async componentDidMount() {
    try {
      this.setState({ backup_codes: await getTwoFactorBackups() });
    } catch (err) {
      if (err.code !== 404) {
        this.setState({ hasError: true, error: err });
      }
    }
  }

  copyToClipboard = event => {
    var textField = document.createElement("textarea");
    textField.style.whiteSpace = "pre-wrap";
    textField.value = this.state.backup_codes.join("\n");
    document.body.appendChild(textField);
    textField.select();
    document.execCommand("copy");
    textField.remove();
    this.setState({ backupsCollected: true });
  };
  downloadTxtFile = () => {
    const element = document.createElement("a");
    const file = new Blob([this.state.backup_codes.join("\n")], {
      type: "text/plain"
    });
    element.href = URL.createObjectURL(file);
    element.download = "two_factor_backups.txt";
    document.body.appendChild(element);
    element.click();
    this.setState({ backupsCollected: true });
  };

  render() {
    const { classes, cancel, advance } = this.props;
    if (this.state.hasError) throw this.state.error;

    const { backup_codes, backupsCollected } = this.state;
    return (
      <Grid
        container
        spacing={16}
        direction="column"
        justify="center"
        alignItems="center"
      >
        {(backup_codes.length === 0 && (
          <>
            <Grid item xs={1}>
              <CircularProgress className={classes.progress} />
            </Grid>
            <Grid item xs={5}>
              <Typography> Generating backup codes </Typography>
            </Grid>
          </>
        )) || (
          <>
            <Grid item xs={5}>
              <Typography>Backup codes</Typography>
            </Grid>
            <Grid item xs={12}>
              <Typography>
                Make sure to note these down - each code will grant you _one_
                access to your account if your authenticator device is not
                available.
              </Typography>
            </Grid>
            <BackupCodesBlock backup_codes={backup_codes} />

            <Grid item xs={12}>
              <Button
                type="submit"
                variant="contained"
                className={classes.button}
                onClick={this.downloadTxtFile}
              >
                Download
              </Button>

              <Button
                type="submit"
                variant="contained"
                className={classes.button}
                onClick={this.copyToClipboard}
              >
                Copy
              </Button>
            </Grid>

            <Grid item xs={8} container justify="space-between">
              <Grid item xs={2}>
                <Button
                  type="submit"
                  variant="contained"
                  className={classes.button}
                  onClick={advance}
                  disabled={!backupsCollected && backup_codes.length > 0}
                >
                  Next
                </Button>
              </Grid>
              <Grid item xs={2}>
                <Button
                  type="submit"
                  variant="contained"
                  className={classes.button}
                  onClick={cancel}
                >
                  Cancel
                </Button>
              </Grid>
            </Grid>
          </>
        )}
        <ErrorDialog
          open={this.state.pageError}
          message={this.state.errors.message}
        />
      </Grid>
    );
  }
}

BackupCodes.propTypes = {
  classes: PropTypes.object.isRequired
};
export default withStyles(styles)(BackupCodes);
