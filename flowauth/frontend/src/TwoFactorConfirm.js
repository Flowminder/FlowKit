/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import { QRCode } from "react-qr-svg";
import ErrorDialog from "./ErrorDialog";
import {
  startTwoFactorSetup,
  getTwoFactorBackups,
  confirmTwoFactor
} from "./util/api";
import Button from "@material-ui/core/Button";
import Paper from "@material-ui/core/Paper";
import PropTypes from "prop-types";
import CircularProgress from "@material-ui/core/CircularProgress";
import { withStyles } from "@material-ui/core/styles";

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
    confirming: false
  };
  async componentDidMount() {
    try {
      const setup_json = await startTwoFactorSetup();
      this.setState(setup_json);
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

  renderBackupCode = code => {
    return (
      <Grid item xs={6}>
        <Typography
          variant="body2"
          align="center"
          className={this.props.classes.codeBlock}
        >
          <span className="trailing_dash">
            {code.slice(0, code.length / 2)}
          </span>
          {code.slice(code.length / 2, code.length)}
        </Typography>
      </Grid>
    );
  };

  handleChange = name => event => {
    this.setState({ two_factor_code: event.target.value });
  };

  advance = () =>
    this.setState({
      confirming: this.state.backupsCollected && this.state.backup_codes.length
    });

  confirm = () => {
    this.setState({ activating: true });
    confirmTwoFactor(this.state.two_factor_code).then(json =>
      this.setState(json)
    );
  };

  render() {
    const { classes } = this.props;
    if (this.state.hasError) throw this.state.error;

    const {
      provisioning_url,
      backup_codes,
      two_factor_code,
      backupsCollected,
      confirming
    } = this.state;
    return (
      <Paper>
        <Grid
          container
          spacing={16}
          direction="column"
          justify="center"
          alignItems="center"
        >
          {backup_codes.length === 0 && (
            <>
              <Grid item xs={1}>
                <CircularProgress className={classes.progress} />
              </Grid>
              <Grid item xs={5}>
                <Typography>Generating backup codes</Typography>
              </Grid>
            </>
          )}

          {backup_codes.length > 0 && confirming && (
            <>
              <Grid item xs={5}>
                <QRCode
                  bgColor="#FFFFFF"
                  fgColor="#000000"
                  level="Q"
                  style={{ width: 256 }}
                  value={provisioning_url}
                />
              </Grid>

              <Grid item xs={2}>
                <TextField
                  id="auth_code"
                  label="Authorisation Code"
                  value={two_factor_code}
                  onChange={this.handleChange("two_factor_code")}
                  margin="normal"
                  variant="outlined"
                />
              </Grid>
              <Grid item xs={1}>
                <Button
                  type="submit"
                  variant="contained"
                  color="primary"
                  onClick={this.confirm}
                >
                  Activate
                </Button>
              </Grid>
            </>
          )}

          {backup_codes.length > 0 && !confirming && (
            <>
              <Grid item xs={5} container direction="row">
                {backup_codes.map(code => this.renderBackupCode(code))}
              </Grid>
              <Grid item xs={5}>
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

              <Grid item xs={12} container justify="space-between">
                <Grid item xs={2}>
                  <Button
                    type="submit"
                    variant="contained"
                    className={classes.button}
                    onClick={this.advance}
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
                    onClick={this.cancel}
                  >
                    Cancel
                  </Button>
                </Grid>
              </Grid>
            </>
          )}
        </Grid>
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
