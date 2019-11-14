/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import Paper from "@material-ui/core/Paper";
import Typography from "@material-ui/core/Typography";
import { getPublicKey } from "./util/api";
import Grid from "@material-ui/core/Grid";
import Tooltip from "@material-ui/core/Tooltip";
import Button from "@material-ui/core/Button";

const styles = theme => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  },
  button: {
    margin: theme.spacing.unit
  },
  gridRoot: {
    flexGrow: 1
  },
  codeBlock: {
    fontFamily: "Consolas, Monaco, 'Andale Mono', 'Ubuntu Mono', monospace"
  }
});

class PublicKey extends React.Component {
  state = { key: "", copySuccess: "" };
  async componentDidMount() {
    try {
      const key = await getPublicKey();
      this.setState({
        key: key.public_key
      });
    } catch (err) {
      this.setState({ hasError: true, error: err });
    }
  }
  copyToEncodedToClipboard = event => {
    const textField = document.createElement("textarea");
    textField.style.whiteSpace = "pre-wrap";
    textField.value = window.btoa(this.state.key);
    document.body.appendChild(textField);
    textField.select();
    document.execCommand("copy");
    textField.remove();
    this.setState({ copySuccess: "Copied!" });
  };
  copyToClipboard = event => {
    const textField = document.createElement("textarea");
    textField.style.whiteSpace = "pre-wrap";
    textField.value = this.state.key;
    document.body.appendChild(textField);
    textField.select();
    document.execCommand("copy");
    textField.remove();
    this.setState({ copySuccess: "Copied!" });
  };
  downloadkey = () => {
    const element = document.createElement("a");
    const file = new Blob([this.state.key], { type: "application/x-pem-file" });
    element.href = URL.createObjectURL(file);
    element.download = "flowauth_public_key.pub";
    document.body.appendChild(element);
    if (window.Cypress) {
      // Do not attempt to actually download the file in test.
      // Just leave the anchor in there. Ensure your code doesn't
      // automatically remove it either.
      return;
    }
    element.click();
    element.remove();
  };

  render() {
    if (this.state.hasError) throw this.state.error;

    const { classes } = this.props;
    const { key, copySuccess } = this.state;
    return (
      <Paper className={classes.root}>
        <div className={classes.gridRoot}>
          <Grid
            container
            spacing={2}
            direction="row"
            justify="flex-start"
            alignItems="flex-start"
          >
            <Grid item xs>
              <Paper className={classes.root} id="public_key_body">
                {key.split("\n").map((val, ix) => {
                  return (
                    <Typography
                      variant="body1"
                      className={classes.codeBlock}
                      noWrap
                    >
                      {val}
                    </Typography>
                  );
                })}
              </Paper>
            </Grid>
            <Grid item xs={3}>
              <Tooltip title={copySuccess} placement="bottom">
                <Button
                  variant="outlined"
                  color="primary"
                  onClick={this.copyToClipboard}
                  className={classes.button}
                >
                  Copy
                </Button>
              </Tooltip>
              <Tooltip title={copySuccess} placement="bottom">
                <Button
                  variant="outlined"
                  color="primary"
                  onClick={this.copyToEncodedToClipboard}
                  className={classes.button}
                >
                  Copy encoded
                </Button>
              </Tooltip>
              <Button
                variant="outlined"
                color="primary"
                id="download"
                onClick={this.downloadkey}
                className={classes.button}
              >
                Download
              </Button>
            </Grid>
          </Grid>
        </div>
      </Paper>
    );
  }
}

PublicKey.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(PublicKey);
