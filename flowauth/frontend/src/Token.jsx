/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import Button from "@material-ui/core/Button";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import Dialog from "@material-ui/core/Dialog";
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import DialogTitle from "@material-ui/core/DialogTitle";
import Tooltip from "@material-ui/core/Tooltip";
import { withStyles } from "@material-ui/core/styles";

const styles = (theme) => ({
  button: {
    margin: theme.spacing.unit,
  },
});
class Token extends React.Component {
  state = {
    isOpen: false,
    copySuccess: "",
  };
  toggleOpen = () => {
    this.setState({ isOpen: !this.state.isOpen });
  };
  copyToClipboard = (event) => {
    var textField = document.createElement("textarea");
    textField.innerText = this.props.token;
    document.body.appendChild(textField);
    textField.select();
    document.execCommand("copy");
    textField.remove();
    this.setState({ copySuccess: "Copied!" });
  };
  downloadTxtFile = () => {
    const element = document.createElement("a");
    const file = new Blob([this.props.token], { type: "text/plain" });
    element.href = URL.createObjectURL(file);
    element.download = this.props.name + ".txt";
    document.body.appendChild(element);
    element.click();
  };
  render() {
    const { name, expiry, token, classes } = this.props;
    const { isOpen, copySuccess } = this.state;
    const isExpired = Date.parse(expiry) < Date.parse(new Date());
    return (
      <React.Fragment>
        <Grid item xs={2}>
          <Typography component="p" className={isExpired ? "expired" : ""}>
            {name}{" "}
          </Typography>
        </Grid>
        <Grid item xs={3}>
          <Typography component="p" className={isExpired ? "expired" : ""}>
            {expiry}
          </Typography>
        </Grid>
        <Grid item xs={7}>
          <Tooltip title={copySuccess} placement="bottom">
            <Button
              variant="outlined"
              color="primary"
              onClick={this.copyToClipboard}
            >
              Copy
            </Button>
          </Tooltip>
          <Button
            variant="outlined"
            color="primary"
            onClick={this.downloadTxtFile}
            className={classes.button}
          >
            Download
          </Button>
          <Button variant="outlined" color="primary" onClick={this.toggleOpen}>
            View
          </Button>
          <Dialog
            open={isOpen}
            onClose={this.toggleOpen}
            scroll="paper"
            aria-labelledby="scroll-dialog-title"
          >
            <DialogTitle id="scroll-dialog-title">Token</DialogTitle>
            <DialogContent>
              <DialogContentText>
                <DialogContentText style={{ wordWrap: "break-word" }}>
                  {token}
                </DialogContentText>
              </DialogContentText>
            </DialogContent>
          </Dialog>
        </Grid>
      </React.Fragment>
    );
  }
}

Token.propTypes = {
  classes: PropTypes.object.isRequired,
};
export default withStyles(styles)(Token);
