/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Dialog from "@material-ui/core/Dialog";
import DialogActions from "@material-ui/core/DialogActions";
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import DialogTitle from "@material-ui/core/DialogTitle";
import Button from "@material-ui/core/Button";

class WarningDialog extends React.Component {
  state = {
    open: this.props.open
  };

  handleClose = () => {
    this.setState({ open: false });
  };

  render() {
    const { message, open, handleClick } = this.props;

    return (
      <Dialog
        open={this.state.open}
        onClose={this.handleClose}
        aria-labelledby="warning-dialog-title"
        aria-describedby="warning-dialog-description"
        id="warning-dialog"
      >
        <DialogTitle id="warning-dialog-title">{"Warning"}</DialogTitle>
        <DialogContent>
          <DialogContentText id="warning-dialog-description">
            {message}
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button
            onClick={this.handleClose}
            color="danger"
            id="warning-dialog-cancle"
          >
            Cancle
          </Button>
          <Button onClick={handleClick} color="primary" id="warning-dialog-yes">
            Yes
          </Button>
        </DialogActions>
      </Dialog>
    );
  }

  componentDidUpdate(prevProps, prevState) {
    if (this.props.open !== prevState.open) {
      this.setState({ open: this.props.open });
    }
  }
}

export default WarningDialog;
