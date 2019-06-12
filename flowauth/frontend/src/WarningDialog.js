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
    open: this.props.open,
    callback: null
  };

  handleClose = () => {
    this.setState({ open: false, callback: null });
  };
  hide = () => this.setState({ open: false, callback: null });
  confirm = () => {
    console.log("confirm");
    this.state.callback();
    this.hide();
  };
  show = callback => event => {
    console.log("show");
    event.preventDefault();

    event = {
      ...event,
      target: { ...event.target, value: event.target.value }
    };

    this.setState({
      open: true,
      callback: () => callback(event)
    });
  };

  render() {
    const { message, open } = this.props;

    return (
      <React.Fragment>
        {this.props.children(this.show)}
        {this.state.open && (
          <Dialog
            // open={this.state.open}
            // onClose={this.handleClose}
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
              <Button onClick={this.hide} color="danger" id="warning-dialog-ok">
                Cancle
              </Button>
              <Button
                onClick={this.confirm}
                color="primary"
                id="warning-dialog-ok"
              >
                Yes
              </Button>
            </DialogActions>
          </Dialog>
        )}
      </React.Fragment>
    );
  }

  // componentDidUpdate(prevProps, prevState) {
  //   if (this.props.open !== prevState.open) {
  //     this.setState({ open: this.props.open });
  //   }
  // }
}

export default WarningDialog;
