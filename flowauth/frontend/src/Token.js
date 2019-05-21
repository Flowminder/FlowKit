/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import Button from "@material-ui/core/Button";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import DeleteIcon from "@material-ui/icons/Delete";
import EditIcon from "@material-ui/icons/Edit";
import RefreshIcon from "@material-ui/icons/Refresh";
import IconButton from "@material-ui/core/IconButton";
import Dialog from "@material-ui/core/Dialog";
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import DialogTitle from "@material-ui/core/DialogTitle";

class Token extends React.Component {
  state = { isOpen: false };
  toggleOpen = () => {
    this.setState({ isOpen: !this.state.isOpen });
  };
  render() {
    const { name, expiry, editAction, token, id } = this.props;
    const { isOpen } = this.state;
    return (
      <React.Fragment>
        <Grid item xs={2}>
          <Typography component="p">{name}</Typography>
        </Grid>
        <Grid item xs={3}>
          <Typography component="p">{expiry}</Typography>
        </Grid>
        <Grid item xs={3}>
          <Button onClick={this.toggleOpen}>Token</Button>
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
        <Grid item xs={4} />
      </React.Fragment>
    );
  }
}

Token.propTypes = {
  classes: PropTypes.object.isRequired
};
export default Token;
