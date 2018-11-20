/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from 'react';
import Dialog from '@material-ui/core/Dialog';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';

class ErrorDialog extends React.Component {
    state = {
        open: this.props.open,
    };

    handleClose = () => {
        this.setState({ open: false });
    }

    render() {
        const { message, open } = this.props;

        return (
            <Dialog
                open={this.state.open}
                onClose={this.handleClose}
                aria-labelledby="error-dialog-title"
                aria-describedby="error-dialog-description"
            >
                <DialogTitle id="error-dialog-title">{"Error"}</DialogTitle>
                <DialogContent>
                    <DialogContentText id="error-dialog-description">{message}</DialogContentText>
                </DialogContent>
            </Dialog>
        );
    }

    componentDidUpdate(prevProps, prevState) {
        if (this.props.open !== prevState.open) {
            this.setState({ open: this.props.open });
        }
    }
}

export default ErrorDialog;