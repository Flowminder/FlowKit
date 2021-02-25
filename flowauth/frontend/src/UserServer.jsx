/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import Paper from "@material-ui/core/Paper";
import Grid from "@material-ui/core/Grid";
import TokenList from "./TokenList";
import TokenDetails from "./TokenDetails";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2,
  },
});

class UserServer extends React.Component {
  state = {
    editing: false,
    editing_item: null,
  };
  toggleEdit = (item_id) => {
    this.setState({
      editing: !this.state.editing,
      editing_item: item_id,
    });
  };

  componentDidUpdate(prevProps) {
    if (this.props.serverID !== prevProps.serverID) {
      this.setState({ editing: false });
    }
  }

  getBody = () => {
    if (this.state.editing) {
      return (
        <TokenDetails
          classes={this.props.classes}
          onClick={this.toggleEdit}
          serverID={this.props.serverID}
          cancel={this.toggleEdit}
        />
      );
    } else {
      return (
        <TokenList
          nickName={this.props.serverName}
          serverID={this.props.serverID}
          editAction={this.toggleEdit}
        />
      );
    }
  };
  render() {
    if (this.state.hasError) throw this.state.error;

    const { classes } = this.props;
    return (
      <Paper className={classes.root}>
        <Grid container spacing={2} alignItems="center">
          {this.getBody()}
        </Grid>
      </Paper>
    );
  }
}

UserServer.propTypes = {
  classes: PropTypes.object.isRequired,
};

UserServer.defaultProps = {
  editing: false,
};

export default withStyles(styles)(UserServer);
