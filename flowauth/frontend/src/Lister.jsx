/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Paper from "@material-ui/core/Paper";
import Grid from "@material-ui/core/Grid";
import Listing from "./Listing";

function Lister(DetailComponent, kind, get_fn, delete_fn) {
  return class extends React.Component {
    state = {
      editing: false,
      editing_item: null
    };
    toggleEdit = item_id => {
      this.setState({
        editing: !this.state.editing,
        editing_item: item_id
      });
    };

    getBody = () => {
      const { editing, editing_item } = this.state;
      if (editing) {
        return (
          <DetailComponent
            classes={this.props.classes}
            onClick={this.toggleEdit}
            item_id={editing_item}
          />
        );
      } else {
        return (
          <Listing
            editAction={this.toggleEdit}
            deleteAction={delete_fn}
            kind={kind}
            getter={get_fn}
          />
        );
      }
    };
    render() {
      const { classes } = this.props;
      return (
        <Paper className={classes.root}>
          <Grid container spacing={16} alignItems="center">
            {this.getBody()}
          </Grid>
        </Paper>
      );
    }
  };
}

export default Lister;
