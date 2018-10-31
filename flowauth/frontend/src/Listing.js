/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import IconButton from "@material-ui/core/IconButton";
import AddIcon from "@material-ui/icons/Add";
import AdminListItem from "./AdminListItem";

const styles = theme => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  }
});

class Listing extends React.Component {
  state = { objs: [] };
  componentDidMount() {
    const { getter } = this.props;
    getter()
      .then(objs => {
        this.setState({ objs: objs });
      })
      .catch(err => {
        this.setState({ hasError: true, error: err });
      });
  }
  rmObj = obj_id => {
    this.setState({
      objs: this.state.objs.filter(obj => obj.id !== obj_id)
    });
  };

  render() {
    if (this.state.hasError) throw this.state.error;

    const { classes, editAction, kind, deleteAction } = this.props;
    const { objs } = this.state;
    return (
      <React.Fragment>
        <Grid item xs={12}>
          <Typography variant="headline" component="h1">
            {kind}
          </Typography>
        </Grid>
        <Grid item xs={6}>
          <Typography component="h3">Name</Typography>
        </Grid>
        <Grid item xs={6} />
        {objs.map(object => (
          <AdminListItem
            name={object.name}
            id={object.id}
            classes={classes}
            editAction={editAction}
            rmAction={this.rmObj}
            deleteAction={deleteAction}
          />
        ))}

        <Grid item xs={11} />
        <Grid item xs>
          <IconButton
            color="inherit"
            aria-label="New"
            onClick={() => editAction(false)}
          >
            <AddIcon />
          </IconButton>
        </Grid>
      </React.Fragment>
    );
  }
}

Listing.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(Listing);
