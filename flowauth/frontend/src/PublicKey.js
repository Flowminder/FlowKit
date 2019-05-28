/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import Paper from "@material-ui/core/Paper";
import Typography from "@material-ui/core/Typography";
import {
  getPublicKey,
} from "./util/api";
import Grid from "@material-ui/core/Grid";
import Tooltip from "@material-ui/core/Tooltip";
import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelDetails from '@material-ui/core/ExpansionPanelDetails';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import Button from "@material-ui/core/Button";

const styles = theme => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2,
    button: {
    margin: theme.spacing.unit,
  }
  }
});

class PublicKey extends React.Component {
  state = {key:""}
  componentDidMount() {
    getPublicKey()
      .then(key => {
        this.setState({
          key: key.public_key
            })
      })
      .catch(err => {
        this.setState({ hasError: true, error: err });
      });
  }


  render() {
    if (this.state.hasError) throw this.state.error;

    const { classes } = this.props;
    const { key } = this.state;
    return (
      <Paper className={classes.root}>
          <Grid container spacing={16} >
        <Grid item xs={9}>
        <Paper className={classes.root}>

        {key.split("\n").map((val, ix) => {return <Typography variant="body1">{val}</Typography>})}

              </Paper>
              </Grid>
              <Grid item  xs={1}>
            <Button variant="outlined" color="primary" onClick={this.copyToClipboard} className={classes.button}>Copy</Button>
              </Grid>
              <Grid item xs={1}>
          <Button variant="outlined" color="primary" onClick={this.downloadTxtFile} className={classes.button}>Download</Button>
              </Grid>
              </Grid>
      </Paper>
    );
  }
}

PublicKey.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(PublicKey);
