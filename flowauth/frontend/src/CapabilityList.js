/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import Paper from "@material-ui/core/Paper";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import IconButton from "@material-ui/core/IconButton";
import AddIcon from "@material-ui/icons/Add";
import AdminListItem from "./AdminListItem";
import TextField from "@material-ui/core/TextField";
import {
  getAllCapabilities,
  newCapability,
  deleteCapability
} from "./util/api";

const styles = theme => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  }
});

class CapabilityList extends React.Component {
  state = { capabilities: [], new_cap: "" };
  componentDidMount() {
    getAllCapabilities()
      .then(capabilities => {
        this.setState({
          capabilities: Object.keys(capabilities)
            .map(key => {
              return { name: key, id: capabilities[key].id };
            })
            .sort()
        });
      })
      .catch(err => {
        this.setState({ hasError: true, error: err });
      });
  }
  handleChange = name => event => {
    this.setState({
      [name]: event.target.value
    });
  };
  rmCapability = capability_id => {
    deleteCapability(capability_id).then(json =>
      this.setState({
        capabilities: this.state.capabilities.filter(
          capability => capability.id !== capability_id
        )
      })
    );
  };
  addCap = () => {
    const { new_cap, capabilities } = this.state;
    if (new_cap.length > 0) {
      newCapability(new_cap).then(json => {
        capabilities.push({ name: json.name, id: json.id });
        this.setState({
          new_cap: "",
          capabilities: capabilities
        });
      });
    }
  };
  renderCapabilities = () => {
    const { classes } = this.props;
    const { capabilities } = this.state;
    return capabilities.map(object => {
      return (
        <AdminListItem
          name={object.name}
          id={object.id}
          classes={classes}
          deleteAction={this.rmCapability}
        />
      );
    });
  };

  render() {
    if (this.state.hasError) throw this.state.error;

    const { classes } = this.props;
    const { new_cap } = this.state;
    return (
      <Paper className={classes.root}>
        <Grid container spacing={16} alignItems="center">
          <Grid item xs={12}>
            <Typography variant="headline" component="h1">
              Capabilities
            </Typography>
          </Grid>
          <Grid item xs={6}>
            <Typography component="h3">Capability name</Typography>
          </Grid>
          <Grid item xs={6} />
          {this.renderCapabilities()}
          <Grid item xs={6}>
            <TextField
              id="standard-name"
              label="New Route"
              className={classes.textField}
              value={new_cap}
              onChange={this.handleChange("new_cap")}
              margin="normal"
              InputLabelProps={{ shrink: true }}
            />
          </Grid>
          <Grid item xs={12} container alignItems="center" justify="flex-end">
            <Grid item>
              <IconButton
                color="inherit"
                aria-label="New"
                onClick={this.addCap}
              >
                <AddIcon />
              </IconButton>
            </Grid>
          </Grid>
        </Grid>
      </Paper>
    );
  }
}

CapabilityList.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(CapabilityList);
