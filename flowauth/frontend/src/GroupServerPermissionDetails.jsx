/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Typography from "@material-ui/core/Typography";
import Grid from "@material-ui/core/Grid";
import { DateTimePicker, MuiPickersUtilsProvider } from "@material-ui/pickers";
import DateFnsUtils from "@date-io/date-fns";
import TextField from "@material-ui/core/TextField";
import {
  getGroupTimeLimits,
  getTimeLimits,
  getGroupCapabilities
} from "./util/api";
import GroupServerAccessRights from "./GroupServerAccessRights";

class GroupServerPermissionDetails extends React.Component {
  state = {
    rights: {},
    max_life: "",
    longest_token_life: "",
    server_latest_expiry: new Date(),
    latest_expiry: new Date()
  };

  handleDateChange = date => {
    const { server, updateServer } = this.props;
    this.setState(Object.assign(this.state, { latest_expiry: date }));
    server["latest_expiry"] = new Date(date).toISOString();
    updateServer(server);
  };

  handleTextChange = name => event => {
    const { server, updateServer } = this.props;
    var val = parseInt(event.target.value, 10);
    if (val <= this.state.longest_token_life) {
      this.setState({
        [name]: val
      });
      server["max_life"] = val;
      updateServer(server);
    }
  };

  handleRightsChange = rights => {
    const { server, updateServer } = this.props;
    server["rights"] = rights;
    updateServer(server);
    updateServer(server);
  };

  async componentDidMount() {
    const { server, updateServer, group_id } = this.props;
    const serverLimits = await getTimeLimits(server.id);
    var limits = serverLimits;
    try {
      const groupLimits = await getGroupTimeLimits(server.id, group_id);
      limits = groupLimits;
    } catch (err) {
      if (err.code !== 404) {
        this.setState({ hasError: true, error: err });
      }
    }

    server["rights"] = await getGroupCapabilities(server.id, group_id);
    const expiry = limits.latest_token_expiry;
    server["latest_expiry"] = new Date(expiry).toISOString();
    server["max_life"] = limits.longest_token_life;
    updateServer(server);
    this.setState({
      latest_expiry: limits.latest_token_expiry,
      max_life: limits.longest_token_life,
      longest_token_life: serverLimits.longest_token_life,
      server_latest_expiry: serverLimits.latest_token_expiry
    });
  }

  render() {
    if (this.state.hasError) throw this.state.error;

    const { latest_expiry, server_latest_expiry } = this.state;
    const { classes, server, group_id } = this.props;

    return (
      <React.Fragment>
        <Grid item xs={12}>
          <Typography variant="h5" component="h1">
            Token Lifetime Limits
          </Typography>
        </Grid>
        <Grid item xs={6}>
          <MuiPickersUtilsProvider utils={DateFnsUtils}>
            <DateTimePicker
              label="Latest expiry"
              value={latest_expiry}
              className={classes.textField}
              onChange={this.handleDateChange}
              disablePast={true}
              maxDate={server_latest_expiry}
              margin="normal"
            />
          </MuiPickersUtilsProvider>
        </Grid>
        <Grid item xs={6}>
          <TextField
            id="standard-name"
            label="Maximum lifetime"
            className={classes.textField}
            type="number"
            value={this.state.max_life}
            onChange={this.handleTextChange("max_life")}
            margin="normal"
          />
        </Grid>
        <Grid item xs={12}>
          <GroupServerAccessRights
            groupId={group_id}
            serverId={server.id}
            parentUpdate={this.handleRightsChange}
          />
        </Grid>
      </React.Fragment>
    );
  }
}

export default GroupServerPermissionDetails;
