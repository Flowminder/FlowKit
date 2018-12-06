/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Typography from "@material-ui/core/Typography";
import Divider from "@material-ui/core/Divider";
import ServerCapability from "./ServerCapability";
import ServerAggregationUnits from "./ServerAggregationUnits";
import Grid from "@material-ui/core/Grid";
import { DateTimePicker, MuiPickersUtilsProvider } from "material-ui-pickers";
import DateFnsUtils from "@date-io/date-fns";
import TextField from "@material-ui/core/TextField";
import {
  getGroupCapabilities,
  getGroupTimeLimits,
  getTimeLimits,
  getCapabilities
} from "./util/api";

class GroupServerPermissionDetails extends React.Component {
  state = {
    permitted: {},
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

  handleChange = (claim_id, claim, right) => event => {
    const { server, updateServer } = this.props;
    var rights = this.state.rights;
    rights[claim] = Object.assign({}, rights[claim]);
    rights[claim].permissions[right] = event.target.checked;
    this.setState({ rights: rights });
    server["rights"] = rights;
    updateServer(server);
  };

  handleAggUnitChange = (claim_id, claim, unit) => event => {
    const { server, updateServer } = this.props;
    var rights = this.state.rights;
    rights[claim] = Object.assign({}, rights[claim]);
    if (event.target.checked) {
      rights[claim].spatial_aggregation.push(unit);
    } else {
      rights[claim].spatial_aggregation = rights[
        claim
      ].spatial_aggregation.filter(u => u != unit);
    }
    this.setState({ rights: rights });
    server["rights"] = rights;
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

  componentDidMount() {
    const { server, updateServer, group_id } = this.props;
    var allowed_caps, server_limits, group_rights, group_limits;
    getCapabilities(server.id)
      .then(json => {
        allowed_caps = json;
        return getTimeLimits(server.id);
      })
      .then(json => {
        server_limits = json;
        return getGroupCapabilities(server.id, group_id);
      })
      .then(json => {
        group_rights = json;
        return getGroupTimeLimits(server.id, group_id);
      })
      .catch(err => {
        if (err.code === 404) {
          group_rights = null;
          return getGroupTimeLimits(server.id, group_id);
        } else {
          throw err;
        }
      })
      .then(json => {
        group_limits = json;
      })
      .catch(err => {
        if (err.code === 404) {
          group_limits = null;
        } else {
          throw err;
        }
      })
      .then(() => {
        var rights =
          group_rights || JSON.parse(JSON.stringify(allowed_caps || {}));
        var limits = group_limits || server_limits;
        this.setState({
          rights: rights,
          permitted: allowed_caps || {},
          latest_expiry: limits.latest_token_expiry,
          max_life: limits.longest_token_life,
          longest_token_life: server_limits.longest_token_life,
          server_latest_expiry: server_limits.latest_token_expiry
        });
        server["rights"] = rights;
        var expiry = limits.latest_token_expiry;
        server["latest_expiry"] = new Date(expiry).toISOString();
        server["max_life"] = limits.longest_token_life;
        updateServer(server);
      })
      .catch(err => {
        this.setState({ hasError: true, error: err });
      });
  }

  isPermitted = (claim, key) => {
    const { permitted } = this.state;
    return permitted[claim].permissions[key];
  };

  isAggUnitPermitted = (claim, key) => {
    const { permitted } = this.state;
    return permitted[claim].spatial_aggregation.indexOf(key) !== -1;
  };

  renderRights = () => {
    var perms = [];
    const { rights } = this.state;
    for (const key in rights) {
      perms.push([
        <ServerCapability
          permissions={rights[key].permissions}
          claim={key}
          claim_id={rights[key].id}
          checkedHandler={this.handleChange}
          permitted={this.isPermitted}
        />,
        key
      ]);
    }
    return perms
      .sort((a, b) => {
        if (a[1] > b[1]) {
          return 1;
        } else if (a[1] < b[1]) {
          return -1;
        } else {
          return 0;
        }
      })
      .map(x => x[0]);
  };

  renderAggUnits = () => {
    var perms = [];
    const { rights } = this.state;
    for (const key in rights) {
      perms.push([
        <ServerAggregationUnits
          units={rights[key].spatial_aggregation}
          claim={key}
          claim_id={rights[key].id}
          checkedHandler={this.handleAggUnitChange}
          permitted={this.isAggUnitPermitted}
        />,
        key
      ]);
    }
    return perms
      .sort((a, b) => {
        if (a[1] > b[1]) {
          return 1;
        } else if (a[1] < b[1]) {
          return -1;
        } else {
          return 0;
        }
      })
      .map(x => x[0]);
  };

  render() {
    if (this.state.hasError) throw this.state.error;

    const { latest_expiry, server_latest_expiry } = this.state;
    const { classes } = this.props;

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
          <Typography variant="h5" component="h1">
            API Permissions
          </Typography>
        </Grid>
        <Divider />
        {this.renderRights()}
        <Grid item xs={12}>
          <Typography variant="h5" component="h1">
            Aggregation Units
          </Typography>
        </Grid>
        <Divider />
        {this.renderAggUnits()}
      </React.Fragment>
    );
  }
}

export default GroupServerPermissionDetails;
