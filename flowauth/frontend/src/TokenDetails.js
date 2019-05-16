/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Typography from "@material-ui/core/Typography";
import Divider from "@material-ui/core/Divider";
import TokenPermission from "./TokenPermission";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import { DateTimePicker, MuiPickersUtilsProvider } from "material-ui-pickers";
import DateFnsUtils from "@date-io/date-fns";
import { getMyRightsForServer, createToken } from "./util/api";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import SubmitButtons from "./SubmitButtons";
import ServerAggregationUnits from "./ServerAggregationUnits";

const styles = theme => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  }
});

class TokenDetails extends React.Component {
  state = {
    nickName: {},
    rights: {},
    expiry: new Date(),
    latest_expiry: new Date(),
    username_helper_text: "",
  };

  handleSubmit = () => {
    const { name, expiry, rights, name_helper_text } = this.state;
    const { serverID, cancel } = this.props;
    if (name && name_helper_text === "") {
      createToken(name, serverID, new Date(expiry).toISOString(), rights)
        .then(
          json => {
            cancel();
          }
        );
    }
  };

  handleDateChange = date => {
    this.setState(Object.assign(this.state, { expiry: date }));
  };

  handleChange = (claim, right) => event => {
    var rights = this.state.rights;
    rights[claim] = Object.assign({}, rights[claim]);
    rights[claim].permissions[right] = event.target.checked;
    this.setState(Object.assign(this.state, { rights: rights }));
  };

  handleAggUnitChange = (claim_id, claim, unit) => event => {
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
  };

  handleNameChange = event => {
    var letters = /^[A-Za-z0-9_]+$/;
    let name = event.target.value;
    if (name.match(letters)) {
      this.setState(Object.assign(this.state, {
        name_helper_text: ""
      }));
    } else if (name.length == 0) {
      this.setState(Object.assign(this.state, {
        name_helper_text: "Token name cannot be blank."
      }));
    } else {
      this.setState(Object.assign(this.state, {
        name_helper_text:
          "Token name may only contain letters, numbers and underscores."
      }));
    }
    this.setState({ name: event.target.value });
  };

  isAggUnitPermitted = (claim, key) => {
    const { permitted } = this.state;
    return permitted[claim].spatial_aggregation.indexOf(key) !== -1;
  };

  componentDidMount() {
    getMyRightsForServer(this.props.serverID)
      .then(json => {
        this.setState({
          rights: JSON.parse(JSON.stringify(json.allowed_claims || {})),
          permitted: json.allowed_claims || {},
          expiry: json.latest_expiry,
          latest_expiry: json.latest_expiry
        });
      })
      .catch(err => {
        this.setState({ hasError: true, error: err });
      });
  }

  renderRights = () => {
    var perms = [];
    const { rights, permitted } = this.state;
    for (const key in rights) {
      perms.push([
        <TokenPermission
          permissions={rights[key].permissions}
          claim={key}
          checkedHandler={this.handleChange}
          permitted={permitted[key].permissions}
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
    const { rights, permitted } = this.state;
    for (const key in rights) {
      perms.push([
        <ServerAggregationUnits
          units={rights[key].spatial_aggregation}
          claim={key}
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

    const { expiry, latest_expiry, name } = this.state;
    const { classes, onClick } = this.props;

    return (
      <React.Fragment>
        <Grid item xs={12}>
          <Typography variant="h5" component="h1">
            Token Name
          </Typography>
        </Grid>
        <Divider />
        <Grid item xs={12}>
          <TextField
            id="name"
            label="Name"
            className={classes.textField}
            value={name}
            onChange={this.handleNameChange}
            margin="normal"
            error={this.state.name_helper_text}
            helperText={this.state.name_helper_text}
          />
        </Grid>
        <Grid item xs={12}>
          <Typography variant="h5" component="h1">
            Token Expiry
          </Typography>
        </Grid>
        <Divider />
        <Grid item xs={12}>
          <MuiPickersUtilsProvider utils={DateFnsUtils}>
            <DateTimePicker
              value={expiry}
              onChange={this.handleDateChange}
              disablePast={true}
              maxDate={latest_expiry}
            />
          </MuiPickersUtilsProvider>
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
        <SubmitButtons handleSubmit={this.handleSubmit} onClick={onClick} />
      </React.Fragment>
    );
  }
}

TokenDetails.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(TokenDetails);
