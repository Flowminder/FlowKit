/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Button from "@material-ui/core/Button";
import Typography from "@material-ui/core/Typography";
import Divider from "@material-ui/core/Divider";
import ServerCapability from "./ServerCapability";
import Grid from "@material-ui/core/Grid";
import { DateTimePicker } from "material-ui-pickers";
import MuiPickersUtilsProvider from "material-ui-pickers/utils/MuiPickersUtilsProvider";
import DateFnsUtils from "material-ui-pickers/utils/date-fns-utils";
import TextField from "@material-ui/core/TextField";
import SubmitButtons from "./SubmitButtons";
import { generate } from "generate-password";
import { getCapabilities, createServer } from "./util/api";

class ServerAdminDetails extends React.Component {
  state = {
    name: "",
    rights: {},
    max_life: "",
    secret_key: generate({ length: 16, numbers: true, symbols: true }),
    latest_expiry: new Date()
  };

  handleSubmit = () => {
    createServer(
      this.state.name,
      new Date(this.state.latest_expiry).toISOString(),
      this.state.max_life,
      this.state.rights
    ).then(json => {
      alert(json.server);
    });
  };

  handleDateChange = date => {
    this.setState(Object.assign(this.state, { latest_expiry: date }));
  };

  handleChange = (claim_id, claim, right) => event => {
    var rights = Object.assign({}, this.state.rights);
    rights[claim].permissions[right] = event.target.checked;
    this.setState(Object.assign(this.state, { rights: rights }));
  };

  handleTextChange = name => event => {
    this.setState({
      [name]: event.target.value
    });
  };

  componentDidMount() {
    this._asyncRequest = getCapabilities()
      .then(json => {
        this._asyncRequest = null;
        Object.keys(json).forEach(
          key =>
            (json[key].permissions = {
              read: false,
              write: false,
              status: false
            })
        );
        this.setState({
          rights: json
        });
      })
      .catch(err => {
        this.setState({ hasError: true, error: err });
      });
  }

  render() {
    if (this.state.hasError) throw this.state.error;

    const { rights, max_life, latest_expiry, name, secret_key } = this.state;
    const { classes, onClick, cancel } = this.props;

    return (
      <React.Fragment>
        <Grid item xs={12}>
          <Typography variant="headline" component="h1">
            New Server
          </Typography>
        </Grid>
        <Grid item xs={6}>
          <TextField
            id="standard-name"
            label="Name"
            className={classes.textField}
            value={this.state.name}
            onChange={this.handleTextChange("name")}
            margin="normal"
          />
        </Grid>
        <Grid item xs={6}>
          <TextField
            id="standard-name"
            label="Secret Key"
            className={classes.textField}
            value={this.state.secret_key}
            onChange={this.handleTextChange("secret_key")}
            margin="normal"
          />
        </Grid>
        <Divider />
        <Grid item xs={12}>
          <Typography variant="headline" component="h1">
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
          <Typography variant="headline" component="h1">
            API Permissions
          </Typography>
        </Grid>
        <Divider />
        {Object.keys(rights).map(key => (
          <ServerCapability
            permissions={rights[key].permissions}
            claim={key}
            claim_id={rights[key].id}
            checkedHandler={this.handleChange}
          />
        ))}
        <SubmitButtons handleSubmit={this.handleSubmit} onClick={onClick} />
      </React.Fragment>
    );
  }
}

export default ServerAdminDetails;
