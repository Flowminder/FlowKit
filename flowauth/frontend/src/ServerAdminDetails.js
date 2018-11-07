/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Typography from "@material-ui/core/Typography";
import Divider from "@material-ui/core/Divider";
import ServerCapability from "./ServerCapability";
import Grid from "@material-ui/core/Grid";
import { DateTimePicker } from "material-ui-pickers";
import MuiPickersUtilsProvider from "material-ui-pickers/utils/MuiPickersUtilsProvider";
import DateFnsUtils from "material-ui-pickers/utils/date-fns-utils";
import RefreshIcon from "@material-ui/icons/Refresh";
import IconButton from "@material-ui/core/IconButton";
import InputAdornment from "@material-ui/core/InputAdornment";
import TextField from "@material-ui/core/TextField";
import SubmitButtons from "./SubmitButtons";
import ServerAggregationUnits from "./ServerAggregationUnits";
import { generate } from "generate-password";
import {
  getAllAggregationUnits,
  getAllCapabilities,
  getCapabilities,
  getTimeLimits,
  createServer,
  getServer,
  editServerCapabilities,
  editServer
} from "./util/api";

class ServerAdminDetails extends React.Component {
  state = {
    name: "",
    rights: {},
    max_life: 1440,
    secret_key: "",
    latest_expiry: new Date(new Date().getTime() + 24 * 60 * 60 * 1000),
    edit_mode: false
  };

  handleSubmit = () => {
    const {
      edit_mode,
      name,
      latest_expiry,
      max_life,
      rights,
      secret_key
    } = this.state;
    const { item_id, onClick } = this.props;
    var task;
    if (edit_mode) {
      task = editServer(
        item_id,
        name,
        secret_key,
        new Date(latest_expiry).toISOString(),
        max_life
      );
    } else {
      task = createServer(
        name,
        secret_key,
        new Date(latest_expiry).toISOString(),
        max_life
      );
    }
    task
      .then(json => {
        return editServerCapabilities(json.id, rights);
      })
      .then(json => {
        onClick();
      })
      .catch(err => {
        this.setState({ hasError: true, error: err });
      });
  };

  generatePassword = event => {
    this.setState({
      secret_key: generate({ length: 16, numbers: true, symbols: true })
    });
  };

  handleDateChange = date => {
    this.setState({ latest_expiry: date });
  };

  handleChange = (claim_id, claim, right) => event => {
    var rights = Object.assign({}, this.state.rights);
    rights[claim].permissions[right] = event.target.checked;
    this.setState(Object.assign(this.state, { rights: rights }));
  };

  handleAggUnitChange = (claim_id, claim, unit) => event => {
    var rights = Object.assign({}, this.state.rights);
    if (event.target.checked) {
      rights[claim].spatial_aggregation.push(unit);
    } else {
      rights[claim].spatial_aggregation = rights[
        claim
      ].spatial_aggregation.filter(u => u != unit);
    }
    this.setState({ rights: rights });
  };

  handleTextChange = name => event => {
    this.setState({
      [name]: event.target.value
    });
  };

  componentDidMount() {
    const { item_id } = this.props;
    var name, rights, secret_key;
    if (item_id && true) {
      getServer(item_id)
        .then(json => {
          name = json.name;
          secret_key = json.secret_key;
          return getCapabilities(item_id);
        })
        .then(json => {
          rights = json;
          return getTimeLimits(item_id);
        })
        .then(json => {
          this.setState({
            name: name,
            rights: rights,
            secret_key: secret_key,
            latest_expiry: json.latest_token_expiry,
            max_life: json.longest_token_life,
            edit_mode: true
          });
        })
        .catch(err => {
          this.setState({ hasError: true, error: err });
        });
    } else {
      getAllCapabilities()
        .then(json => {
          this.setState({
            rights: json,
            secret_key: generate({ length: 16, numbers: true, symbols: true })
          });
        })
        .catch(err => {
          this.setState({ hasError: true, error: err });
        });
    }
  }

  render() {
    if (this.state.hasError) throw this.state.error;

    const { rights, latest_expiry, name, secret_key, max_life } = this.state;
    const { classes, onClick } = this.props;

    return (
      <React.Fragment>
        <Grid item xs={12}>
          <Typography variant="headline" component="h1">
            {(this.state.edit_mode && "Edit ") || "New "} Server
          </Typography>
        </Grid>
        <Grid item xs={6}>
          <TextField
            id="standard-name"
            label="Name"
            className={classes.textField}
            value={name}
            onChange={this.handleTextChange("name")}
            margin="normal"
          />
        </Grid>
        <Grid item xs={6}>
          <TextField
            id="standard-name"
            className={classes.textField}
            label="Secret Key"
            value={secret_key}
            onChange={this.handleTextChange("secret_key")}
            margin="normal"
            InputProps={{
              endAdornment: (
                <InputAdornment position="end">
                  <IconButton
                    color="inherit"
                    className={classes.button}
                    aria-label="New password"
                    onClick={this.generatePassword}
                  >
                    <RefreshIcon />
                  </IconButton>
                </InputAdornment>
              )
            }}
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
            label="Maximum lifetime (minutes)"
            className={classes.textField}
            type="number"
            value={max_life}
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
            permitted={() => {
              return true;
            }}
          />
        ))}
        <Grid item xs={12}>
          <Typography variant="headline" component="h1">
            Aggregation Units
          </Typography>
        </Grid>
        <Divider />
        {Object.keys(rights).map(key => (
          <ServerAggregationUnits
            units={rights[key].spatial_aggregation}
            claim={key}
            claim_id={rights[key].id}
            checkedHandler={this.handleAggUnitChange}
            permitted={() => {
              return true;
            }}
          />
        ))}
        <SubmitButtons handleSubmit={this.handleSubmit} onClick={onClick} />
      </React.Fragment>
    );
  }
}

export default ServerAdminDetails;
