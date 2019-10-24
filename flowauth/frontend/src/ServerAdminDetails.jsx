/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Typography from "@material-ui/core/Typography";
import Divider from "@material-ui/core/Divider";
import Grid from "@material-ui/core/Grid";
import { DateTimePicker, MuiPickersUtilsProvider } from "@material-ui/pickers";
import DateFnsUtils from "@date-io/date-fns";
import TextField from "@material-ui/core/TextField";
import SubmitButtons from "./SubmitButtons";
import ErrorDialog from "./ErrorDialog";
import {
  getAllCapabilities,
  getCapabilities,
  getTimeLimits,
  createServer,
  getServer,
  editServerCapabilities,
  editServer
} from "./util/api";
import RightsCascade from "./RightsCascade";

function scopeStringListToGraph(array) {
  const nested = {};
  array.forEach(scope => {
    let obj = nested;
    scope.split(":").forEach(k => {
      if (!(k in obj)) {
        obj[k] = {};
      }
      obj = obj[k];
    });
  });
  return nested;
}

function jsonify(tree, labels) {
  const list = Object.keys(tree).map(k => {
    const ll = labels.concat([k]);
    const v = tree[k];
    if (Object.keys(v).length === 0) {
      return { label: k, value: ll.join(":") };
    } else {
      return { label: k, value: ll.join(":"), children: jsonify(v, ll) };
    }
  });
  return list;
}

class ServerAdminDetails extends React.Component {
  state = {
    name: "",
    rights: [],
    enabledRights: [],
    max_life: 1440,
    latest_expiry: new Date(new Date().getTime() + 24 * 60 * 60 * 1000),
    edit_mode: false,
    name_helper_text: "",
    key_helper_text: "",
    maxlife_helper_text: "",
    pageError: false,
    errors: { message: "" }
  };

  handleSubmit = () => {
    const {
      edit_mode,
      name,
      latest_expiry,
      max_life,
      rights,
      enabledRights,
      name_helper_text,
      key_helper_text,
      maxlife_helper_text
    } = this.state;
    const { item_id, onClick } = this.props;
    if (
      name &&
      name_helper_text === "" &&
      key_helper_text === "" &&
      max_life &&
      maxlife_helper_text === ""
    ) {
      var task;
      if (edit_mode) {
        task = editServer(
          item_id,
          name,
          new Date(latest_expiry).toISOString(),
          max_life
        );
      } else {
        task = createServer(
          name,
          new Date(latest_expiry).toISOString(),
          max_life
        );
      }
      task
        .then(json => {
          return editServerCapabilities(json.id, enabledRights);
        })
        .then(json => {
          onClick();
        })
        .catch(err => {
          if (err.code === 400) {
            this.setState({ pageError: true, errors: err });
          } else {
            this.setState({ hasError: true, error: err });
          }
        });
    }
  };

  fieldHasError = field => {
    if (this.state.hasError && this.state.error.code === 400) {
      return this.state.error.bad_field === field;
    } else {
      return false;
    }
  };

  handleDateChange = date => {
    this.setState({ latest_expiry: date });
  };

  handleRightsChange = value => {
    this.setState({ enabledRights: value });
  };

  handleTextChange = name => event => {
    this.setState({
      pageError: false,
      errors: ""
    });
    var state = {
      [name]: event.target.value
    };
    if (name === "name") {
      var letters = /^[A-Za-z0-9_]+$/;
      let servername = event.target.value;
      if (servername.match(letters) && servername.length <= 120) {
        state = Object.assign(state, {
          name_helper_text: ""
        });
      } else if (servername.length == 0) {
        state = Object.assign(state, {
          name_helper_text: "Server name can not be blank."
        });
      } else if (!servername.match(letters)) {
        state = Object.assign(state, {
          name_helper_text:
            "Server name may only contain letters, numbers and underscores."
        });
      } else {
        state = Object.assign(state, {
          name_helper_text: "Server name must be 120 characters or less."
        });
      }
    }

    if (name === "max_life") {
      let maxlife = event.target.value;
      if (maxlife.length == 0) {
        state = Object.assign(state, {
          maxlife_helper_text: "Maximum lifetime minutes can not be blank."
        });
      } else {
        state = Object.assign(state, {
          maxlife_helper_text: ""
        });
      }
    }
    this.setState(state);
  };

  async componentDidMount() {
    const { item_id } = this.props;
    var name, rights;
    try {
      const all_capabilities = getAllCapabilities();
      if (item_id !== -1) {
        const server = getServer(item_id);
        const capabilities = getCapabilities(item_id);
        const time_limits = getTimeLimits(item_id);
        this.setState({
          name: (await server).name,
          rights: jsonify(scopeStringListToGraph(Object.keys(await capabilities)), []),
          enabledRights: Object.keys(rights).filter(k => rights[k])
          latest_expiry: (await time_limits).latest_token_expiry,
          max_life: (await time_limits).longest_token_life,
          edit_mode: true
        });
      }
      this.setState({ permitted: await all_capabilities });
    } catch (err) {
      this.setState({ hasError: true, error: err });
    }
  }

  render() {
    if (this.state.hasError && this.state.error.code === 401)
      throw this.state.error;

    const { rights, latest_expiry, name, enabledRights, max_life } = this.state;
    const { classes, onClick } = this.props;

    return (
      <React.Fragment>
        <Grid item xs={12}>
          <Typography variant="h5" component="h1">
            {(this.state.edit_mode && "Edit ") || "New "} Server
          </Typography>
        </Grid>
        <Grid item xs={6}>
          <TextField
            // error={this.fieldHasError("name")}
            placeholder="Maximum 120 characters"
            id="name"
            label="Name"
            className={classes.textField}
            value={name}
            onChange={this.handleTextChange("name")}
            margin="normal"
            required={true}
            error={this.state.name_helper_text}
            helperText={this.state.name_helper_text}
          />
        </Grid>
        <Grid item xs={6} />
        <Divider />
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
              margin="normal"
            />
          </MuiPickersUtilsProvider>
        </Grid>
        <Grid item xs={6}>
          <TextField
            id="max-life"
            label="Maximum lifetime (minutes)"
            className={classes.textField}
            type="number"
            value={max_life}
            onChange={this.handleTextChange("max_life")}
            margin="normal"
            required={true}
            error={this.state.maxlife_helper_text}
            helperText={this.state.maxlife_helper_text}
          />
        </Grid>
        <Grid item xs={12}>
          <RightsCascade
            options={rights}
            value={enabledRights}
            onChange={this.handleRightsChange}
          />
        </Grid>
        <ErrorDialog
          open={this.state.pageError}
          message={this.state.errors.message}
        />
        <SubmitButtons handleSubmit={this.handleSubmit} onClick={onClick} />
      </React.Fragment>
    );
  }
}

export default ServerAdminDetails;
