/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Typography from "@material-ui/core/Typography";
import Divider from "@material-ui/core/Divider";
import Grid from "@material-ui/core/Grid";
import Button from "@material-ui/core/Button";
import { DateTimePicker, MuiPickersUtilsProvider } from "@material-ui/pickers";
import DateFnsUtils from "@date-io/date-fns";
import TextField from "@material-ui/core/TextField";
import SubmitButtons from "./SubmitButtons";
import ErrorDialog from "./ErrorDialog";
import {
  createServer,
  editServer,
  editServerCapabilities,
  getAllCapabilities,
  getCapabilities,
  getServer,
  getTimeLimits
} from "./util/api";
import RightsCascade from "./RightsCascade";
import { jsonify, scopesGraph } from "./util/util";

class ServerAdminDetails extends React.Component {
  constructor() {
    super();
    this.state = {
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

    this.fileReader = new FileReader();

    this.fileReader.onload = event => {
      const parsedSpec = JSON.parse(event.target.result);
      const specScopes = parsedSpec["components"]["securitySchemes"]["token"][
        "x-security-scopes"
      ].reduce((obj, cur) => ({ ...obj, [cur]: true }), {});
      const scopeGraph = scopesGraph(specScopes);
      const enabledKeys = [];
      const scopes = jsonify(
        scopeGraph,
        [],
        Object.keys(specScopes),
        enabledKeys
      );

      this.setState({
        rights: scopes,
        fullRights: Object.keys(specScopes),
        enabledRights: enabledKeys
      });
    };
  }

  handleSubmit = () => {
    const {
      edit_mode,
      name,
      latest_expiry,
      max_life,
      fullRights,
      enabledRights,
      name_helper_text,
      key_helper_text,
      maxlife_helper_text
    } = this.state;
    const { item_id, onClick } = this.props;

    const rightsObjs = fullRights.reduce(
      (obj, cur) => ({
        ...obj,
        [cur]: enabledRights.some(r => cur.startsWith(r))
      }),
      {}
    );

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
          return editServerCapabilities(json.id, rightsObjs);
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
    if (item_id !== -1) {
      try {
        const capAwait = getCapabilities(item_id);
        const limitsAwait = getTimeLimits(item_id);
        const scopeGraph = scopesGraph(await capAwait);
        const enabledKeys = [];
        const rights = await capAwait;
        const scopes = jsonify(
          scopeGraph,
          [],
          Object.keys(rights).filter(k => rights[k]),
          enabledKeys
        );
        this.setState({
          name: (await getServer(item_id)).name,
          rights: scopes,
          fullRights: Object.keys(rights),
          enabledRights: enabledKeys,
          latest_expiry: (await limitsAwait).latest_token_expiry,
          max_life: (await limitsAwait).longest_token_life,
          edit_mode: true
        });
      } catch (err) {
        this.setState({ hasError: true, error: err });
      }
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
          <Typography variant="h5" component="h1">
            Upload API Spec
          </Typography>
        </Grid>
        <Grid item xs={12}>
          <input
            accept=".json,.yml"
            className={classes.input}
            id="spec-upload-button"
            type="file"
            style={{ display: "none" }}
            onChange={event => {
              this.fileReader.readAsText(event.target.files[0]);
            }}
          />
          <label htmlFor="spec-upload-button">
            <Button
              variant="contained"
              component="span"
              className={classes.button}
            >
              Upload
            </Button>
          </label>
        </Grid>
        <Grid item xs={12}>
          <Typography variant="h5" component="h1">
            Available API scopes
          </Typography>
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
