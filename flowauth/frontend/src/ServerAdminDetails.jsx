/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Typography from "@material-ui/core/Typography";
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
  getCapabilities,
  getServer,
  getTimeLimits,
} from "./util/api";
import RightsCascade from "./RightsCascade";
import { jsonify, scopesGraph } from "./util/util";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core";

const styles = (theme) => ({
  button: {
    margin: theme.spacing.unit,
  },
});

class ServerAdminDetails extends React.Component {
  constructor() {
    super();
    this.state = {
      name: "",
      rights: [],
      enabledRights: [],
      fullRights: [],
      max_life: 1440,
      latest_expiry: new Date(new Date().getTime() + 24 * 60 * 60 * 1000),
      name_helper_text: "",
      key_helper_text: "",
      maxlife_helper_text: "",
      pageError: false,
      errors: { message: "" },
    };

    this.fileReader = new FileReader();

    this.fileReader.onload = (event) => {
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
        scopeGraph: scopeGraph,
        fullRights: Object.keys(specScopes),
        enabledRights: enabledKeys,
        name: parsedSpec["components"]["securitySchemes"]["token"][
          "x-audience"
        ],
      });
    };
  }

  submitAndExit = () => {
    this.handleSubmit().then();
  };

  handleSubmit = async () => {
    const {
      name,
      latest_expiry,
      max_life,
      fullRights,
      enabledRights,
      name_helper_text,
      key_helper_text,
      maxlife_helper_text,
    } = this.state;
    const { item_id, onClick } = this.props;

    const rightsObjs = fullRights.reduce(
      (obj, cur) => ({
        ...obj,
        [cur]: enabledRights.some((r) => cur.startsWith(r)),
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
      const server = this.editMode()
        ? editServer(
            item_id,
            name,
            new Date(latest_expiry).toISOString(),
            max_life
          )
        : createServer(name, new Date(latest_expiry).toISOString(), max_life);
      try {
        await editServerCapabilities((await server).id, rightsObjs);
        onClick();
      } catch (err) {
        if (err.code === 400) {
          this.setState({ pageError: true, errors: err });
        } else {
          this.setState({ hasError: true, error: err });
        }
      }
    }
  };

  handleDateChange = (date) => {
    this.setState({ latest_expiry: date });
  };

  handleRightsChange = (value) => {
    this.setState({ enabledRights: value });
  };

  handleTextChange = (name) => (event) => {
    this.setState({
      pageError: false,
      errors: "",
    });
    this.setState({
      [name]: event.target.value,
    });

    if (name === "max_life") {
      let maxlife = event.target.value;
      if (maxlife.length === 0) {
        this.setState({
          maxlife_helper_text: "Maximum lifetime minutes can not be blank.",
        });
      } else {
        this.setState({
          maxlife_helper_text: "",
        });
      }
    }
  };

  editMode = () => {
    return this.props.item_id !== -1;
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
          Object.keys(rights).filter((k) => rights[k]),
          enabledKeys
        );
        const serverName = (await getServer(item_id)).name;
        const latestExpiry = (await limitsAwait).latest_token_expiry;
        const maxLife = (await limitsAwait).longest_token_life;
        this.setState((state, props) => {
          return {
            name: serverName,
            rights: state.rights.length == 0 ? scopes : state.rights,
            fullRights:
              state.fullRights.length == 0
                ? Object.keys(rights)
                : state.fullRights,
            enabledRights:
              state.enabledRights.length == 0
                ? enabledKeys
                : state.enabledRights,
            latest_expiry: latestExpiry,
            max_life: maxLife,
          };
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
            {(this.editMode() && "Edit ") || "New "} Server
          </Typography>
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
            onChange={(event) => {
              this.fileReader.readAsText(event.target.files[0]);
            }}
          />
          <label htmlFor="spec-upload-button">
            <Button
              variant="contained"
              component="span"
              className={classes.button}
              id="spec-upload-button-target"
            >
              Upload
            </Button>
          </label>
        </Grid>
        <Grid item xs={6}>
          <TextField
            // error={this.fieldHasError("name")}
            id="name"
            label="Name"
            className={classes.textField}
            value={name}
            margin="normal"
            disabled={true}
          />
        </Grid>
        <Grid item xs={6} />
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
        <SubmitButtons handleSubmit={this.submitAndExit} onClick={onClick} />
      </React.Fragment>
    );
  }
}
ServerAdminDetails.propTypes = {
  classes: PropTypes.object.isRequired,
};
export default withStyles(styles)(ServerAdminDetails);
