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
import PermissionDetails from "./PermissionDetails";
import WarningDialog from "./WarningDialog";

const styles = theme => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  },
  heading: {
    fontSize: theme.typography.pxToRem(18),
    fontWeight: theme.typography.alignCenter
  }
});
class TokenDetails extends React.Component {
  constructor(props) {
    super(props);
    this.nameRef = React.createRef();
  }
  state = {
    nickName: {},
    rights: {},
    expiry: new Date(),
    latest_expiry: new Date(),
    name_helper_text: "",
    uiReady: new Promise(() => {}),
    pageError: false,
    errors: { message: "" }
  };
  completeToken = () => {
    const { name, expiry, rights } = this.state;
    const { serverID, cancel } = this.props;
    createToken(name, serverID, new Date(expiry).toISOString(), rights).then(
      json => {
        cancel();
      }
    );
  };
  handleSubmit = async () => {
    const { name, name_helper_text, uiReady } = this.state;
    await uiReady;
    const checkedCheckboxes = document.querySelectorAll(
      'input[type="checkbox"]:checked'
    );
    if (name && name_helper_text === "" && checkedCheckboxes.length != 0) {
      this.completeToken();
    } else if (checkedCheckboxes.length == 0) {
      this.setState({
        pageError: true,
        errors: {
          message:
            "Warning: no permissions will be granted by this token. Are you sure?"
        }
      });
    } else if (!name) {
      this.setState({
        name_helper_text: "Token name cannot be blank."
      });
      this.scrollToRef(this.nameRef);
    }
  };

  handleDateChange = date => {
    this.setState(Object.assign(this.state, { expiry: date }));
  };

  scrollToRef = ref => ref.current.scrollIntoView();

  handleNameChange = event => {
    var letters = /^[A-Za-z0-9_]+$/;
    let name = event.target.value;
    if (name.match(letters)) {
      this.setState(
        Object.assign(this.state, {
          name_helper_text: ""
        })
      );
    } else if (name.length == 0) {
      this.setState(
        Object.assign(this.state, {
          name_helper_text: "Token name cannot be blank."
        })
      );
    } else {
      this.setState(
        Object.assign(this.state, {
          name_helper_text:
            "Token name may only contain letters, numbers and underscores."
        })
      );
    }
    this.setState({ name: event.target.value });
  };

  componentDidMount() {
    this.setState({
      uiReady: getMyRightsForServer(this.props.serverID)
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
        })
    });
  }

  render() {
    if (this.state.hasError) throw this.state.error;

    const {
      expiry,
      latest_expiry,
      name,
      name_helper_text,
      rights,
      permitted
    } = this.state;
    const { classes, onClick } = this.props;

    return (
      <React.Fragment>
        <div ref={this.nameRef} />
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
            error={name_helper_text !== ""}
            helperText={name_helper_text}
          />
        </Grid>
        <Grid item xs={12}>
          <Typography variant="h5" component="h1">
            Token Expiry
          </Typography>
        </Grid>
        <Divider />
        <Grid item xs>
          <MuiPickersUtilsProvider utils={DateFnsUtils}>
            <DateTimePicker
              value={expiry}
              onChange={this.handleDateChange}
              disablePast={true}
              maxDate={latest_expiry}
              format="yyyy/MM/dd HH:mm:ss"
              ampm={false}
              margin="normal"
              helperText={new Date().toTimeString().slice(9)} // Display the timezone
            />
          </MuiPickersUtilsProvider>
        </Grid>
        <Grid item xs={12}>
          <PermissionDetails
            rights={rights}
            permitted={permitted}
            updateRights={rights => this.setState({ rights: rights })}
          />
        </Grid>
        <WarningDialog
          open={this.state.pageError}
          message={this.state.errors.message}
          handleClick={this.completeToken}
        />
        <SubmitButtons handleSubmit={this.handleSubmit} onClick={onClick} />
      </React.Fragment>
    );
  }
}

TokenDetails.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(TokenDetails);
