/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import RoleMembersPicker from "./RoleMembersPicker";
import SubmitButtons from "./SubmitButtons";
import { createRole, getServer, getRole, editRole } from "./util/api";
import { useEffect, useState } from "react";
import { DateTimePicker, MuiPickersUtilsProvider } from "@material-ui/pickers";
import DateFnsUtils from "@date-io/date-fns";
import RoleScopePicker from "./RoleScopePicker";
import Dialog from "@material-ui/core/Dialog";
import DialogActions from "@material-ui/core/DialogActions";
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import DialogTitle from "@material-ui/core/DialogTitle";
import Button from "@material-ui/core/Button";

function RoleDetails(props) {
  //Properties:
  //role_id

  const { role_id, classes, onClick, server_id } = props;

  const [role, setRole] = useState({});
  const [server, setServer] = useState({});
  const [name, setRoleName] = useState("");
  const [members, setMembers] = useState([]);
  const [edit_mode, setEditMode] = useState(false);
  const [name_helper_text, setNameHelperText] = useState("");
  const [nameIsValid, setNameIsValid] = useState(true);
  const [errors, setErrors] = useState("Unknown error, please report");
  const [is_errored, setIsErrored] = useState(false);
  const [expiryDate, setExpiryDate] = useState(new Date());
  const [maxLifetime, setMaxLifetime] = useState("");
  const [lifetimeHelperText, setLifetimeHelperText] = useState("");
  const [lifetimeIsValid, setLifetimeIsValid] = useState(true);
  const [scopes, setScopes] = useState([]);
  const [formIsValid, setFormIsValid] = useState(false);

  const validation_vars = [nameIsValid, lifetimeIsValid, scopes, members];

  // get appropriate roles + servers on server_id set
  useEffect(() => {
    const fetch_role = async () => {
      const role = await getRole(role_id);
      setRole(role);
    };

    const fetch_server = async () => {
      const server = await getServer(server_id);
      setServer(server);
      setExpiryDate(server.latest_token_expiry);
      setMaxLifetime(String(server.longest_token_life_minutes));
    };

    const handle_role_error = (err) => {
      if (err.code !== 404) {
        setRole({});
        setErrors(err.message);
        setIsErrored(true);
      }
    };

    if (server_id >= 0) {
      fetch_server().then(
        () => {
          if (role_id >= 0) {
            fetch_role().catch(handle_role_error);
          }
        },
        (err) => console.error(err)
      );
    }
  }, [server_id]);

  //When Role changes, replace role.name, role.server and role.members with
  //the parts from the others.
  useEffect(() => {
    if (Object.keys(role).length !== 0) {
      //ffs, Javascript
      setRoleName(role.name);
      setMembers(role.users);
      setExpiryDate(role.latest_token_expiry);
      setMaxLifetime(String(role.longest_token_life_minutes));
      setScopes(role.scopes);
      setEditMode(true);
    } else {
      console.log("Role empty, setting defaults");
      setRoleName("");
      setMembers([]);
      setScopes([]);
      setEditMode(false);
    }
  }, [role]);

  //Validate Rolename on change
  useEffect(() => {
    console.log("Name:" + name);
    var letters = /^[A-Za-z0-9_]+$/;
    if (name.match(letters)) {
      setNameHelperText("");
      setNameIsValid(true);
    } else if (name.length === 0) {
      setNameHelperText("Role name cannot be blank.");
      setNameIsValid(false);
    } else {
      setNameHelperText(
        "Role name may only contain letters, numbers and underscores."
      );
      setNameIsValid(false);
    }
  }, [name]);

  //Validate lifetime on change
  useEffect(() => {
    console.log("New lifetime: " + maxLifetime);
    var numbers = /^[0-9]+$/;
    if (maxLifetime.match(numbers)) {
      setLifetimeHelperText("");
      setLifetimeIsValid(true);
    } else if (maxLifetime === "") {
      setLifetimeHelperText("Maximum lifetime cannot be blank");
      setLifetimeIsValid(false);
    } else {
      setLifetimeHelperText("Lifetime must be a number");
      setLifetimeIsValid(false);
    }
  }, [maxLifetime]);

  //Make sure entire form is valid on change for any relevant members
  useEffect(() => {
    const valid_vars = validation_vars.map((v) => typeof v);
    if (valid_vars.includes("undefined")) {
      setFormIsValid(false);
    } else if (nameIsValid && lifetimeIsValid && scopes.length > 0) {
      console.log("Form is valid");
      setFormIsValid(true);
    } else {
      setFormIsValid(false);
    }
  }, validation_vars);

  const handleNameChange = (event) => {
    setRoleName(event.target.value);
  };

  const handleLifetimeChange = (event) => {
    setMaxLifetime(event.target.value);
  };

  const handleMembersChange = (new_members) => {
    setMembers(new_members.map((x) => x.id));
  };

  const handleScopesChange = (new_scopes) => {
    console.debug("Scopes now:", new_scopes);
    setScopes(new_scopes.filter((s) => s.enabled === true));
  };

  //Either update or create a new role on 'submit' button
  const handleSubmit = async () => {
    console.log("Role form submitted");
    // Need to throw an error here if !formIsValid
    if (!formIsValid) {
      setIsErrored(true);
      setErrors(new Error("Validation error"));
    } else if (edit_mode) {
      try {
        await editRole(
          role.id,
          name,
          scopes.map((s) => s.id),
          members,
          expiryDate,
          maxLifetime
        );
        onClick();
      } catch (err) {
        console.error("Error in editRole");
        console.error(err);
        setIsErrored(true);
        setErrors(err.statusText);
      }
    } else {
      try {
        await createRole(
          name,
          server.id,
          scopes.map((s) => s.id),
          members,
          expiryDate,
          maxLifetime
        );
        onClick();
      } catch (err) {
        console.error("Error in createRole");
        console.error(err);
        setIsErrored(true);
        setErrors(err.statusText);
      }
    }
  };

  return (
    <React.Fragment>
      <Grid item xs={12}>
        <Typography variant="h5" component="h1">
          {(edit_mode && "Edit Role") || "New Role"}
        </Typography>
      </Grid>
      <Grid item xs={12}>
        <Typography varient="h5" component="h2">
          Server: {server.name}
        </Typography>
      </Grid>
      <Grid item xs={12}>
        <TextField
          id="name"
          label="Name"
          // className={classes.textField}
          required={true}
          value={name}
          onChange={handleNameChange}
          margin="normal"
          error={!nameIsValid}
          helperText={name_helper_text}
        />
      </Grid>
      <Grid>
        <Grid item xs={6}>
          <MuiPickersUtilsProvider utils={DateFnsUtils}>
            <DateTimePicker
              label="Expiry date"
              value={expiryDate}
              onChange={setExpiryDate}
            />
          </MuiPickersUtilsProvider>
        </Grid>

        <TextField
          id="lifetime"
          label="Max. lifetime (mins)"
          // className={classes.textField}
          required={true}
          onChange={handleLifetimeChange}
          value={maxLifetime}
          margin="normal"
          error={!lifetimeIsValid}
          helperText={lifetimeHelperText}
        />
      </Grid>

      <Grid xs={12}>
        <RoleMembersPicker
          role_id={role.id}
          updateMembers={handleMembersChange}
        />
      </Grid>

      <Grid item xs={12}>
        {/* Scope picker */}
        <RoleScopePicker
          role_id={role.id}
          server_id={server_id}
          updateScopes={handleScopesChange}
          data-cy={"RoleScopePicker"}
        />
      </Grid>
      <ValidationDialog
        open={is_errored}
        message={errors}
        onClose={() => setIsErrored(false)}
      />

      <Grid item xs={12} />
      <SubmitButtons
        handleSubmit={handleSubmit}
        onClick={onClick}
        enabled={formIsValid}
      />
    </React.Fragment>
  );
}

function ValidationDialog(props) {
  const { open, onClose, message } = props;

  return (
    <Dialog
      open={open}
      onClose={onClose}
      aria-labelledby="error-dialog-title"
      aria-describedby="error-dialog-description"
      id="error-dialog"
      data-cy="error_dialog"
    >
      <DialogTitle id="error-dialog-title">{"Error"}</DialogTitle>
      <DialogContent>
        <DialogContentText id="error-dialog-description">
          {message}
        </DialogContentText>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} color="primary" id="error-dialog-ok">
          OK
        </Button>
      </DialogActions>
    </Dialog>
  );
}

export default RoleDetails;
