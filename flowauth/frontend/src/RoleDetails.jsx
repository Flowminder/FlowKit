/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import { createRole, editRoleScopes, getServers } from "./util/api";
import Typography from "@material-ui/core/Typography";
import RoleMembersPicker from "./RoleMembersPicker";
import SubmitButtons from "./SubmitButtons";
import ErrorDialog from "./ErrorDialog";
import {
  getRole,
  editRoleMembers,
  editScopes,
  renameRole,
} from "./util/api"
import { useEffect, useState } from "react";
import { DateTimePicker, MuiPickersUtilsProvider } from "@material-ui/pickers";
import DateFnsUtils from "@date-io/date-fns";
import RoleScopePicker from "./RoleScopePicker";
import { Dropdown } from "rsuite";
import { FormControl, MenuItem, Select } from "@material-ui/core";

function RoleDetails(props) {
  //Properties:
  //item_id

  const {item_id, classes, onClick} = props
  
  const [role, setRole] = useState({})
  const [name, setRoleName] = useState("");
  const [server, setServer] = useState(-1);
  const [members, setMembers] = useState([]);
  const [edit_mode, setEditMode] = useState(false);
  const [name_helper_text, setNameHelperText] = useState("");
  const [nameIsValid, setNameIsValid] = useState(true);
  const [errors, setErrors] = useState({message:""});
  const [is_errored, setIsErrored] = useState(false);
  const [pageErrored, setPageErrored] = useState(false);
  const [expiryDate, setExpiryDate] = useState(new Date());
  const [maxLifetime, setMaxLifetime] = useState("")
  const [lifetimeHelperText, setLifetimeHelperText] = useState("")
  const [lifetimeIsValid, setLifetimeIsValid] = useState(true)
  const [scopes, setScopes] = useState([])
  const [serverList, setServerList] = useState([])
  
  // get appropriate Role on load
  useEffect( 
    () => {
      if (item_id >= 0){
        console.log(item_id);
        const fetch_role = async () => {
          const role = await getRole(item_id);
          console.log("Role fetched");
          console.log(role);
          setRole(role);
          const servers = await getServers();
          setServerList(servers);
        }

        fetch_role()
        .catch((err) => {
          if (err.code !== 404){
            console.log("Error:" + err)
            setRole({});
            setErrors(err.message)
            setIsErrored(true)
          }
        })
      }
    }, [])

  //When Role changes, replace role.name, role.server and role.members with 
  //the parts from the others.
  useEffect(() => {
    console.log("Trying to update the UI using the following role...")
    console.log(role)
    if (Object.keys(role).length !== 0){   //ffs, Javascript
      console.log("Role not empty")
      setRoleName(role.name);
      setServer(role.server);
      setMembers(role.members);
      setExpiryDate(role.latest_token_expiry);
      setMaxLifetime(String(role.longest_token_life_minutes));
      setEditMode(true);
    } else {
      console.log("Role empty, setting defaults")
      setRoleName("");
      setServer(-1)
      setMembers([])
      setEditMode(false)
    }
  }, [role])

  //Validate Rolename on change
  useEffect(() => {
    console.log("Name:" + name)
    var letters = /^[A-Za-z0-9_]+$/;
    if (name.match(letters)) {
      setNameHelperText("");
      setNameIsValid(true)
    } else if (name.length === 0) {
      setNameHelperText("Role name can not be blank.");
      setNameIsValid(false)
    } else {
      setNameHelperText(
        "Role name may only contain letters, numbers and underscores.",
      )
      setNameIsValid(false)
    };
  }, [name])

  //Validate lifetime on change
  useEffect(() => {
    console.log("New lifetime: " + maxLifetime)
    var numbers = /^[0-9]+$/;
    if (maxLifetime.match(numbers)){
      setLifetimeHelperText("");
      setLifetimeIsValid(true)
    } else if (maxLifetime == "") {
      setLifetimeHelperText("Maximum lifetime cannot be blank");
      setLifetimeIsValid(false)
    } else {
      setLifetimeHelperText("Lifetime must be a number")
      setLifetimeIsValid(false)
    }
  }, [maxLifetime])
  
  const handleNameChange = (event) => {
    console.log("Name change event handled")
    setRoleName(event.target.value)
  }

  const handleLifetimeChange = (event) => {
    console.log("Lifetime change event handled");
    setMaxLifetime(event.target.value)
  }

  // // Throw error on error
  // useEffect((error) => {
  //   if (error !== {message:""}){
  //     throw error
  //   }
  // }, setErrors)

 const handleSubmit = async () => {
 //   const { name_helper_text, members, edit_mode, name }
 //   const { item_id, onClick } = this.props;
  console.log("Role form submitted")
    if (nameIsValid && lifetimeIsValid) {
      const submitFunc = edit_mode
        ? () => renameRole(role.id, name)
        : () => createRole(name, server.id, [], []);
      submitFunc().then(
        editRoleMembers(role.id, role.members)
      ).then(editRoleScopes(role.id, scopes))
      .catch((err) => {
        console.log("Update errored")
        setErrors(err)
        if (err.code === 400) {
          setPageErrored(true)
        } else {
          setIsErrored(true)
        }
      })
    }
  };

return (
    <React.Fragment>
      <Grid xs={12}>
        <Typography variant="h5" component="h1">
          {(edit_mode && "Edit Role") || "New Role"}
        </Typography>
      </Grid>
      <Grid xs={12}>
        <TextField
          id="name"
          label="Name"
          className={classes.textField}
          required={true}
          value={name}
          onChange={handleNameChange}
          margin="normal"
          error={!nameIsValid}
          helperText={name_helper_text}
        />
      </Grid>
      <Grid xs={12}>
        <Typography variant="h5" component="h1">
          Members
        </Typography>
      </Grid>
      <MuiPickersUtilsProvider utils={DateFnsUtils}>
            <DateTimePicker
              label="Expiry date"
              value = {expiryDate}
              onChange={setExpiryDate}
            />
          </MuiPickersUtilsProvider> 
      
      <TextField
        id="lifetime"
        label="Maximum lifetime (minutes)"
        className={classes.textField}
        required={true}
        value={maxLifetime}
        onChange={handleLifetimeChange}
        margin="normal"
        error={!lifetimeIsValid}
        helperText={lifetimeHelperText}
      />
        
      <Grid xs={12}>
        <RoleMembersPicker
          role_id={item_id}
          updateMembers={setMembers}
        />
      </Grid>

      {/* Server picker */}
      <Grid xs={12}>
        <FormControl disabled={edit_mode}>
        <Select 
          labelId="server_label"
          id="server"
          value={server}
          label="server"
          onChange={setServer}
        >
          {
            serverList.map( (this_server) => {
                <MenuItem value={this_server.id}>{this_server.name}</MenuItem>
              }
            )
          }
        </Select>
        </FormControl>
      </Grid>

      {/* Scope picker */}
      <Grid xs={12}>
        <FormControl disabled={server===-1}>
        <RoleScopePicker
          role_id={role.id}
          server_id={role.server}
          updateScopes={setScopes}
        />
        </FormControl>
      </Grid>

      <ErrorDialog
        open={is_errored}
        message={errors.message}
      />

      <Grid item xs={12} />
      <SubmitButtons handleSubmit={handleSubmit} onClick={onClick} />
    </React.Fragment>
  );
}

export default RoleDetails;
