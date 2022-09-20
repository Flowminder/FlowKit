/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import RoleMembersPicker from "./RoleMembersPicker";
import SubmitButtons from "./SubmitButtons";
import ErrorDialog from "./ErrorDialog";
import {
  createRole,
  getServers,
  getServer,
  getRole,
  editRole,
} from "./util/api"
import { useEffect, useState } from "react";
import { DateTimePicker, MuiPickersUtilsProvider } from "@material-ui/pickers";
import DateFnsUtils from "@date-io/date-fns";
import RoleScopePicker from "./RoleScopePicker";
import { FormControl, MenuItem, Select, ListItem, InputLabel } from "@material-ui/core";

function RoleDetails(props) {
  //Properties:
  //item_id

  const {item_id, classes, onClick} = props
  
  const [role, setRole] = useState({})
  const [name, setRoleName] = useState("");
  const [server_id, setServer] = useState(-1);
  const [members, setMembers] = useState([]);
  const [edit_mode, setEditMode] = useState(false);
  const [name_helper_text, setNameHelperText] = useState("");
  const [nameIsValid, setNameIsValid] = useState(true);
  const [errors, setErrors] = useState({message:""});
  const [is_errored, setIsErrored] = useState(false);
  const [expiryDate, setExpiryDate] = useState(new Date());
  const [maxLifetime, setMaxLifetime] = useState("")
  const [lifetimeHelperText, setLifetimeHelperText] = useState("")
  const [lifetimeIsValid, setLifetimeIsValid] = useState(true)
  const [scopes, setScopes] = useState([])
  const [serverList, setServerList] = useState([])
  const [formIsValid, setFormIsValid] = useState(false)

  const validation_vars = [nameIsValid, lifetimeIsValid, scopes, members, server_id]
  
  // get appropriate Role on load
  useEffect( 
    () => {

      const fetch_role = (async () => {
        const role = await getRole(item_id);
        console.log("Role fetched");
        console.log(role);
        setRole(role);
      });

      const fetch_servers = (async () => {
        console.log("Fetching servers")
        const servers = await getServers();
        console.log("Servers:");
        console.log(servers);
        setServerList(servers);
      });
      
      fetch_servers()
      .catch((err) => {
        console.log("Server list error:" + err)
        if (err.code !== 404){
          setServerList([]);
          setErrors(err.message);
          setIsErrored(true)
        }
      })

      if (item_id >= 0){
        console.log(item_id);
        fetch_role()
        .catch((err) => {
          console.log("Role err:" + err)
          if (err.code !== 404){
            setRole({});
            setErrors(err.message);
            setIsErrored(true);
          }
        })
     }
    
  }, [])

  //When Role changes, replace role.name, role.server and role.members with 
  //the parts from the others.
  useEffect(() => {
      if (serverList !== []){
        console.log("Trying to update the UI using the following role...")
        console.log(role)
        if (Object.keys(role).length !== 0){   //ffs, Javascript
          console.log("Role not empty")
          setRoleName(role.name);
          setServer(role.server);
          setMembers(role.users);
          setExpiryDate(role.latest_token_expiry);
          setMaxLifetime(String(role.longest_token_life_minutes));
          setScopes(role.scopes)
          setEditMode(true);
        } else {
          console.log("Role empty, setting defaults")
          setRoleName("");
          setServer(-1);
          setMembers([]);
          setScopes([])
          setEditMode(false);
      }
    }
  }, [role, serverList])

  //When server_id changes, update the expiry date to the server's expiry date
  useEffect(() => {
    const fetch_server = (async () => {
      const server = getServer(server_id)
      setExpiryDate(server.expiry_date)
    })

    fetch_server()
    .catch((err) => {
      setErrors(err.message); 
      setIsErrored(true)
    })   
  }, [server_id])

  //Validate Rolename on change
  useEffect(() => {
    console.log("Name:" + name)
    var letters = /^[A-Za-z0-9_]+$/;
    if (name.match(letters)) {
      setNameHelperText("");
      setNameIsValid(true)
    } else if (name.length === 0) {
      setNameHelperText("Role name cannot be blank.");
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
    } else if (maxLifetime === "") {
      setLifetimeHelperText("Maximum lifetime cannot be blank");
      setLifetimeIsValid(false)
    } else {
      setLifetimeHelperText("Lifetime must be a number")
      setLifetimeIsValid(false)
    }
  }, [maxLifetime])

  //Make sure entire form is valid on change for any relevant members
  useEffect(() => {
    const valid_vars = validation_vars.map(v => typeof(v))
    if (valid_vars.includes("undefined")){
      setFormIsValid(false)
    } else if (nameIsValid && lifetimeIsValid && scopes.length > 0 && members.length > 0 && server_id >= 0){
      console.log("Form is valid")
      setFormIsValid(true)
    } else {
      setFormIsValid(false)
    }
  }, validation_vars)

  const handleNameChange = (event) => {
    setRoleName(event.target.value)
  }

  const handleLifetimeChange = (event) => {
    setMaxLifetime(event.target.value)
  }

  const handleServerChange = (event) => {
    const index = event.target.value
    setServer(index)
  }

  const handleMembersChange  = (new_members) => {
    setMembers(new_members.map(x => x.id))
  }

  const handleScopesChange = (new_scopes) => {
    setScopes(new_scopes)
  }

 //Either update or create a new role on 'submit' button
 const handleSubmit = async () => {
  console.log("Role form submitted")
      // Need to throw an error here if !formIsValid
    if (!formIsValid) {
      setIsErrored(true)
      setErrors(new Error("Uncaught form validation error; please report to Flowminder."))
    }
    else if (edit_mode){
      await editRole(
        role.id,
        name,
        scopes.map(s => s.id),
        members,
        expiryDate,
        maxLifetime)
      .catch((err)=>{
        setIsErrored(true)
        setErrors(err)
      })
    } else {
      await createRole(
        name,
        server_id,
        scopes.map(s => s.id),
        members,
        expiryDate,
        maxLifetime)
      .catch((err) => {
        setIsErrored(true)
        setErrors(err)
      })
    }
    onClick()
  };

console.log("Prerendering:")
console.log("server: ",server_id)
console.log("Server list: ",JSON.stringify(serverList))

return (
    <React.Fragment>

      <Grid item xs={12}>
        <Typography variant="h5" component="h1">
          {(edit_mode && "Edit Role") || "New Role"}
        </Typography>
      </Grid>
      <Grid item xs={12}>
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
          updateMembers={handleMembersChange}
        />
      </Grid>
      <Grid item xs={12}>

      {/* Server picker */}
      <FormControl disabled = {edit_mode}>
        <InputLabel id="server_picker">Server</InputLabel>
        <Select 
          labelId="server_label"
          options={serverList.map(this_server => this_server.id)}
          id="server" 
          value={server_id}
          label="Server"
          onChange={handleServerChange}
          // native={false}
        >
          <MenuItem label={-1} key={-1} value={-1}>-</MenuItem>
          {serverList.map( (this_server)=> {
            return <MenuItem label={this_server.id} key={this_server.id} value={this_server.id}>{this_server.name}</MenuItem>
          })}
        </Select>
      </FormControl>
      </Grid>

      <Grid item xs={12}>
      {/* Scope picker */} 
        <RoleScopePicker
          role_id={role.id}
          server_id={server_id}
          updateScopes={handleScopesChange}
        />
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
