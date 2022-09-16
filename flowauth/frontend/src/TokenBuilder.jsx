/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React, {Fragment, useState, useEffect} from "react";
import Grid from "@material-ui/core/Grid";
import Stack from "@material-ui/core/Grid";
import TextField from "@material-ui/core/TextField";
import { DateTimePicker, MuiPickersUtilsProvider } from "@material-ui/pickers"
import DateFnsUtils from "@date-io/date-fns";
import Typography from "@material-ui/core/Typography";
import { withStyles } from "@material-ui/core/styles"
import UserRoleList from "./UserRoleList";
import { getDisabledState } from "rsuite/esm/CheckTreePicker/utils";
import { Button, Dialog, DialogActions, DialogContentText, DialogTitle } from "@material-ui/core";
import ScopedCssBaseline from "@material-ui/core/ScopedCssBaseline";
import { scopes_with_roles } from "./util/util";
import {createToken, getMyRolesOnServer} from "./util/api"
import UserRolesPicker from "./UserRolesPicker";
import SubmitButtons from "./SubmitButtons";
import CompoundChecklist from "./TokenRolesPicker";
import ScopeDetails from "./ScopeDetails";
import TokenRolesPicker from "./TokenRolesPicker";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  }
})


function TokenBuilder(props) {

  const {activeServer, onClick} = props
  
  const [name, setName] = useState("")
  const [nameHelperText, setNameHelperText] = useState("")
  const [nameIsValid, setNameIsValid] = useState("")
  const [roles, setRoleState] = useState([])
  const [activeRoles, setActiveRoles] = useState([])
  const [checked, setChecked] = useState([])
  const [tokenErrorOpen, setTokenErrorOpen] = useState(false)
  const [tokenError, setTokenError] = useState("")
  const [tokenWarningOpen, setTokenWarningOpen] = useState(false)

  // Run on initial load to get roles
  useEffect(() => {
    getMyRolesOnServer(activeServer)
    .then(roles => setRoleState(roles),
    (err) => console.log(err))
  }, [])

  // Run when setChecked is updated to make sure that this is reflected in activeScopes
  useEffect(() => {
    setActiveRoles(checked.map(i => roles[i]))
    console.log("Active roles now:")
    console.log(activeRoles)
  }, [checked])

  //Validates token name on change
  useEffect(() => {
    console.log("Name:" + name)
    var letters = /^[A-Za-z0-9_]+$/;
    if (name.match(letters)) {
      setNameHelperText("");
      setNameIsValid(true)
    } else if (name.length === 0) {
      setNameHelperText("Token name cannot be blank.");
      setNameIsValid(false)
    } else {
      setNameHelperText(
        "Token name may only contain letters, numbers and underscores.",
      )
      setNameIsValid(false)
    };
  }, [name])

  // Keeps track of which boxes are toggled
  const handleToggle = (value) => () => {
    const currentIndex = checked.indexOf(value);
    const newChecked = [...checked]
    console.log("box checked")
    if (currentIndex === -1) {
      newChecked.push(value)
    } else {
      newChecked.splice(currentIndex, 1)
    }
    setChecked(newChecked)
  }

  //Pops up a marquee containing the token for copy-and-paste or download
  const requestToken = () => {

    createToken(
      name,
      activeServer,
      activeRoles
    ).then((token) => {
      console.log("Token got");
      console.log(token)
      onClick()
    },(err) => {
      console.log("Token error")
      console.log(err)
      setTokenError(err.message)
      setTokenErrorOpen(true)
    })
  }

  //Checks all tickboxes in the scopes list
  const checkAll = () => {
    var i;
    var all_checked = [];
    for (i = 0; i < roles.length; i++){
      all_checked.push(i)
    }
    setChecked(all_checked)
  }

  //Handles form submission
  const submitForm = () => {
    if (activeRoles.length === 0){
      setTokenError("At least one role must be selected")
      setTokenErrorOpen(true)
    } else {
      requestToken()
    }
  }

  //Handles the token error box being closed
  const closeTokenError = () => {
    setTokenErrorOpen(false)
  }

  //Handles the token warning box being closed
  const closeTokenWarning = () => {
    setTokenWarningOpen(false)
  }

  //Handles the token name being changed
  const handleNameChange = (event) => {
    setName(event.target.value)
  }


  return (
    <Fragment>

      <Dialog
        open = {tokenErrorOpen}
        onClose = {closeTokenError}
      >
        <DialogTitle>
          Error
        </DialogTitle>
        <DialogContentText>
          {tokenError}
        </DialogContentText>
        <DialogActions>
          <Button 
          onClick={closeTokenError}
          autoFocus
          >
            Close
          </Button>
        </DialogActions>
      </Dialog>

      <TextField
        id="name"
        label="Name"
        required={true}
        onChange = {handleNameChange}
        value={name}
        margin="normal"
        error={!nameIsValid}
        helperText={nameHelperText}
      />
      <Grid container xs={8}>
        <TokenRolesPicker
          roles = {roles}
          detailsComponent = {ScopeDetails}
          checkAll = {checkAll}
          handleToggle = {handleToggle}
          checked = {checked}
        />
      </Grid>
      <SubmitButtons handleSubmit={submitForm} onClick={onClick} />


    </Fragment>
  );
}

export default withStyles(styles)(TokenBuilder)