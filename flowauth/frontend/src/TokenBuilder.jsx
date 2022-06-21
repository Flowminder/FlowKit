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
import UserScopesList from "./UserScopesList";
import { getDisabledState } from "rsuite/esm/CheckTreePicker/utils";
import { Button, Dialog, DialogActions, DialogContentText, DialogTitle } from "@material-ui/core";
import ScopedCssBaseline from "@material-ui/core/ScopedCssBaseline";
import { scopes_with_roles } from "./util/util";
import {createToken, getUserRoles} from "./util/api"

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  }
})
function TokenBuilder(props) {

  const [selectedDate, handleDateChange] = useState(new Date());
  const {serverID} = props
  const [scopes, setScopeState] = useState([])
  const [activeScopes, setActiveScopes] = useState([])
  const [checked, setChecked] = useState([])
  const [tokenErrorOpen, setTokenErrorOpen] = useState(false)
  const [token, setToken] = useState("")
  const [tokenError, setTokenError] = useState("")

  // Run on initial load to get roles and transform to scopes
  useEffect(() => {
    getUserRoles(serverID)
    .then((roles) => {
      setScopeState(scopes_with_roles(roles));
    }, (err) => console.log(err))
  }, [])

  // Run when setChecked is updated to make sure that this is reflected in activeScopes
  useEffect(() => {
    setActiveScopes(checked.map((scope_index) => scopes[scope_index]))
    console.log("Active scopes now:")
    console.log(activeScopes)
  }, [checked])


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
    console.log("Requesting scopes:")
    console.log(activeScopes)
    createToken(
      "foo",
      serverID,
      selectedDate,
      activeScopes
    ).then((token) => {
      console.log("Token got");
      console.log(token)
      setToken(token.token)
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
    for (i = 0; i < scopes.length; i++){
      all_checked.push(i)
    }
    setChecked(all_checked)
  }

  //Handles the token error box being closed
  const closeTokenError = () => {
    setTokenErrorOpen(false)
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

      <Grid container xs={8}>
        <UserScopesList
          scopes = {scopes}
          checkAll = {checkAll}
          handleToggle = {handleToggle}
          checked = {checked}
          setScopeState = {setScopeState}
        />

        <Stack>
          <MuiPickersUtilsProvider utils={DateFnsUtils}>
            <DateTimePicker
              label="Expiry date"
              value = {selectedDate}
              onChange={handleDateChange}
            />
          </MuiPickersUtilsProvider> 
          <Button
            onClick = {requestToken}
          >
            Get token
          </Button>
        </Stack>
      </Grid>
      <TextField
        label='token'
        fullWidth
        multiline
        value={token}
      />

    </Fragment>
  );
}

export default withStyles(styles)(TokenBuilder)