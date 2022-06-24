/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import { createRole } from "./util/api";
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

function RoleDetails(props) {
  //Properties:
  //item_id

  const {item_id, classes, onClick} = props
  
  const [role, setRole] = useState({})
  const [name, setRoleName] = useState("");
  const [server, setServer] = useState({})
  const [members, setMembers] = useState([]);
  const [edit_mode, setEditMode] = useState(false);
  const [name_helper_text, setNameHelperText] = useState("")
  const [errors, setErrors] = useState({message:""})
  const [is_errored, setIsErrored] = useState(false)
  const [pageErrored, setPageErrored] = useState(false)
  
  // get appropriate Role on load
  useEffect(
    () => {
      console.log(item_id);
      getRole(item_id)
      .then((role) => {
        console.log("Role fetched")
        console.log(role);
        setRole(role);
      })
      .catch((err) => {
        if (err.code !== 404){
          setRole({});
          setErrors(err.message)
          setIsErrored(true)
        }
      }
    )
  }, [])

  //When Role changes, replace role.name, role.server and role.members with 
  //the parts from the others.
  useEffect(() => {
    console.log("Trying to update the UI using the following role...")
    console.log(role)
    if (role == undefined){  // Pretty sure we want '==' and not '===' here
      setRoleName(role.name);
      setServer(role.server);
      setMembers(role.members);
    }
  }, [setRole])

  //Validate Rolename on change
  useEffect(() => {
    console.log("Name:" + name)
    var letters = /^[A-Za-z0-9_]+$/;
    if (name.match(letters)) {
      setNameHelperText("");
    } else if (name.length === 0) {
      setNameHelperText("Role name can not be blank.");
    } else {
      this.setState({
        name_helper_text:
          "Role name may only contain letters, numbers and underscores.",
      });
    }
  }, [setRoleName])

  //Throw error on error
  // useEffect((error) => {
  //   if (error !== {message:""}){
  //     throw error
  //   }
  // }, setErrors)

 const handleSubmit = async () => {
 //   const { name_helper_text, members, edit_mode, name }
 //   const { item_id, onClick } = this.props;

    if (name_helper_text === "") {
      const role = edit_mode
        ? renameRole(item_id, name)
        : createRole(name, []);
      try {
        await editRoleMembers(role.id, members);
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
          margin="normal"
          error={name_helper_text}
          helperText={name_helper_text}
        />
      </Grid>

      <Grid xs={12}>
        <Typography variant="h5" component="h1">
          Members
        </Typography>
      </Grid>
      <Grid xs={12}>
        <RoleMembersPicker
          role_id={item_id}
          updateMembers={setMembers}
        />
      </Grid>
      <ErrorDialog
        open={pageErrored}
        message={errors.message}
      />
      <Grid item xs={12} />
      <SubmitButtons handleSubmit={handleSubmit} onClick={onClick} />
    </React.Fragment>
  );
}

export default RoleDetails;
