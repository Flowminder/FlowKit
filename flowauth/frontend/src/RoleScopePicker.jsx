/* eslint-disable react/prop-types */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import {useEffect, useState} from "react"
import { Grid } from "rsuite";
import { getServerScopes, getRoleScopes } from "./util/api";
import { List, ListItem, Checkbox, ListItemIcon, ListItemText, Collapse, Button} from "@material-ui/core"
import { ExpandLess, ExpandMore} from "@material-ui/icons";
import { makeStyles } from "@material-ui/core/styles"
//import {scopesGraph, jsonify, highest_common_roots} from "./util/util"

const useStyles = makeStyles((theme) => ({
  ".MuiListItem-root": {
    "flex-direction": "column",
    "align-items": "self-start"
  },
  ".CollapsibleScope":{
    "display":"flex"
  },
  ".MuiSvgIcon-root": {
    "margin":"4px 0"
  }
}));

function ScopeItem(props) {
  var {scope_name, init_check, onChangeCallback} = props

  const [isChecked, setIsChecked] = useState(init_check)

  const flip_check = () => {
    setIsChecked(!isChecked)
  }

  useEffect(() => {
    onChangeCallback(scope_name)
  }, [isChecked])

  return <ListItem key={scope_name} value={isChecked} onClick={flip_check}>
    <ListItemIcon>
      <Checkbox checked ={isChecked} />
    </ListItemIcon>
    <ListItemText primary={scope_name} />
  </ListItem>
}


function NestedScopeList(props) {
  const classes = useStyles()
  const {outer_scope, inner_scopes, onChangeCallback} = props
  const [open, setOpen] = useState(false)
  const handleClick = () =>{
    setOpen(!open)
  }

  const newCallback = (inner_scope) => {
    onChangeCallback(outer_scope + ":" + inner_scope)
  }

  return <ListItem  className={classes[".MuiListItem-root"]}>
    <div className={classes[".CollapsibleScope"]}>
      <ListItemText primary = {outer_scope} >
      </ListItemText>
      <Button onClick={handleClick}>
        {open ? <ExpandLess className={classes[".MuiSvgIcon-root"]} /> : <ExpandMore className={classes[".MuiSvgIcon-root"]} />}
      </Button>
      
    </div>
    <Collapse in={open}>
      <ScopeList scopes = {inner_scopes} onChangeCallback = {newCallback}/>
    </Collapse>

  </ListItem>
}


function ScopeList (props) {
  const classes = useStyles();
  const {scopes, onChangeCallback} = props
  const [flatScopes, setFlatScopes] = useState([])
  const [nestedScopes, setNestedScopes] = useState([])
  // const flat_scopes 
  // const nested_scopes
  useEffect(() => {
    setFlatScopes(scopes.filter(s => !s.name.includes(":")))

    // This mess takes every scope that is 
    // {name:scope1:scope2, enabled:true}, {name:scope1:scope3, enabled:false}
    // and turns it into 
    // {outer_scope : scope1, inner_scopes[{name:scope2, enabled:true}, {name:scope3, enabled:false}]}
    const complex_scopes = scopes.filter(s => s.name.includes(":"))
    // If Array.prototype.group() existed it would be ideal here...
    const tl_scopes = [...new Set(complex_scopes.map(s => s.name.split(":")[0]))]
    const nested_scopes = tl_scopes.map(
      ts => new Object({"outer_scope":ts,
        "inner_scopes":complex_scopes
          .filter(cs => cs.name.startsWith(ts))
          .map(cs => new Object({
            "name": cs.name.replace(ts, "").replace(":", ""),
            "enabled": cs.enabled
          }))
      }))
    setNestedScopes(nested_scopes)
  }, [scopes])

  return <List 
    disablePadding
    className = {classes[".MuiListItem-root"]}>
    {flatScopes.map(
      scope => <ScopeItem 
        scope_name = {scope.name}
        key={scope.name}
        init_checked={scope.enabled}
        onChangeCallback={onChangeCallback}
      />
    )}
    {nestedScopes.map( scope =>
      <NestedScopeList outer_scope = {scope.outer_scope} inner_scopes={scope.inner_scopes} key={scope.name} onChangeCallback={onChangeCallback}/>
    )}
  </List>
}


// Component for picking scopes for a role
function RoleScopePicker (props) {

  const {role_id, server_id, updateScopes} = props
  const [checkedScopes, setCheckedScopes] = useState([])
  const [hasError, setHasError] = useState(false)
  const [error, setError] = useState({})

  //Initial setup
  useEffect(
    () => {
      const fetch_scopes = async () => {
        const role_scopes = await getRoleScopes(role_id)
        const server_scopes = await getServerScopes(server_id)
        const checked_scopes = server_scopes.map(
          srv_scope => new Object({
            "name":srv_scope.name,
            "enabled":role_scopes.map(y => y.name).includes(srv_scope.name)})
        )
        setCheckedScopes(checked_scopes)
      }
      
      fetch_scopes().catch((err) => console.error(err))
      // This needs to be cancellable
    }, []
  )

  const onChangeCallback = (changed_scopes) => {
    const new_scopes = checkedScopes.map(s => changed_scopes.includes(s.name) ? {"name":s.name, "enabled":!s.enabled} : s)
    console.debug(new_scopes)
    setCheckedScopes(new_scopes)
  }

  useEffect(() => updateScopes(checkedScopes), [checkedScopes])

  return <ScopeList scopes = {checkedScopes} onChangeCallback = {onChangeCallback} />
}


export default RoleScopePicker
