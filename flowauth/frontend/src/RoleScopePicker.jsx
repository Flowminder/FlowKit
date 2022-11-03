/* eslint-disable react/prop-types */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

// BUG: If you check a middle-level query, then check a leaf query in a 
// neighbouring branch, then the mid-query branch you selected first gets
// flipped

import React from "react";
import {useEffect, useState} from "react"
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
  var {scope, init_check, flipScopeCallback} = props

  const [isChecked, setIsChecked] = useState(init_check)

  useEffect(() => {
    console.debug(`Initial state of ${scope.name}; ${scope.enabled}`)
  },[])


  // When an inner scope item is checked, use flipScopeCallback to
  // flip the scope with scope.key in the root of the component
  const onClick = () => {
    flipScopeCallback(scope.key, !scope.enabled)
  }

  useEffect(() => {
    setIsChecked(scope.enabled)
  }, [scope])

  return <ListItem key={scope.key} value={scope.enabled} onClick={onClick}>
    <ListItemIcon>
      <Checkbox checked ={isChecked} />
    </ListItemIcon>
    <ListItemText primary={scope.name} />
  </ListItem>
}


function NestedScopeList(props) {
  const classes = useStyles()
  const {outer_scope, inner_scopes, flipScopeCallback} = props
  const [open, setOpen] = useState(true) //remember to put this back to false
  const [isChecked, setIsChecked] = useState(false)
  const [isIndeterminant, setIsIndeterminant] = useState(false)

  useEffect(() => {
    console.debug(`Inner scopes changed on ${outer_scope}`)
    console.debug(inner_scopes)
    if (inner_scopes.every(s => s.enabled === true)){
      setIsChecked(true)
      setIsIndeterminant(false)
    }
    else if (inner_scopes.every(s => s.enabled === false)){
      setIsChecked(false)
      setIsIndeterminant(false)
    }
    else {
      setIsChecked(true)
      setIsIndeterminant(true)
    }
  }, [inner_scopes])
  
  const handleChevronClick = () =>{
    setOpen(!open)
  }

  const handleCheckboxClick = () => {
    var is_checked
    if(isIndeterminant){
      setIsIndeterminant(false)
      is_checked = true
    } else {
      is_checked = !isChecked
    }
    inner_scopes.forEach(s => {
      flipScopeCallback(s.key, is_checked)
    })  
  }

  return <ListItem  className={classes[".MuiListItem-root"]}>
    <div className={classes[".CollapsibleScope"]}>
      <Checkbox checked={isChecked} indeterminate={isIndeterminant} onClick={handleCheckboxClick} />
      <ListItemText primary = {outer_scope} />
      <Button onClick={handleChevronClick}>
        {open ? <ExpandLess className={classes[".MuiSvgIcon-root"]} /> : <ExpandMore className={classes[".MuiSvgIcon-root"]} />}
      </Button>
    </div>
    <Collapse in={open}>
      <ScopeList scopes = {inner_scopes} flipScopeCallback={flipScopeCallback}/>
    </Collapse>
  </ListItem>
}


function ScopeList (props) {
  const classes = useStyles();
  const {scopes, flipScopeCallback} = props
  const [flatScopes, setFlatScopes] = useState([])
  const [nestedScopes, setNestedScopes] = useState([])

  useEffect(() => {
    const flat_scopes = scopes.filter(s => !s.name.includes(":"))
    setFlatScopes(flat_scopes)
    
    // This mess takes every scope that is 
    // {name: scope1:scope2, key: scope1:scope2 enabled:true}, {name: scope1:scope3, key: scope1:scope3, enabled:false}
    // and turns it into 
    // {outer_scope : scope1, inner_scopes[{name:scope2, key: scope1:scope2 enabled:true}, {name:scope3, key: scope1:scope3, enabled:false}]}
    const complex_scopes = scopes.filter(s => s.name.includes(":"))
    // If Array.prototype.group() existed it would be ideal here...
    const tl_scopes = [...new Set(complex_scopes.map(s => s.name.split(":")[0]))]
    const nested_scopes = tl_scopes.map(
      ts => new Object({"outer_scope":ts,
        "inner_scopes":complex_scopes
          .filter(cs => cs.name.startsWith(ts))
          .map(cs => new Object({
            "name": cs.name.replace(ts, "").replace(":", ""),
            "key": cs.key,
            "enabled": cs.enabled
          }))
      }))
    setNestedScopes(nested_scopes)
  }, [scopes])

  return <List disablePadding className = {classes[".MuiListItem-root"]}>
    {flatScopes.map(scope => 
      <ScopeItem 
        scope = {scope}
        key = {scope.key}
        flipScopeCallback={flipScopeCallback}
        init_check = {scope.enabled}
      />
    )}
    {nestedScopes.map( scope =>
      <NestedScopeList 
        outer_scope = {scope.outer_scope}
        inner_scopes = {scope.inner_scopes}
        key = {scope.outer_scope}
        flipScopeCallback = {flipScopeCallback}
      />
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
            "key":srv_scope.name,
            "enabled":role_scopes.map(y => y.name).includes(srv_scope.name)})
        )
        setCheckedScopes(checked_scopes)
      }
      
      fetch_scopes().catch((err) => console.error(err))
      // This needs to be cancellable
    }, []
  )

  // Callback that flips all checkboxes
  const flipScope = (changed_scope_name, enabled) => {
    console.debug(`Flipping ${changed_scope_name} to ${enabled} from RoleScopeCallback`)
    const new_scopes = checkedScopes.map(s => changed_scope_name === s.name ? {"name":s.name, "key":s.key, "enabled":enabled} : s)
    setCheckedScopes(new_scopes)
  }

  useEffect(
    () => {
      updateScopes(checkedScopes)
      console.debug("Checked scopes:", checkedScopes)
    }, [checkedScopes]
  )

  return <ScopeList scopes = {checkedScopes} flipScopeCallback = {flipScope} />
}

export default RoleScopePicker
