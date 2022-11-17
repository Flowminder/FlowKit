/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/* eslint-disable react/prop-types */

import React from "react";
import {useEffect, useState} from "react"
import { getServerScopes, getRoleScopes } from "./util/api";
import { List, ListItem, Checkbox, ListItemIcon, ListItemText, Collapse, Button} from "@material-ui/core"
import { ExpandLess, ExpandMore} from "@material-ui/icons";
import { makeStyles } from "@material-ui/core/styles"

const useStyles = makeStyles((theme) => ({
  ".MuiListItem-root": {
    "flex-direction": "column",
    "align-items": "self-start"
  },
  ".CollapsibleScope":{
    "display":"flex"
  },
  ".MuiSvgIcon-root": {
    "margin":"2px 0"
  },
  ".MuiTextItem-root": {
    "align-self":"center"
  }
}));


function ScopeItem(props) {
  const classes = useStyles()
  var {scope, init_check, flipScopeCallback} = props

  const [isChecked, setIsChecked] = useState(init_check)

  // When an inner scope item is checked, use flipScopeCallback to
  // flip the scope with scope.key in the root of the component
  const onClick = () => {
    flipScopeCallback([scope.key], !scope.enabled)
  }

  useEffect(() => {
    setIsChecked(scope.enabled)
  }, [scope])

  return <ListItem 
    key={scope.key}
    value={scope.enabled} 
    onClick={onClick}
    data-cy={`scope-item-${scope.key}`}
  >
    <ListItemIcon>
      <Checkbox checked ={isChecked} className={classes[".MuiSvgIcon-root"]} data-cy={"checkbox"}/>
    </ListItemIcon>
    <ListItemText primary={scope.name} />
  </ListItem>
}


function NestedScopeList(props) {
  const classes = useStyles()
  const {outer_scope, inner_scopes, flipScopeCallback} = props
  const [open, setOpen] = useState(false) //remember to put this back to false
  const [isChecked, setIsChecked] = useState(false)
  const [isIndeterminant, setIsIndeterminant] = useState(false)

  useEffect(() => {
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
    flipScopeCallback(inner_scopes.map(s => s.key), is_checked) 
  }

  return <ListItem 
    className={classes[".MuiListItem-root"]}
    data-cy={`nested-${outer_scope}`}
  >
    <div className={classes[".CollapsibleScope"]}>
      <Checkbox 
        checked={isChecked}
        indeterminate={isIndeterminant}
        onClick={handleCheckboxClick} 
        data-cy={"checkbox"}
      />
      <ListItemText primary = {outer_scope} className={classes[".MuiTextItem-root"]} />
      <Button onClick={handleChevronClick} data-cy={"chevron"}>
        {open ? <ExpandLess className={classes[".MuiSvgIcon-root"]} /> : <ExpandMore className={classes[".MuiSvgIcon-root"]} />}
      </Button>
    </div>
    <Collapse in={open} unmountOnExit>
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
            "enabled": cs.enabled,
            "id": cs.id
          }))
      }))
    setNestedScopes(nested_scopes)
  }, [scopes])

  return <List 
    disablePadding
    className = {classes[".MuiListItem-root"]}
    data-cy = {`scope-list-${flatScopes[0] ? flatScopes[0].key : ""}`}
  >
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
        const role_scopes = await getRoleScopes(role_id).catch(err => {
          if (err.code === 404){
            return []
          }
        })
        const server_scopes = await getServerScopes(server_id)
        const checked_scopes = server_scopes.map(
          srv_scope => new Object({
            "name":srv_scope.name,
            "key":srv_scope.name,
            "enabled":role_scopes.map(y => y.name).includes(srv_scope.name),
            "id":srv_scope.id
          }))
        setCheckedScopes(checked_scopes)
      }
      
      fetch_scopes().catch((err) => console.error(err))

      return() => {}
      // This needs to be cancellable
    }, [role_id, server_id]
  )

  // Callback that flips all checkboxes
  const flipScopes = (changed_scope_names, enabled) => {
    console.debug(`Flipping ${changed_scope_names} to ${enabled} from RoleScopeCallback`)
    const new_scopes = checkedScopes.map(s => changed_scope_names.includes(s.name) ? {"name":s.name, "key":s.key, "enabled":enabled, "id":s.id} : s)
    setCheckedScopes(new_scopes)
  }

  useEffect(
    () => {
      updateScopes(checkedScopes)
    }, [checkedScopes]
  )

  return <ScopeList scopes = {checkedScopes} flipScopeCallback = {flipScopes} />
}

export default RoleScopePicker
