/* eslint-disable react/prop-types */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import {useEffect, useState} from "react"
import { Grid } from "rsuite";
import { getServerScopes, getRoleScopes } from "./util/api";
import { List, ListItem, Checkbox, ListItemIcon, ListItemText} from "@material-ui/core"
//import {scopesGraph, jsonify, highest_common_roots} from "./util/util"

function ScopeItem(props) {
  const {scope_name, is_checked} = props
  return <ListItem key = {scope_name} role={undefined} value={is_checked} onClick={undefined}>
    <ListItemIcon>
      <Checkbox checked ={is_checked} />
    </ListItemIcon>
    <ListItemText primary={scope_name} />
  </ListItem>
}

function ScopeList (props) {
  const {scopes} = props
  const [flatScopes, setFlatScopes] = useState([])
  const [nestedScopes, setNestedScopes] = useState([])
  // const flat_scopes 
  // const nested_scopes
  useEffect(() => {
    console.debug("Scopes", scopes)
    setFlatScopes(scopes.filter(s => !s.name.includes(":")))
    setNestedScopes(scopes.filter(s => s.name.includes(":")))
  }, [scopes])

  return <List>
    {flatScopes.map(
      scope => <ScopeItem scope_name = {scope.name} key={scope.name} is_checked={scope.enabled}/>
    )}
    {/* map<ScopeList scopes={nested_scopes} /> */}
  </List>
}

// Component for picking scopes for a role
function RoleScopePicker (props) {
  const {role_id, server_id, updateScopes} = props
  const [roleScopes, setRoleScopes] = useState([])
  const [serverScopes, setServerScopes] = useState([])
  const [checkedScopes, setCheckedScopes] = useState([])
  const [rightsChoices, setRightsChoices] = useState({})
  const [selectedRights, setSelectedRights] = useState([])
  const [hasError, setHasError] = useState(false)
  const [error, setError] = useState({})

  useEffect(
    () => {
      const fetch_scopes = async () => {
        const role_scopes = await getRoleScopes(role_id)
        const server_scopes = await getServerScopes(server_id)
        const checked_scopes = server_scopes.map(
          srv_scp => new Object({
            "name":srv_scp.name,
            "enabled":role_scopes.map(y => y.name).includes(srv_scp.name)})
        )
        setCheckedScopes(checked_scopes)
      }
      
      fetch_scopes().catch((err) => console.error(err))
      // This needs to be cancellable
    }, []
  )
  
  return <ScopeList scopes = {checkedScopes}></ScopeList>

}


export default RoleScopePicker
