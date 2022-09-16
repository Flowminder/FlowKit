/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import {useEffect, useState} from "react"
import { getServerScopes, getRoleScopes } from "./util/api";
import RightsCascade from "./RightsCascade";
import {scopesGraph, jsonify, highest_common_roots} from "./util/util"

// Component for picking scopes for a role
function RoleScopePicker (props) {
  const {role_id, server_id, updateScopes} = props
  const [roleScopes, setRoleScopes] = useState([])
  const [serverScopes, setServerScopes] = useState([])
  const [rightsChoices, setRightsChoices] = useState({})
  const [selectedRights, setSelectedRights] = useState([])
  const [hasError, setHasError] = useState(false)
  const [error, setError] = useState({})

  useEffect(() => {
    const fetch_role_scopes = async (server_scopes) => {
      console.log("Fetching role scopes...")
      const role_scopes = await getRoleScopes(role_id);
      console.log("Role scopes fetched: ", role_scopes)
      console.log("Server scopes are:", server_scopes)
      setRoleScopes(role_scopes);
      
      // selectedRights contains the list of names of selected scopes for this role.
      // The trick is that, in the case of nested scopes, selectedRights only contains
      // the shortest needed name.
      // For example, there are two scopes available, admin1:foo and admin1:bar.
      // If roleScopes contains both admin1:foo and admin1:bar, selectedRights should
      // only contain admin1 - presence of only that implies that all sub-scopes are selected as well.
      // On the other hand, if roleScopes only contains admin1:foo, selectedRights should also
      // only contain admin1:foo

      // Sorting this out is at present an exercise for the reader of this comment - please
      // send answers on a postcard to John Roberts, c.o. Institute for Care of Victims of Unregulated Javascript.

      const sel_rights = highest_common_roots(
        scopesGraph(role_scopes.map(x => x.name)),
        scopesGraph(server_scopes.map(x => x.name)))
      setSelectedRights(sel_rights)
    }

    const fetch_server_scopes = async() => {
      console.log("Fetching server scopes....")
      const server_scopes = await getServerScopes(server_id);
      console.log("Server scopes fetched: ", server_scopes)
      setServerScopes(server_scopes)
      return server_scopes
    }

    const set_initial_state = async () => {
      if (server_id >= 0){
        const server_scopes = await fetch_server_scopes()
        await fetch_role_scopes(server_scopes)
      }
    }

    set_initial_state().catch(err => console.log(err))
    
  }, [role_id, server_id])

  // On scopes change, jsonify the scopes into the appropriate format for
  // MultiCascader
  useEffect(() =>{
    if (serverScopes.length > 0){
      const unneeded = []
      const baz = scopesGraph(serverScopes.map(x => x.name))
      const foo = jsonify(baz, unneeded);
      setRightsChoices(foo)
    }

  }, [serverScopes])

  useEffect(() => {
    updateScopes(roleScopes)
  }, [roleScopes])
  
  const handleChange = (checked_roles) => {
    //'checked_roles' in this case is a list of _labels_ from RightsCascade's onChange
    // So we need to filter RoleScopes to all the scopes with that 'name' string
    // This is further complicated by RightsCascade...apparently only returning the root
    // label of a tree? This could be a case of it returning the lowest common root of a
    // selected node; so if there is _only_ admin3:dummy_query:dummy_query, and you select
    // admin3 -> dummy_query -> dummy_query, you have selected all _admin3_, therefore 
    // new_labels contains only admin3.
    
    setSelectedRights(checked_roles)
    setRoleScopes(serverScopes.filter(
      s_scope => checked_roles.some(
        c_role => s_scope.name.startsWith(c_role)
      )
    ))
  };

  return (
    <RightsCascade
      options={rightsChoices}
      value={selectedRights}
      onChange={handleChange}
      // label={"Scopes"}
    />
  );
}

export default RoleScopePicker
