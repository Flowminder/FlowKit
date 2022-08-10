/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import {useEffect, useState} from "react"
import { getServerScopes, getRoleScopes } from "./util/api";
import RightsCascade from "./RightsCascade";
import {scopesGraph, jsonify} from "./util/util"

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
    const fetch_role_scopes =(async () => {
      console.log("Fetching role scopes...")
      const role_scopes = await getRoleScopes(role_id);
      console.log("Role scopes fetched: ", role_scopes)
      setRoleScopes(role_scopes);
      
      // See mega-comment in handleChange for the inverse of this
      setSelectedRights(role_scopes.map(x => x.name))
    })
    const fetch_server_scopes = (async() => {
      console.log("Fetching server scopes....")
      const server_scopes = await getServerScopes(server_id);
      console.log("Server scopes fetched: ", server_scopes)
      setServerScopes(server_scopes)
    })
    
     if (server_id >= 0){
       fetch_role_scopes()
       .catch((err) => {
         if (err.code === 404) {
           setRoleScopes([]);
          } else {
            throw err;
          }
        })
        fetch_server_scopes()
        .catch((err) => {
          if (err.code === 404) {
            setServerScopes([]);
          } else {
            throw err;
          }
      })
  }
  }, [role_id, server_id])

  //On scopes change, update the bits to be fed to RightsCascade
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
    // new_labels contains only admin3. Fix by modifying the includes function below.
    
    setSelectedRights(checked_roles)
    setRoleScopes(serverScopes.filter(
      s_scope => checked_roles.some(
        c_role => s_scope.name.startsWith(c_role)
      )
    ))
    // setRoleScopes(serverScopes.filter(x => (
    //   x.name.startswith(new_label) for new_label in new_labels)))
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
