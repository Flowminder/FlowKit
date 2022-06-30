/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import {useEffect, useState} from "react"
import { getServerScopes, getRoleScopes } from "./util/api";
import Picker from "./Picker";

// Component for picking scopes for a role
function RoleScopePicker (props) {
  const {role_id, server_id, updateScopes} = props

  const [roleScopes, setRoleScopes] = useState([])
  const [serverScopes, setServerScopes] = useState([])
  const [hasError, setHasError] = useState(false)
  const [error, setError] = useState({})

  useEffect(() => {
    const fetch_role_scopes =(async () => {
      console.log("Fetching role scopes...")
      const role_scopes = await getRoleScopes(role_id);
      console.log("Role scopes fetched: ", role_scopes)
      setRoleScopes(role_scopes);
    })
    const fetch_server_scopes = (async() => {
      console.log("Fetching server scopes....")
      const server_scopes = await getServerScopes(server_id);
      console.log("Server scopes fetched: ", server_scopes)
      setServerScopes(server_scopes)
    })
    
     if (role_id >= 0 && server_id >= 0){
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

  const handleChange = (event) => {
    setRoleScopes(event.target.value)
    updateScopes(event.target.value);
  };

  return (
    <Picker
      objs={roleScopes}
      all_objs={serverScopes}
      hasError={hasError}
      error={error}
      handleChange={handleChange}
      label={"Scopes"}
    />
  );
}

export default RoleScopePicker
