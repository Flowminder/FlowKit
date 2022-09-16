/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import {useEffect, useState} from "react"
import { getRoles, getUserRoles } from "./util/api";
import Picker from "./Picker";

// Component for picking scopes for a role
function UserRolesPicker (props) {
  const {user_id, updateRoles} = props

  const [userRoles, setUserRoles] = useState([])
  const [allRoles, setAllRoles] = useState([])
  const [hasError, setHasError] = useState(false)
  const [error, setError] = useState({})

  useEffect(() => {

    const fetch_all_roles = (async() => {
      console.log("Fetching all roles....")
      const all_roles = await getRoles();
      console.log("All roles fetched: ", all_roles)
      setAllRoles(all_roles)
    })
  
    fetch_all_roles()
    .catch((err) => {
      if (err.code === 404) {
        setAllRoles([]);
      } else {
        throw err;
      }
    })
  }, [])

  // When selectedRoles is modified, fetch and setup user roles
  useEffect(() => {
    const fetch_user_roles =(async () => {
      console.log("Fetching user roles...")
      const user_roles = await getUserRoles(user_id);
      console.log("User roles fetched: ", user_roles)
      setUserRoles(user_roles);
    })
    
    fetch_user_roles()
    .catch((err) => {
      if (err.code === 404) {
        setUserRoles([]);
      } else {
        throw err;
      }
    })
    
  }, [allRoles, user_id])


  const handleChange = (event) => {
    setUserRoles(event.target.value)
    updateRoles(event.target.value);
  };

  return (
    <Picker
      objs={userRoles}
      all_objs={allRoles}
      hasError={hasError}
      error={error}
      handleChange={handleChange}
      label={"Roles"}
    />
  );
}

export default UserRolesPicker
