/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

function getCookieValue(a) {
  var b = document.cookie.match("(^|;)\\s*" + a + "\\s*=\\s*([^;]+)");
  return b ? b.pop() : "";
}

function APIError(message, code) {
  this.message = message;
  this.code = code;
  this.name = "APIError";
}

APIError.prototype = new Error();

export async function getUsers() {
  var err;
  var response = await fetch("/admin/users", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getUser(user_id) {
  var err;
  var response = await fetch("/admin/users/" + user_id, {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}
export async function getUserGroup(user_id) {
  var err;
  var response = await fetch("/admin/users/" + user_id + "/user_group", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function editPassword(oldPassword, newPassword) {
  var dat = {
    method: "PATCH",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      password: oldPassword,
      newPassword: newPassword
    })
  };

  var err;
  var response = await fetch("/user/password", dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("Trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function editUser(user_id, username, password, is_admin) {
  var dat = {
    method: "PATCH",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      username: username,
      password: password,
      is_admin: is_admin
    })
  };

  var err;
  var response = await fetch("/admin/users/" + user_id, dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("Trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function deleteAggregationUnit(unit_id) {
  var dat = {
    method: "DELETE",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    }
  };

  var err;
  var response = await fetch("/spatial_aggregation/" + unit_id, dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function newAggregationUnit(name) {
  var dat = {
    method: "POST",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ name: name })
  };

  var err;
  var response = await fetch("/spatial_aggregation", dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function deleteCapability(capability_id) {
  var dat = {
    method: "DELETE",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    }
  };

  var err;
  var response = await fetch("/admin/capabilities/" + capability_id, dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function newCapability(name) {
  var dat = {
    method: "POST",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ name: name })
  };

  var err;
  var response = await fetch("/admin/capabilities", dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function editGroupMemberships(user_id, group_ids) {
  var dat = {
    method: "PATCH",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ groups: group_ids })
  };

  var err;
  var response = await fetch("/admin/users/" + user_id + "/groups", dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getGroups() {
  var err;
  var response = await fetch("/admin/groups", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getGroupServers(group_id) {
  var err;
  var response = await fetch("/admin/groups/" + group_id + "/servers", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getServers() {
  var err;
  var response = await fetch("/admin/servers", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getServer(server_id) {
  var err;
  var response = await fetch("/admin/servers/" + server_id, {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getGroup(group_id) {
  var err;
  var response = await fetch("/admin/groups/" + group_id, {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getGroupMembers(group_id) {
  var err;
  var response = await fetch("/admin/groups/" + group_id + "/members", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getGroupsForUser(user_id) {
  var err;
  var response = await fetch("/admin/users/" + user_id + "/groups", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getMyServers() {
  var err;
  var response = await fetch("/user/servers", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getMyTokensForServer(server_id) {
  var err;
  var response = await fetch("/user/tokens/" + server_id, {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getMyRightsForServer(server_id) {
  var err;
  var response = await fetch("/user/servers/" + server_id, {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getCapabilities(server_id) {
  var err;
  var response = await fetch("/admin/servers/" + server_id + "/capabilities", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getGroupCapabilities(server_id, group_id) {
  var err;
  var response = await fetch(
    "/admin/groups/" + group_id + "/servers/" + server_id + "/capabilities",
    {
      credentials: "same-origin",
      headers: {
        "X-CSRF-Token": getCookieValue("X-CSRF")
      }
    }
  );
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getGroupTimeLimits(server_id, group_id) {
  var err;
  var response = await fetch(
    "/admin/groups/" + group_id + "/servers/" + server_id + "/time_limits",
    {
      credentials: "same-origin",
      headers: {
        "X-CSRF-Token": getCookieValue("X-CSRF")
      }
    }
  );
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getAllCapabilities() {
  var err;
  var response = await fetch("/admin/capabilities", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getAllAggregationUnits() {
  var err;
  var response = await fetch("/spatial_aggregation", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function getTimeLimits(server_id) {
  var err;
  var response = await fetch("/admin/servers/" + server_id + "/time_limits", {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF")
    }
  });
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function createUser(username, password, is_admin) {
  var dat = {
    method: "POST",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      username: username,
      password: password,
      is_admin: is_admin
    })
  };

  var err;
  var response = await fetch("/admin/users", dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function deleteUser(user_id) {
  var dat = {
    method: "DELETE",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    }
  };

  var err;
  var response = await fetch("/admin/users/" + user_id, dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function editServerCapabilities(server_id, rights) {
  var dat = {
    method: "PATCH",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify(rights)
  };

  var err;
  var response = await fetch(
    "/admin/servers/" + server_id + "/capabilities",
    dat
  );
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function editServer(
  server_id,
  server_name,
  secret_key,
  latest_token_expiry,
  longest_token_life
) {
  var dat = {
    method: "PATCH",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      name: server_name,
      secret_key: secret_key,
      latest_token_expiry: latest_token_expiry,
      longest_token_life: longest_token_life
    })
  };

  var err;
  var response = await fetch("/admin/servers/" + server_id, dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function createServer(
  server_name,
  secret_key,
  latest_token_expiry,
  longest_token_life
) {
  var dat = {
    method: "POST",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      name: server_name,
      secret_key: secret_key,
      latest_token_expiry: latest_token_expiry,
      longest_token_life: longest_token_life
    })
  };

  var err;
  var response = await fetch("/admin/servers", dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function deleteServer(server_id) {
  var dat = {
    method: "DELETE",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    }
  };

  var err;
  var response = await fetch("/admin/servers/" + server_id, dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function editMembers(group_id, member_ids) {
  var dat = {
    method: "PATCH",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ members: member_ids })
  };

  var err;
  var response = await fetch("/admin/groups/" + group_id + "/members", dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function editGroupServers(group_id, servers) {
  var dat = {
    method: "PATCH",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ servers: servers })
  };

  var err;
  var response = await fetch("/admin/groups/" + group_id + "/servers", dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function renameGroup(group_id, group_name) {
  var dat = {
    method: "PATCH",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ name: group_name })
  };

  var err;
  var response = await fetch("/admin/groups/" + group_id, dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function deleteGroup(group_id) {
  var dat = {
    method: "DELETE",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    }
  };

  var err;
  var response = await fetch("/admin/groups/" + group_id, dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function createGroup(group_name) {
  var dat = {
    method: "POST",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ name: group_name })
  };

  var err;
  var response = await fetch("/admin/groups", dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function createToken(name, server_id, expiry, claims) {
  var dat = {
    method: "POST",
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ name: name, expiry: expiry, claims: claims })
  };

  var err;
  var response = await fetch("/user/tokens/" + server_id, dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function login(username, password) {
  var dat = {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    credentials: "same-origin",
    body: JSON.stringify({ username: username, password: password })
  };

  var err;
  var response = await fetch("/signin", dat);
  if (response.ok) {
    return await response.json();
  } else {
    try {
      console.log("trying to throw..");
      err = await response.json();
    } catch (err) {
      console.log(err);
      throw new APIError(response.statusText, response.status);
    }
    throw err;
  }
}

export async function logout() {
  await fetch("/signout", {
    credentials: "same-origin"
  });
}
