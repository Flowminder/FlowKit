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

async function getResponse(path, dat) {
  // Send data 'dat' to path 'path'.
  // Return response json if response is OK, otherwise throw error.
  var fullDat = {
    credentials: "same-origin",
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
      "Content-Type": "application/json",
    },
    ...dat,
  };
  var err;
  var response = await fetch(path, fullDat);
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

async function getResponseDefault(path) {
  // Same as 'getResponse', but with default data.
  var dat = {
    headers: {
      "X-CSRF-Token": getCookieValue("X-CSRF"),
    },
  };
  return await getResponse(path, dat);
}

export async function getUsers() {
  return await getResponseDefault("/admin/users");
}

export async function getPublicKey() {
  return await getResponseDefault("/admin/public_key");
}

export async function getUser(user_id) {
  return await getResponseDefault("/admin/users/" + user_id);
}

export async function getUserGroup(user_id) {
  return await getResponseDefault("/admin/users/" + user_id + "/tokens_group");
}

export async function editPassword(oldPassword, newPassword) {
  var dat = {
    method: "PATCH",
    body: JSON.stringify({
      password: oldPassword,
      newPassword: newPassword,
    }),
  };
  return await getResponse("/user/password", dat);
}

export async function editUser(
  user_id,
  username,
  password,
  is_admin,
  require_two_factor,
  has_two_factor
) {
  var dat = {
    method: "PATCH",
    body: JSON.stringify({
      username: username,
      password: password,
      is_admin: is_admin,
      require_two_factor: require_two_factor,
      has_two_factor: has_two_factor,
    }),
  };
  return await getResponse("/admin/users/" + user_id, dat);
}

export async function editGroupMemberships(user_id, group_ids) {
  var dat = {
    method: "PATCH",
    body: JSON.stringify({ groups: group_ids }),
  };
  return await getResponse("/admin/users/" + user_id + "/groups", dat);
}

export async function getGroups() {
  return await getResponseDefault("/admin/groups");
}

export async function getGroupServers(group_id) {
  return await getResponseDefault("/admin/groups/" + group_id + "/servers");
}

export async function getServers() {
  return await getResponseDefault("/admin/servers");
}

export async function getServer(server_id) {
  return await getResponseDefault("/admin/servers/" + server_id);
}

export async function getGroup(group_id) {
  return await getResponseDefault("/admin/groups/" + group_id);
}

export async function getGroupMembers(group_id) {
  return await getResponseDefault("/admin/groups/" + group_id + "/members");
}

export async function getGroupsForUser(user_id) {
  return await getResponseDefault("/admin/users/" + user_id + "/groups");
}

export async function getMyServers() {
  return await getResponseDefault("/tokens/servers");
}

export async function getMyTokensForServer(server_id) {
  return await getResponseDefault("/tokens/tokens/" + server_id);
}

export async function getMyRightsForServer(server_id) {
  return await getResponseDefault("/tokens/servers/" + server_id);
}

export async function getCapabilities(server_id) {
  return await getResponseDefault(
    "/admin/servers/" + server_id + "/capabilities"
  );
}

export async function getGroupCapabilities(server_id, group_id) {
  return await getResponseDefault(
    "/admin/groups/" + group_id + "/servers/" + server_id + "/capabilities"
  );
}

export async function getGroupTimeLimits(server_id, group_id) {
  return await getResponseDefault(
    "/admin/groups/" + group_id + "/servers/" + server_id + "/time_limits"
  );
}

export async function getAllCapabilities() {
  return await getResponseDefault("/admin/capabilities");
}

export async function getAllAggregationUnits() {
  return await getResponseDefault("/spatial_aggregation");
}

export async function getTimeLimits(server_id) {
  return await getResponseDefault(
    "/admin/servers/" + server_id + "/time_limits"
  );
}

export async function createUser(
  username,
  password,
  is_admin,
  require_two_factor
) {
  var dat = {
    method: "POST",
    body: JSON.stringify({
      username: username,
      password: password,
      is_admin: is_admin,
      require_two_factor: require_two_factor,
    }),
  };
  return await getResponse("/admin/users", dat);
}

export async function deleteUser(user_id) {
  var dat = {
    method: "DELETE",
  };
  return await getResponse("/admin/users/" + user_id, dat);
}

export async function editServerCapabilities(server_id, rights) {
  var dat = {
    method: "PATCH",
    body: JSON.stringify(rights),
  };
  return await getResponse(
    "/admin/servers/" + server_id + "/capabilities",
    dat
  );
}

export async function editServer(
  server_id,
  server_name,
  latest_token_expiry,
  longest_token_life_minutes
) {
  var dat = {
    method: "PATCH",
    body: JSON.stringify({
      name: server_name,
      latest_token_expiry: latest_token_expiry,
      longest_token_life_minutes: longest_token_life_minutes,
    }),
  };
  return await getResponse("/admin/servers/" + server_id, dat);
}

export async function createServer(
  server_name,
  latest_token_expiry,
  longest_token_life_minutes
) {
  var dat = {
    method: "POST",
    body: JSON.stringify({
      name: server_name,
      latest_token_expiry: latest_token_expiry,
      longest_token_life_minutes: longest_token_life_minutes,
    }),
  };
  return await getResponse("/admin/servers", dat);
}

export async function deleteServer(server_id) {
  var dat = {
    method: "DELETE",
  };
  return await getResponse("/admin/servers/" + server_id, dat);
}

export async function editMembers(group_id, member_ids) {
  var dat = {
    method: "PATCH",
    body: JSON.stringify({ members: member_ids }),
  };
  return await getResponse("/admin/groups/" + group_id + "/members", dat);
}

export async function editGroupServers(group_id, servers) {
  var dat = {
    method: "PATCH",
    body: JSON.stringify({ servers: servers }),
  };
  return await getResponse("/admin/groups/" + group_id + "/servers", dat);
}

export async function renameGroup(group_id, group_name) {
  var dat = {
    method: "PATCH",
    body: JSON.stringify({ name: group_name }),
  };
  return await getResponse("/admin/groups/" + group_id, dat);
}

export async function deleteGroup(group_id) {
  var dat = {
    method: "DELETE",
  };
  return await getResponse("/admin/groups/" + group_id, dat);
}

export async function createGroup(group_name) {
  var dat = {
    method: "POST",
    body: JSON.stringify({ name: group_name }),
  };
  return await getResponse("/admin/groups", dat);
}

export async function createToken(name, server_id, expiry, claims) {
  var dat = {
    method: "POST",
    body: JSON.stringify({ name: name, expiry: expiry, claims: claims }),
  };
  return await getResponse("/tokens/tokens/" + server_id, dat);
}

export async function login(username, password) {
  var dat = {
    method: "POST",
    body: JSON.stringify({ username: username, password: password }),
  };
  return await getResponse("/signin", dat);
}

export async function twoFactorlogin(username, password, two_factor_code) {
  var dat = {
    method: "POST",
    body: JSON.stringify({
      username: username,
      password: password,
      two_factor_code: two_factor_code,
    }),
  };
  return await getResponse("/signin", dat);
}

export async function startTwoFactorSetup() {
  var dat = {
    method: "POST",
  };
  return await getResponse("/user/enable_two_factor", dat);
}

export async function getTwoFactorBackups() {
  var dat = {
    method: "GET",
  };
  return await getResponse("/user/generate_two_factor_backups", dat);
}

export async function confirmTwoFactorBackups(backup_codes_signature) {
  var dat = {
    method: "POST",
    body: JSON.stringify({
      backup_codes_signature: backup_codes_signature,
    }),
  };
  return await getResponse("/user/generate_two_factor_backups", dat);
}

export async function confirmTwoFactor(
  two_factor_code,
  secret,
  backup_codes_signature
) {
  var dat = {
    method: "POST",
    body: JSON.stringify({
      two_factor_code: two_factor_code,
      secret: secret,
      backup_codes_signature: backup_codes_signature,
    }),
  };
  return await getResponse("/user/confirm_two_factor", dat);
}

export async function disableTwoFactor() {
  var dat = {
    method: "POST",
  };
  return await getResponse("/user/disable_two_factor", dat);
}

export async function isLoggedIn() {
  return await getResponseDefault("/is_signed_in");
}

export async function isTwoFactorActive() {
  return await getResponseDefault("/user/two_factor_active");
}

export async function isTwoFactorRequired() {
  return await getResponseDefault("/user/two_factor_required");
}

export async function getVersion() {
  return await getResponseDefault("/version");
}

export async function logout() {
  try {
    return await getResponseDefault("/signout");
  } catch (err) {
    if (err.code === 401) {
      // Being logged out is the desired response here
      return {};
    } else {
      throw err;
    }
  }
}
