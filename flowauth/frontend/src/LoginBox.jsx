/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import FormControl from "@material-ui/core/FormControl";
import InputLabel from "@material-ui/core/InputLabel";
import Input from "@material-ui/core/Input";
import React from "react";

function LoginBox(props) {
  const { username, password, usernameChangeHandler, passwordChangeHandler } =
    props;
  return (
    <>
      <FormControl margin="normal" required fullWidth>
        <InputLabel htmlFor="username">Username</InputLabel>
        <Input
          id="username"
          name="username"
          autoComplete="username"
          autoFocus
          value={username}
          onChange={usernameChangeHandler}
        />
      </FormControl>
      <FormControl margin="normal" required fullWidth>
        <InputLabel htmlFor="password">Password</InputLabel>
        <Input
          name="password"
          type="password"
          id="password"
          value={password}
          onChange={passwordChangeHandler}
          autoComplete="current-password"
        />
      </FormControl>
    </>
  );
}

export default LoginBox;
