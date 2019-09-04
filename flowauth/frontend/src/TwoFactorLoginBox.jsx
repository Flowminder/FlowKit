/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import FormControl from "@material-ui/core/FormControl";
import InputLabel from "@material-ui/core/InputLabel";
import Input from "@material-ui/core/Input";
import React from "react";

function TwoFactorLoginBox(props) {
  const { two_factor_code, handleChange } = props;
  return (
    <FormControl margin="normal" required fullWidth>
      <InputLabel htmlFor="auth_code">Authorisation code</InputLabel>
      <Input
        name="two_factor_code"
        type="password"
        id="two_factor_code"
        value={two_factor_code}
        onChange={handleChange}
      />
    </FormControl>
  );
}

export default TwoFactorLoginBox;
