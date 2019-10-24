/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import { MultiCascader } from "rsuite";
import "rsuite/dist/styles/rsuite-default.css";

class RightsCascade extends React.Component {
  render() {
    const { options, value, onChange } = this.props;
    if (options) {
      return (
        <MultiCascader
          block
          data={options}
          value={value}
          onChange={onChange}
          preventOverflow
          menuWidth={400}
        />
      );
    }
  }
}

export default RightsCascade;
