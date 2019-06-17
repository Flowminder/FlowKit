/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import { getTwoFactorBackups } from "./util/api";
import ErrorDialog from "./ErrorDialog";
import BackupCodes from "./BackupCodes";

class GenerateBackupCodes extends React.Component {
  state = {
    backup_codes: [],
    hasError: false,
    pageError: false,
    errors: {}
  };
  async componentDidMount() {
    try {
      this.setState({ backup_codes: await getTwoFactorBackups() });
    } catch (err) {
      if (err.code !== 404) {
        this.setState({ hasError: true, error: err });
      }
    }
  }

  render() {
    const { classes, cancel, advance } = this.props;
    if (this.state.hasError) throw this.state.error;

    const { backup_codes } = this.state;
    return (
      <>
        <BackupCodes
          advance={advance}
          backup_codes={backup_codes}
          cancel={cancel}
        />
        <ErrorDialog
          open={this.state.pageError}
          message={this.state.errors.message}
        />
      </>
    );
  }
}

export default GenerateBackupCodes;
