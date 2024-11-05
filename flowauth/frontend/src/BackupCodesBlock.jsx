/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import React from "react";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core";
const styles = (theme) => ({
  button: {
    margin: theme.spacing.unit,
  },
  codeBlock: {
    fontFamily: "Consolas, Monaco, 'Andale Mono', 'Ubuntu Mono', monospace",
  },
});
class BackupCodesBlock extends React.Component {
  renderBackupCode = (code) => {
    return (
      <Grid item xs={6}>
        <Typography
          variant="body2"
          align="center"
          className={this.props.classes.codeBlock}
          data-cy="backup_code"
        >
          <span className="trailing_dash">
            {code.slice(0, code.length / 2)}
          </span>
          {code.slice(code.length / 2, code.length)}
        </Typography>
      </Grid>
    );
  };

  render() {
    const { backup_codes } = this.props;
    return (
      <Grid item xs={4} container direction="row">
        {backup_codes.map((code) => this.renderBackupCode(code))}
      </Grid>
    );
  }
}

BackupCodesBlock.propTypes = {
  classes: PropTypes.object.isRequired,
};
export default withStyles(styles)(BackupCodesBlock);
