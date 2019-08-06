/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import { withStyles } from "@material-ui/core/styles";
import Paper from "@material-ui/core/Paper";
import Typography from "@material-ui/core/Typography";
import { getVersion } from "./util/api";
import PropTypes from "prop-types";

const styles = theme => ({
  root: {
    position: "absolute",
    display: "block",
    bottom: 0,
    right: 0,
    opacity: 0.8,
    zIndex: 1300,
    padding: 2
  }
});

class Version extends React.Component {
  state = {
    version: null
  };

  async componentDidMount() {
    this.setState(await getVersion());
  }

  render() {
    const { classes } = this.props;
    const { version } = this.state;

    return (
      <>
        <Paper className={classes.root} id="flowauth_version">
          <Typography variant="body2" gutterBottom>
            FlowAuth v{version}
          </Typography>
        </Paper>
      </>
    );
  }
}

Version.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(Version);
