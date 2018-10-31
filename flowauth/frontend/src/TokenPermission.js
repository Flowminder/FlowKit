/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import FormLabel from "@material-ui/core/FormLabel";
import FormControl from "@material-ui/core/FormControl";
import FormGroup from "@material-ui/core/FormGroup";
import FormControlLabel from "@material-ui/core/FormControlLabel";
import Checkbox from "@material-ui/core/Checkbox";
import Grid from "@material-ui/core/Grid";

const styles = theme => ({
  root: {
    display: "flex"
  },
  formControl: {
    margin: theme.spacing.unit * 3
  }
});

class TokenPermission extends React.Component {
  handleChange = name => event => {
    this.setState({ [name]: event.target.checked });
  };

  render() {
    const {
      classes,
      claim,
      permissions,
      checkedHandler,
      permitted
    } = this.props;

    return (
      <Grid item xs>
        <FormControl component="fieldset" className={classes.formControl}>
          <FormLabel component="legend">{claim}</FormLabel>
          <FormGroup>
            {Object.keys(permissions).map(key => (
              <FormControlLabel
                control={
                  <Checkbox
                    checked={permissions[key]}
                    onChange={checkedHandler(claim, key)}
                    value={claim + ":" + key}
                    color="primary"
                  />
                }
                disabled={!permitted[key]}
                label={key}
              />
            ))}
          </FormGroup>
        </FormControl>
      </Grid>
    );
  }
}

TokenPermission.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(TokenPermission);
