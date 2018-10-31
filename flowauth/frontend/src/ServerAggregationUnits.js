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
import { getAllAggregationUnits } from "./util/api";

const styles = theme => ({
  root: {
    display: "flex"
  },
  formControl: {
    margin: theme.spacing.unit * 3
  }
});

class ServerAggregationUnits extends React.Component {
  state = { all_agg_units: [] };

  componentDidMount() {
    getAllAggregationUnits()
      .then(json => {
        this.setState({ all_agg_units: json });
        console.log(json);
      })
      .catch(err => {
        this.setState({ hasError: true, error: err });
      });
  }

  render() {
    if (this.state.hasError) throw this.state.error;
    const { all_agg_units } = this.state;
    const {
      classes,
      claim_id,
      claim,
      units,
      checkedHandler,
      permitted
    } = this.props;

    return (
      <Grid item xs>
        <FormControl component="fieldset" className={classes.formControl}>
          <FormLabel component="legend">{claim}</FormLabel>
          <FormGroup>
            {all_agg_units.map(unit => (
              <FormControlLabel
                control={
                  <Checkbox
                    checked={units.indexOf(unit.name) !== -1}
                    onChange={checkedHandler(claim_id, claim, unit.name)}
                    value={claim + ":" + unit.name}
                    color="primary"
                    disabled={!permitted(claim, unit.name)}
                  />
                }
                label={unit.name}
              />
            ))}
          </FormGroup>
        </FormControl>
      </Grid>
    );
  }
}

ServerAggregationUnits.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(ServerAggregationUnits);
