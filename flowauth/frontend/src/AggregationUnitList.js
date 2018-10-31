/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import Paper from "@material-ui/core/Paper";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import IconButton from "@material-ui/core/IconButton";
import AddIcon from "@material-ui/icons/Add";
import AggregationUnitListItem from "./AggregationUnitListItem";
import TextField from "@material-ui/core/TextField";
import { getAllAggregationUnits, newAggregationUnit } from "./util/api";

const styles = theme => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  }
});

class AggregationUnitList extends React.Component {
  state = { units: [], new_unit: "" };
  componentDidMount() {
    getAllAggregationUnits()
      .then(units => {
        this.setState({
          units: units.sort()
        });
      })
      .catch(err => {
        this.setState({ hasError: true, error: err });
      });
  }
  handleChange = name => event => {
    this.setState({
      [name]: event.target.value
    });
  };
  rmUnit = unit_id => {
    this.setState({
      units: this.state.units.filter(unit => unit.id !== unit_id)
    });
  };
  addCap = () => {
    const { new_unit, units } = this.state;
    if (new_unit.length > 0) {
      newAggregationUnit(new_unit).then(json => {
        units.push({ name: json.name, id: json.id });
        this.setState({
          new_unit: "",
          units: units
        });
      });
    }
  };
  renderunits = () => {
    const { classes } = this.props;
    const { units } = this.state;
    return units.map(object => {
      return (
        <AggregationUnitListItem
          name={object.name}
          id={object.id}
          classes={classes}
          rmUnit={this.rmUnit}
        />
      );
    });
  };

  render() {
    if (this.state.hasError) throw this.state.error;

    const { classes } = this.props;
    const { new_unit } = this.state;
    return (
      <Paper className={classes.root}>
        <Grid container spacing={16} alignItems="center">
          <Grid item xs={12}>
            <Typography variant="headline" component="h1">
              Aggregation Units
            </Typography>
          </Grid>
          <Grid item xs={6}>
            <Typography component="h3">Aggregation unit name</Typography>
          </Grid>
          <Grid item xs={6} />
          {this.renderunits()}
          <Grid item xs={6}>
            <TextField
              id="standard-name"
              label="New Aggregation Unit"
              className={classes.textField}
              value={new_unit}
              onChange={this.handleChange("new_unit")}
              margin="normal"
              InputLabelProps={{ shrink: true }}
            />
          </Grid>
          <Grid item xs={5} />
          <Grid item xs>
            <IconButton color="inherit" aria-label="New" onClick={this.addCap}>
              <AddIcon />
            </IconButton>
          </Grid>
        </Grid>
      </Paper>
    );
  }
}

AggregationUnitList.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(AggregationUnitList);
