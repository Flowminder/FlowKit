/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Typography from "@material-ui/core/Typography";
import TokenPermission from "./TokenPermission";
import Grid from "@material-ui/core/Grid";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import ServerAggregationUnits from "./ServerAggregationUnits";
import ExpansionPanel from "@material-ui/core/ExpansionPanel";
import ExpansionPanelSummary from "@material-ui/core/ExpansionPanelSummary";
import ExpansionPanelDetails from "@material-ui/core/ExpansionPanelDetails";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import Checkbox from "@material-ui/core/Checkbox";

const styles = theme => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  },
  heading: {
    fontSize: theme.typography.pxToRem(18),
    fontWeight: theme.typography.alignCenter
  }
});
class PermissionDetails extends React.Component {
  state = {
    isPermissionChecked: true,
    isAggregationChecked: true,
    permissionIndeterminate: false,
    aggregateIndeterminate: false,
    totalAggregateUnits: 0,
    pageError: false,
    errors: { message: "" }
  };

  handleChange = (claim, right) => event => {
    this.setState({ pageError: false, errors: "" });
    const { updateRights, rights, permitted } = this.props;

    rights[claim].permissions[right] = event.target.checked;
    const permissionSet = new Set();
    for (const keys in rights) {
      for (const key in rights[keys].permissions) {
        if (permitted[keys].permissions[key]) {
          permissionSet.add(rights[keys].permissions[key]);
        }
      }
    }
    const indeterminate = permissionSet.size > 1;
    updateRights(rights);
    this.setState({ permissionIndeterminate: indeterminate });
  };

  handleAggUnitChange = (claim_id, claim, unit) => event => {
    const { updateRights, rights } = this.props;
    this.setState({ pageError: false, errors: "" });
    const { totalAggregateUnits } = this.state;
    if (event.target.checked) {
      rights[claim].spatial_aggregation.push(unit);
    } else {
      rights[claim].spatial_aggregation = rights[
        claim
      ].spatial_aggregation.filter(u => u != unit);
    }
    console.log(rights);
    const listUnits = [];
    for (const key in rights) {
      for (const keys in rights[key].spatial_aggregation) {
        listUnits.push(keys);
      }
    }
    updateRights(rights);
    this.setState({
      aggregateIndeterminate: totalAggregateUnits != listUnits.length
    });
  };
  handlePermissionCheckbox = event => {
    const { updateRights, rights, permitted } = this.props;
    const toCheck = event.target.checked;
    event.stopPropagation();
    this.setState({
      isPermissionChecked: toCheck,
      permissionIndeterminate: false,
      pageError: false,
      errors: ""
    });

    for (const keys in permitted) {
      for (const key in permitted[keys].permissions) {
        rights[keys].permissions[key] =
          permitted[keys].permissions[key] && toCheck;
      }
    }
    updateRights(rights);
  };
  handleAggregationCheckbox = event => {
    const { updateRights, rights, permitted } = this.props;
    const toCheck = event.target.checked;
    this.setState({
      pageError: false,
      errors: "",
      isAggregationChecked: toCheck,
      aggregateIndeterminate: false
    });
    event.stopPropagation();
    var listUnits = [];
    for (const key in rights) {
      if (toCheck) {
        rights[key].spatial_aggregation = permitted[key].spatial_aggregation;
      } else {
        rights[key].spatial_aggregation = [];
      }
      for (const keys in rights[key].spatial_aggregation) {
        listUnits.push(keys);
      }
    }
    updateRights(rights);
  };

  isAggUnitPermitted = (claim, key) => {
    const { permitted } = this.props;
    return permitted[claim].spatial_aggregation.indexOf(key) !== -1;
  };

  static getDerivedStateFromProps(props, state) {
    const { permitted, rights } = props;

    if (permitted) {
      const totalAggUnits = Object.keys(permitted).reduce(
        (acc, k) => acc + permitted[k].spatial_aggregation.length,
        0
      );
      const permissionSet = new Set();
      for (const keys in rights) {
        for (const key in rights[keys].permissions) {
          if (permitted[keys].permissions[key]) {
            permissionSet.add(rights[keys].permissions[key]);
          }
        }
      }
      return {
        totalAggregateUnits: totalAggUnits,
        permissionIndeterminate: permissionSet.size > 1
      };
    } else {
      return state;
    }
  }

  renderRights = () => {
    var perms = [];
    const { rights, permitted } = this.props;
    for (const key in rights) {
      perms.push([
        <TokenPermission
          permissions={rights[key].permissions}
          claim={key}
          checkedHandler={this.handleChange}
          permitted={permitted[key].permissions}
        />,
        key
      ]);
    }
    return perms
      .sort((a, b) => {
        if (a[1] > b[1]) {
          return 1;
        } else if (a[1] < b[1]) {
          return -1;
        } else {
          return 0;
        }
      })
      .map(x => x[0]);
  };

  renderAggUnits = () => {
    var perms = [];
    const { rights } = this.props;
    for (const key in rights) {
      perms.push([
        <ServerAggregationUnits
          units={rights[key].spatial_aggregation}
          claim={key}
          checkedHandler={this.handleAggUnitChange}
          permitted={this.isAggUnitPermitted}
        />,
        key
      ]);
    }
    return perms
      .sort((a, b) => {
        if (a[1] > b[1]) {
          return 1;
        } else if (a[1] < b[1]) {
          return -1;
        } else {
          return 0;
        }
      })
      .map(x => x[0]);
  };

  render() {
    if (this.state.hasError) throw this.state.error;

    const {
      aggregateIndeterminate,
      isAggregationChecked,
      isPermissionChecked,
      permissionIndeterminate
    } = this.state;
    const { classes, permitted } = this.props;

    if (permitted) {
      return (
        <>
          <ExpansionPanel>
            <ExpansionPanelSummary expandIcon={<ExpandMoreIcon id="api-exp" />}>
              <Checkbox
                checked={isPermissionChecked}
                indeterminate={permissionIndeterminate}
                data-cy="permissions-top-level"
                id="permissions"
                value="checkedB"
                color="primary"
                onClick={this.handlePermissionCheckbox}
              />

              <Grid item xs={2}>
                <Typography
                  className={classes.heading}
                  style={{ paddingTop: 10 }}
                >
                  API Permissions
                </Typography>
              </Grid>
            </ExpansionPanelSummary>
            <ExpansionPanelDetails>
              <Grid container spacing={5}>
                {this.renderRights()}
              </Grid>
            </ExpansionPanelDetails>
          </ExpansionPanel>
          <ExpansionPanel>
            <ExpansionPanelSummary
              expandIcon={<ExpandMoreIcon id="unit-exp" />}
            >
              <Checkbox
                id="units"
                checked={isAggregationChecked}
                indeterminate={aggregateIndeterminate}
                value="checkedB"
                color="primary"
                onClick={this.handleAggregationCheckbox}
              />

              <Grid item xs={3}>
                <Typography
                  className={classes.heading}
                  style={{ paddingTop: 10 }}
                >
                  Aggregation Units
                </Typography>
              </Grid>
            </ExpansionPanelSummary>
            <ExpansionPanelDetails>
              <Grid container spacing={5}>
                {this.renderAggUnits()}
              </Grid>
            </ExpansionPanelDetails>
          </ExpansionPanel>
        </>
      );
    } else {
      return <></>;
    }
  }
}

PermissionDetails.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(PermissionDetails);
