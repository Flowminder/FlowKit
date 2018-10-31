/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import GroupServerPicker from "./GroupServerPicker";
import ExpansionPanel from "@material-ui/core/ExpansionPanel";
import ExpansionPanelSummary from "@material-ui/core/ExpansionPanelSummary";
import ExpansionPanelDetails from "@material-ui/core/ExpansionPanelDetails";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import GroupServerPermissionDetails from "./GroupServerPermissionDetails";
import PropTypes from "prop-types";

class GroupServerPermissions extends React.Component {
  updateServer = server => {
    const { servers, updateServers } = this.props;
    servers[servers.map(s => s.id).indexOf(server.id)] = server;
    updateServers(servers);
  };

  render() {
    const { classes, updateServers, group_id, servers } = this.props;

    return (
      <React.Fragment>
        <Grid xs={12}>
          <Typography variant="headline" component="h1">
            Servers
          </Typography>
        </Grid>
        <GroupServerPicker
          group_id={group_id}
          servers={servers}
          updateServers={updateServers}
        />
        {servers.map(server => (
          <Grid item xs={12}>
            <ExpansionPanel>
              <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                <Typography className={classes.heading}>
                  {server.name}
                </Typography>
              </ExpansionPanelSummary>
              <ExpansionPanelDetails>
                <Grid container spacing={16}>
                  <GroupServerPermissionDetails
                    classes={this.props.classes}
                    server={server}
                    updateServer={this.updateServer}
                    group_id={group_id}
                  />
                </Grid>
              </ExpansionPanelDetails>
            </ExpansionPanel>
          </Grid>
        ))}
      </React.Fragment>
    );
  }
}

GroupServerPermissions.propTypes = {
  classes: PropTypes.object.isRequired
};

export default GroupServerPermissions;
