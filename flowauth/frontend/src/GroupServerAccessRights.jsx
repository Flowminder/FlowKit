/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Typography from "@material-ui/core/Typography";
import Grid from "@material-ui/core/Grid";
import ErrorDialog from "./ErrorDialog";
import { getGroupCapabilities, getCapabilities } from "./util/api";
import RightsCascade from "./RightsCascade";
import { jsonify, scopesGraph } from "./util/util";

class GroupServerAccessRights extends React.Component {
  state = {
    rights: [],
    enabledRights: [],
    fullRights: [],
    errors: { message: "" },
  };

  handleRightsChange = (value) => {
    const { parentUpdate } = this.props;
    this.setState({ enabledRights: value });
    const { fullRights } = this.state;
    parentUpdate(
      fullRights.filter((r) => value.some((cur) => r.startsWith(cur))),
    );
  };

  async componentDidMount() {
    const { groupId, serverId } = this.props;
    const groupCapabilities = await getGroupCapabilities(serverId, groupId);
    const serverCapabilities = await getCapabilities(serverId);
    const scopesObj = {};
    Object.keys(serverCapabilities).forEach((sCap) => {
      if (serverCapabilities[sCap]) {
        scopesObj[sCap] = groupCapabilities.includes(sCap);
      }
    });

    const scopeGraph = scopesGraph(scopesObj);
    const enabledKeys = [];
    const scopes = jsonify(
      scopeGraph,
      [],
      Object.keys(scopesObj).filter((k) => scopesObj[k]),
      enabledKeys,
    );

    this.setState({
      rights: scopes,
      fullRights: Object.keys(scopesObj),
      enabledRights: enabledKeys,
    });
  }

  render() {
    if (this.state.hasError && this.state.error.code === 401)
      throw this.state.error;

    const { rights, enabledRights } = this.state;

    return (
      <React.Fragment>
        <Grid item xs={12}>
          <Typography variant="h5" component="h1">
            Available API scopes
          </Typography>
        </Grid>
        <Grid item xs={12}>
          <RightsCascade
            options={rights}
            value={enabledRights}
            onChange={this.handleRightsChange}
          />
        </Grid>
        <ErrorDialog
          open={this.state.pageError}
          message={this.state.errors.message}
        />
      </React.Fragment>
    );
  }
}

export default GroupServerAccessRights;
