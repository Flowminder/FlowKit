/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import TextField from "@material-ui/core/TextField";
import Grid from "@material-ui/core/Grid";
import { createGroup } from "./util/api";
import Typography from "@material-ui/core/Typography";
import GroupMembersPicker from "./GroupMembersPicker";
import GroupServerPermissions from "./GroupServerPermissions";
import SubmitButtons from "./SubmitButtons";
import {
  getGroup,
  editMembers,
  renameGroup,
  editGroupServers
} from "./util/api";

class GroupDetails extends React.Component {
  state = { name: "", members: [], servers: [], edit_mode: false };

  componentDidMount() {
    this._asyncRequest = getGroup(this.props.item_id)
      .then(json => {
        this.setState(Object.assign(json || {}, json && { edit_mode: true }));
      })
      .catch(err => {
        if (err.code !== 404) {
          this.setState({ hasError: true, error: err });
        }
      });
  }

  handleChange = name => event => {
    this.setState({
      [name]: event.target.value
    });
  };
  updateMembers = members => {
    this.setState({ members: members });
  };
  updateServers = servers => {
    this.setState({ servers: servers });
  };
  handleSubmit = () => {
    var task;
    if (this.state.edit_mode) {
      task = renameGroup(this.props.item_id, this.state.name);
    } else {
      task = createGroup(this.state.name, []);
    }

    task
      .then(json => {
        console.log(json);
        return editMembers(json.id, this.state.members);
      })
      .then(json => {
        return editGroupServers(json.id, this.state.servers);
      })
      .then(json => {
        this.props.onClick();
      });
  };
  render() {
    if (this.state.hasError) throw this.state.error;

    const { classes, onClick, item_id } = this.props;
    const { servers } = this.state;

    return (
      <React.Fragment>
        <Grid xs={12}>
          <Typography variant="h5" component="h1">
            {(this.state.edit_mode && "Edit Group") || "New Group"}
          </Typography>
        </Grid>
        <Grid xs={12}>
          <TextField
            id="group_name"
            label="Name"
            className={classes.textField}
            value={this.state.name}
            onChange={this.handleChange("name")}
            margin="normal"
          />
        </Grid>

        <Grid xs={12}>
          <Typography variant="h5" component="h1">
            Members
          </Typography>
        </Grid>
        <Grid xs={12}>
          <GroupMembersPicker
            group_id={item_id}
            updateMembers={this.updateMembers}
          />
        </Grid>
        <GroupServerPermissions
          group_id={item_id}
          updateServers={this.updateServers}
          servers={servers}
          classes={classes}
        />

        <Grid item xs={12} />
        <SubmitButtons handleSubmit={this.handleSubmit} onClick={onClick} />
      </React.Fragment>
    );
  }
}

export default GroupDetails;
