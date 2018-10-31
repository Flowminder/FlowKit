/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import { getServers } from "./util/api";
import Picker from "./Picker";

class GroupServerPicker extends React.Component {
  state = {
    all_servers: []
  };

  handleChange = event => {
    this.props.updateServers(event.target.value);
  };

  deleteServer = del => {
    const servers = [...this.props.servers];
    const serverToDelete = this.props.servers.indexOf(del);
    servers.splice(serverToDelete, 1);
    this.props.updateServers(servers);
  };

  getData = () => {
    var all_servers;
    const { servers } = this.props;
    getServers()
      .then(json => {
        all_servers = json;
      })
      .catch(err => {
        if (err.code === 404) {
          all_servers = [];
        } else {
          throw err;
        }
      })
      .then(() => {
        this.setState({
          all_servers: all_servers.map(server => {
            var groupHas = servers.map(s => s.id);
            var inList = groupHas.indexOf(server.id);
            return inList !== -1 ? servers[inList] : server;
          })
        });
      })
      .catch(err => {
        this.setState({ hasError: true, error: err });
      });
  };

  componentDidMount() {
    this.getData();
  }

  componentDidUpdate(prevProps) {
    const { group_id } = this.props;
    if (group_id !== prevProps.group_id) {
      this.getData();
    }
  }

  render() {
    if (this.state.hasError) throw this.state.error;

    const { servers } = this.props;
    const { all_servers, hasError, error } = this.state;
    return (
      <Picker
        objs={servers}
        all_objs={all_servers}
        hasError={hasError}
        error={error}
        handleChange={this.handleChange}
        label={"Servers"}
      />
    );
  }
}

export default GroupServerPicker;
