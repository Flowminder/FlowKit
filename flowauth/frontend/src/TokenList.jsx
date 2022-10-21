/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core/styles";
import Grid from "@material-ui/core/Grid";
import Collapse from "@material-ui/core/Collapse";
import Typography from "@material-ui/core/Typography";
import IconButton from "@material-ui/core/IconButton";
import AddIcon from "@material-ui/icons/Add";
import Token from "./Token";
import { getMyTokensForServer, getServer } from "./util/api";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2,
  },
});

class TokenList extends React.Component {
  state = { tokens: [], serverName: "" };

  updateTokenList = () => {
    getMyTokensForServer(this.props.serverID)
      .then((tokens) => {
        tokens.sort((a, b) => Date.parse(b.expires) - Date.parse(a.expires));
        this.setState({ tokens: tokens });
      })
      .catch((err) => {
        console.error(err);
        this.setState({ hasError: true, error: err });
      });
  };
  getServerName = () => {
    getServer(this.props.serverID)
    .then(server => this.setState({serverName: server.name}))
    .catch(err => console.error(err))
  }
  componentDidMount() {
    this.updateTokenList();
    this.getServerName();
  }

  componentDidUpdate(prevProps) {
    if (this.props.serverID !== prevProps.serverID) {
      this.updateTokenList();
    }
  }
  render() {
    const { classes, nickName, editAction} = this.props;
    const { tokens, serverName } = this.state;

    const now = Date.parse(new Date());
    const activeTokens = tokens.filter(
      (token) => Date.parse(token.expires) > now
    );
    const expiredTokens = tokens.filter(
      (token) => Date.parse(token.expires) <= now
    );

    return (
      <React.Fragment>
        <React.Fragment>
          <Grid item xs={12}>
            <Typography variant="h5" component="h3">
              Server: {serverName}
            </Typography>
          </Grid>
          <Grid item xs={12}>
            <Typography variant="h5" component="h1">
              {nickName}
            </Typography>
          </Grid>
          <Grid item xs={12} />
          <Grid item xs={11}>
            <Typography variant="h5" component="h2">
              ✅&nbsp; Active tokens
            </Typography>
          </Grid>
          <Grid item xs={1}>
            <IconButton
              color="inherit"
              id="new"
              aria-label="New"
              onClick={editAction}
            >
              <AddIcon />
            </IconButton>
          </Grid>
          <Grid item xs={12} />
          <Grid item xs={4}>
            <Typography component="h3">Nickname</Typography>
          </Grid>
          <Grid item xs={3}>
            <Typography component="h3">Expiry</Typography>
          </Grid>
          <Grid item xs={5} />
          {activeTokens.map((object) => (
            <Token
              name={object.name}
              expiry={object.expires}
              token={object.token}
              classes={classes}
              editAction={editAction}
            />
          ))}
        </React.Fragment>
        {expiredTokens.length > 0 && (
          <React.Fragment>
            <Grid item xs={12} />
            <Grid item xs={12}>
              <Typography variant="h5" component="h2">
                ⏰&nbsp;Expired tokens
              </Typography>
            </Grid>
            <Grid item xs={12} />
            <Grid item xs={3}>
              <Typography component="h3">Nickname</Typography>
            </Grid>
            <Grid item xs={3}>
              <Typography component="h3">Expired</Typography>
            </Grid>

            <Grid item xs={6} />
            <Grid item xs={12} />
            {expiredTokens.map((object) => (
              <Token
                name={object.name}
                expiry={object.expires}
                token={object.token}
                classes={classes}
                editAction={editAction}
              />
            ))}
            <Grid item xs={12} />
          </React.Fragment>
        )}
      </React.Fragment>
    );
  }
}

TokenList.propTypes = {
  classes: PropTypes.object.isRequired,
};

export default withStyles(styles)(TokenList);
