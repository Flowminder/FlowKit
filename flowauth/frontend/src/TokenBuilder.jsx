/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Grid from "@material-ui/core/Grid";
import Stack from "@material-ui/core/Grid"
import DatePicker from "@material-ui/pickers/DatePicker"
import Typography from "@material-ui/core/Typography";
import { withStyles } from "@material-ui/core/styles"
import { Button } from "rsuite";
import UserRoleList from "./UserRoleList";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  }
})

class TokenBuilder extends React.Component {

  state = {
    token_date: Date(2020, 1, 1)
  }

  handleDateChange = (new_date) => {
    this.setState({token_date:new_date})
  }

  render() {
    return (
      <React.Fragment>
        <Grid container xs={8}>
          <UserRoleList />
        </Grid>
        <Grid container xs={4}>
          <Stack>
            <DatePicker
              label="Expiry date"
              value = {this.state.token_date}
              onChange={this.handleDateChange}
            />
            <Button>
              Get token
            </Button>
          </Stack>
        </Grid>
      </React.Fragment>
    )
  }
}

export default withStyles(styles)(TokenBuilder)