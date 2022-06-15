/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React, {Fragment, useState} from "react";
import Grid from "@material-ui/core/Grid";
import Stack from "@material-ui/core/Grid"
import { DateTimePicker, MuiPickersUtilsProvider } from "@material-ui/pickers"
import DateFnsUtils from "@date-io/date-fns";
import Typography from "@material-ui/core/Typography";
import { withStyles } from "@material-ui/core/styles"
import { Button } from "rsuite";
import UserRoleList from "./UserRoleList";
import { getDisabledState } from "rsuite/esm/CheckTreePicker/utils";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  }
})
function TokenBuilder(props) {

  const [selectedDate, handleDateChange] = useState(new Date());
  const {user, server} = props

    return (
      <Fragment>
        <Grid container xs={8}>
          <UserRoleList
            user = {user}
            server = {server}
          />

          <Stack>
            <MuiPickersUtilsProvider utils={DateFnsUtils}>
              <DateTimePicker
                label="Expiry date"
                value = {selectedDate}
                onChange={handleDateChange}
              />
            </MuiPickersUtilsProvider> 
            <Button>
              Get token
            </Button>
          </Stack>
        </Grid>

      </Fragment>
    );
}

export default withStyles(styles)(TokenBuilder)