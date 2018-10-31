/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import DeleteIcon from "@material-ui/icons/Delete";
import IconButton from "@material-ui/core/IconButton";
import { deleteCapability } from "./util/api";

function CapabilityListItem(props) {
  const { name, id, rmCapability } = props;
  console.log(props);
  return (
    <React.Fragment>
      <Grid item xs={6}>
        <Typography component="p">{name}</Typography>
      </Grid>
      <Grid item xs={5} />
      <Grid item xs>
        <IconButton color="inherit">
          <DeleteIcon
            onClick={() => deleteCapability(id) && rmCapability(id)}
          />
        </IconButton>
      </Grid>
    </React.Fragment>
  );
}

export default CapabilityListItem;
