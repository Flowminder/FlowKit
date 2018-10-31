/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import DeleteIcon from "@material-ui/icons/Delete";
import EditIcon from "@material-ui/icons/Edit";
import IconButton from "@material-ui/core/IconButton";

function AdminListItem(props) {
  const { name, id, editAction, rmAction, deleteAction } = props;
  return (
    <React.Fragment>
      <Grid item xs={6}>
        <Typography component="p">{name}</Typography>
      </Grid>
      <Grid item xs={4} />
      <Grid item xs>
        <IconButton color="inherit" onClick={() => editAction(id)}>
          <EditIcon />
        </IconButton>
      </Grid>
      <Grid item xs>
        <IconButton color="inherit">
          <DeleteIcon onClick={() => deleteAction(id) && rmAction(id)} />
        </IconButton>
      </Grid>
    </React.Fragment>
  );
}

export default AdminListItem;
