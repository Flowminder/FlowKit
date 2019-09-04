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
  const { name, id, editAction, deleteAction } = props;
  return (
    <Grid item container xs={12} alignItems="center" justify="space-between">
      <Grid item xs>
        <Typography component="p">{name}</Typography>
      </Grid>
      <Grid
        item
        container
        xs={editAction ? 2 : 1}
        alignItems="center"
        justify="flex-end"
      >
        {editAction && (
          <Grid item>
            <IconButton
              color="inherit"
              onClick={() => editAction(id)}
              id={"edit_" + id}
            >
              <EditIcon />
            </IconButton>
          </Grid>
        )}
        <Grid item>
          <IconButton
            color="inherit"
            onClick={() => deleteAction(id)}
            id={"rm_" + id}
          >
            <DeleteIcon />
          </IconButton>
        </Grid>
      </Grid>
    </Grid>
  );
}

export default AdminListItem;
