/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import DeleteIcon from "@material-ui/icons/Delete";
import IconButton from "@material-ui/core/IconButton";
import { deleteAggregationUnit } from "./util/api";

function AggregationUnitListItem(props) {
  const { name, id, rmUnit } = props;
  console.log(props);
  return (
    <React.Fragment>
      <Grid item xs={6}>
        <Typography component="p">{name}</Typography>
      </Grid>
      <Grid item xs={5} />
      <Grid item xs>
        <IconButton color="inherit">
          <DeleteIcon onClick={() => deleteAggregationUnit(id) && rmUnit(id)} />
        </IconButton>
      </Grid>
    </React.Fragment>
  );
}

export default AggregationUnitListItem;
