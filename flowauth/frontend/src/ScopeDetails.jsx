/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import { Divider, List, Typography, ListItem } from "@material-ui/core";
import { Fragment } from "react";

function ScopeDetails(props) {
  const { scope } = props;

  return (
    <Fragment>
      <ListItem variant="h6">
        {scope.name}
        <List>
          {scope.roles.map((role) => (
            <ListItem>
              <Typography variant="body1" gutterTop>
                {role}
              </Typography>
              <Divider />
            </ListItem>
          ))}
        </List>
      </ListItem>
    </Fragment>
  );
}
export default ScopeDetails;
