/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
 
import { withStyles } from "@material-ui/core/styles";
import RoleDetails from "./RoleAdmin";
import Lister from "./Lister";
import { getRoles, deleteRole } from "./util/api";

const styles = (theme) => ({
  root: {
    ...theme.mixins.gutters(),
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2,
  },
});
const RoleList = Lister(
  RoleDetails,
  "Roles",
  getRoles,
  deleteRole
);
export default withStyles(styles)(RoleList);

