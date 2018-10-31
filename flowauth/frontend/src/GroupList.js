/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import { withStyles } from "@material-ui/core/styles";
import GroupDetails from "./GroupDetails";
import Lister from "./Lister";
import { getGroups, deleteGroup } from "./util/api";

const styles = theme => ({
	root: {
		...theme.mixins.gutters(),
		paddingTop: theme.spacing.unit * 2,
		paddingBottom: theme.spacing.unit * 2
	}
});
const GroupList = Lister(GroupDetails, "Groups", getGroups, deleteGroup);
export default withStyles(styles)(GroupList);
