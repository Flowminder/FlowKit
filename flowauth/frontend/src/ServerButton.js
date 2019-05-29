/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import ListItem from "@material-ui/core/ListItem";
import ListItemText from "@material-ui/core/ListItemText";

function ServerButton(props) {
	const { id, name, onClick } = props;

	return (
		<React.Fragment>
			<ListItem button id="servers" onClick={() => onClick(id, name)}>
				<ListItemText primary={name} />
			</ListItem>
		</React.Fragment>
	);
}

export default ServerButton;
