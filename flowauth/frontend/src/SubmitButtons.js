/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Button from "@material-ui/core/Button";
import Grid from "@material-ui/core/Grid";

function SubmitButtons(props) {
	const { handleSubmit, onClick } = props;
	return (
		<Grid item xs={12} container direction="row-reverse" spacing={16}>
			<Grid item>
				<Button
					type="submit"
					fullWidth
					variant="contained"
					color="primary"
					onClick={handleSubmit}
				>
					Save
				</Button>
			</Grid>
			<Grid item>
				<Button
					type="submit"
					fullWidth
					variant="contained"
					color="secondary"
					onClick={onClick}
				>
					Cancel
				</Button>
			</Grid>
		</Grid>
	);
}

export default SubmitButtons;
