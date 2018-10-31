/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Button from "@material-ui/core/Button";
import Paper from "@material-ui/core/Paper";
import Grid from "@material-ui/core/Grid";
import TextField from "@material-ui/core/TextField";
import PropTypes from "prop-types";
import LockIcon from "@material-ui/icons/Lock";
import LockOpenIcon from "@material-ui/icons/LockOpen";
import { withStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import InputAdornment from "@material-ui/core/InputAdornment";
import { editPassword } from "./util/api";
var zxcvbn = require("zxcvbn");

const styles = theme => ({
	root: {
		...theme.mixins.gutters(),
		paddingTop: theme.spacing.unit * 2,
		paddingBottom: theme.spacing.unit * 2
	}
});

class UserDetails extends React.Component {
	state = {
		oldPassword: "",
		newPasswordA: "",
		newPasswordB: "",
		password_strength: null
	};

	handleSubmit = () => {
		if (this.state.newPasswordA === this.state.newPasswordB) {
			editPassword(this.state.oldPassword, this.state.newPasswordA).then(
				() => {
					this.setState({ hasError: true, error: "Logged out" });
				}
			);
		}
	};

	handleTextChange = name => event => {
		var passStrength = zxcvbn(event.target.value);
		var state = {
			[name]: event.target.value
		};
		if (name === "newPasswordA") {
			state = Object.assign(state, {
				password_strength: passStrength.score
			});
		}
		this.setState(state);
	};

	render() {
		if (this.state.hasError) throw this.state.error;

		const { classes } = this.props;
		const {
			oldPassword,
			newPasswordA,
			newPasswordB,
			password_strength
		} = this.state;

		return (
			<Paper className={classes.root}>
				<Grid container spacing={16} alignItems="center">
					<Grid item xs={12}>
						<Typography variant="headline" component="h1">
							Reset password
						</Typography>
					</Grid>
					<Grid xs={3}>
						<TextField
							id="standard-name"
							className={classes.textField}
							label="Old Password"
							type="password"
							value={oldPassword}
							onChange={this.handleTextChange("oldPassword")}
							margin="normal"
						/>
					</Grid>
					<Grid xs={9} />
					<Grid xs={3}>
						<TextField
							id="standard-name"
							className={classes.textField}
							label="New Password"
							type="password"
							value={newPasswordA}
							onChange={this.handleTextChange("newPasswordA")}
							margin="normal"
							InputProps={{
								endAdornment: (
									<InputAdornment position="end">
										{(password_strength ||
											password_strength === 0) &&
											((password_strength > 3 && (
												<LockIcon />
											)) || (
												<LockOpenIcon color="secondary" />
											))}
									</InputAdornment>
								)
							}}
						/>
					</Grid>
					<Grid xs={9} />
					<Grid xs={3}>
						<TextField
							error={newPasswordA !== newPasswordB}
							id="standard-name"
							className={classes.textField}
							label="New Password"
							type="password"
							value={newPasswordB}
							onChange={this.handleTextChange("newPasswordB")}
							margin="normal"
						/>
					</Grid>
					<Grid xs={9} />
					<Grid item xs={12} container direction="row-reverse">
						<Grid item>
							<Button
								type="submit"
								fullWidth
								variant="raised"
								color="primary"
								onClick={this.handleSubmit}
							>
								Save
							</Button>
						</Grid>
					</Grid>
				</Grid>
			</Paper>
		);
	}
}
UserDetails.propTypes = {
	classes: PropTypes.object.isRequired
};

export default withStyles(styles)(UserDetails);
