/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import Button from "@material-ui/core/Button";
import Paper from "@material-ui/core/Paper";
import Grid from "@material-ui/core/Grid";
import TextField from "@material-ui/core/TextField";
import FormControl from "@material-ui/core/FormControl";
import Input from "@material-ui/core/Input";
import InputLabel from "@material-ui/core/InputLabel";
import PropTypes from "prop-types";
import LockIcon from "@material-ui/icons/Lock";
import LockOpenIcon from "@material-ui/icons/LockOpen";
import { withStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import InputAdornment from "@material-ui/core/InputAdornment";
import { editPassword } from "./util/api";
import ErrorDialog from "./ErrorDialog";
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
		password_strength: null,
		hasError: false,
		error: { message: "" }
	};

	handleSubmit = async event => {
		event.preventDefault();
		if (this.state.newPasswordA === this.state.newPasswordB) {
			editPassword(this.state.oldPassword, this.state.newPasswordA)
				.then(() => {
					this.setState({
						passwordChanged: true,
						oldPassword: "",
						newPasswordA: "",
						newPasswordB: "",
						password_strength: null,
						hasError: false,
						error: { message: "" }
					});
				})
				.catch(err => {
					this.setState({ hasError: true, error: err });
				});
		}
		else {
			this.setState({ hasError: true, error: { message: "Passwords do not match" } });
		}
	};

	handleTextChange = name => event => {
		var passStrength = zxcvbn(event.target.value);
		var state = {
			[name]: event.target.value,
			hasError: false,
			error: { message: "" }
		};
		if (name === "newPasswordA") {
			state = Object.assign(state, {
				password_strength: passStrength.score
			});
		}
		this.setState(state);
	};

	render() {
		if (this.state.passwordChanged) throw "Logged out";

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
					<Grid item xs={12}>
						<form className={classes.form} onSubmit={this.handleSubmit}>
							<Grid xs={3}>
								<FormControl margin="normal" required fullWidth>
									<InputLabel htmlFor="oldPassword">Old Password</InputLabel>
									<Input
										id="oldPassword"
										name="oldPassword"
										type="password"
										value={oldPassword}
										onChange={this.handleTextChange("oldPassword")}
										margin="normal"
									/>
								</FormControl>
							</Grid>
							<Grid xs={9} />
							<Grid xs={3}>
								<FormControl margin="normal" required fullWidth>
									<InputLabel htmlFor="newPasswordA">New Password</InputLabel>
									<Input
										id="newPasswordA"
										name="newPasswordA"
										type="password"
										value={newPasswordA}
										onChange={this.handleTextChange("newPasswordA")}
										margin="normal"
										endAdornment={
											<InputAdornment position="end">
												{(password_strength ||
													password_strength === 0) &&
													((password_strength > 3 && (
														<LockIcon />
													)) || (
															<LockOpenIcon color="secondary" />
														))}
											</InputAdornment>
										}
									/>
								</FormControl>
							</Grid>
							<Grid xs={9} />
							<Grid xs={3}>
								<FormControl
									margin="normal"
									required
									fullWidth
									error={newPasswordA !== newPasswordB}
								>
									<InputLabel htmlFor="newPasswordB">New Password</InputLabel>
									<Input
										id="newPasswordB"
										name="newPasswordB"
										type="password"
										value={newPasswordB}
										onChange={this.handleTextChange("newPasswordB")}
										margin="normal"
									/>
								</FormControl>
							</Grid>
							<Grid xs={9} />
							<Grid item xs={12} container direction="row-reverse">
								<Grid item>
									<Button
										type="submit"
										fullWidth
										variant="raised"
										color="primary"
										className={classes.submit}
									>
										Save
							</Button>
								</Grid>
							</Grid>
						</form>
					</Grid>
				</Grid>
				<ErrorDialog open={this.state.hasError} message={this.state.error.message} />
			</Paper>
		);
	}
}
UserDetails.propTypes = {
	classes: PropTypes.object.isRequired
};

export default withStyles(styles)(UserDetails);
