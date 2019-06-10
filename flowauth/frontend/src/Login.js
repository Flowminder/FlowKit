/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import Avatar from "@material-ui/core/Avatar";
import Button from "@material-ui/core/Button";
import CssBaseline from "@material-ui/core/CssBaseline";
import FormControl from "@material-ui/core/FormControl";
import Input from "@material-ui/core/Input";
import InputLabel from "@material-ui/core/InputLabel";
import Paper from "@material-ui/core/Paper";
import Typography from "@material-ui/core/Typography";
import withStyles from "@material-ui/core/styles/withStyles";
import { twoFactorlogin, isLoggedIn } from "./util/api";
import ErrorDialog from "./ErrorDialog";

const styles = theme => ({
  layout: {
    width: "auto",
    display: "block", // Fix IE11 issue.
    marginLeft: theme.spacing.unit * 3,
    marginRight: theme.spacing.unit * 3,
    [theme.breakpoints.up(400 + theme.spacing.unit * 3 * 2)]: {
      width: 400,
      marginLeft: "auto",
      marginRight: "auto"
    }
  },
  paper: {
    marginTop: theme.spacing.unit * 8,
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    padding: `${theme.spacing.unit * 2}px ${theme.spacing.unit * 3}px ${theme
      .spacing.unit * 3}px`
  },
  avatar: {
    margin: theme.spacing.unit,
    backgroundColor: "white"
  },
  form: {
    width: "100%", // Fix IE11 issue.
    marginTop: theme.spacing.unit
  },
  submit: {
    marginTop: theme.spacing.unit * 3
  }
});

class Login extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      username: "",
      password: "",
      two_factor_code: "",
      need_two_factor: false,
      hasError: false,
      error: { message: "" }
    };
  }

  handleSubmit = async event => {
    event.preventDefault();
    twoFactorlogin(
      this.state.username,
      this.state.password,
      this.state.two_factor_code
    )
      .then(json => {
        this.props.setLoggedIn(json.is_admin);
      })
      .catch(err => {
        if (err.need_two_factor) {
          this.setState({ need_two_factor: true, hasError: false });
        } else {
          this.setState({
            hasError: true,
            error: err,
            need_two_factor: false,
            two_factor_code: ""
          });
        }
      });
  };

  handleChange = event => {
    this.setState({
      [event.target.id]: event.target.value,
      hasError: false,
      error: { message: "" }
    });
  };

  componentDidMount() {
    isLoggedIn()
      .then(json => {
        this.props.setLoggedIn(json.is_admin);
      })
      .catch(err => {
        if (err.code !== 401) {
          this.setState({ hasError: true, error: err });
        }
      });
  }

  render() {
    const { classes } = this.props;
    const { need_two_factor } = this.state;

    return (
      <React.Fragment>
        <CssBaseline />
        <main className={classes.layout}>
          <Paper className={classes.paper}>
            <Avatar
              className={classes.avatar}
              src={require("./img/flowminder_logo.png")}
            />
            <Typography variant="h5">Sign in</Typography>
            <form className={classes.form} onSubmit={this.handleSubmit}>
              {!need_two_factor && (
                <>
                  <FormControl margin="normal" required fullWidth>
                    <InputLabel htmlFor="username">Username</InputLabel>
                    <Input
                      id="username"
                      name="username"
                      autoComplete="username"
                      autoFocus
                      value={this.state.username}
                      onChange={this.handleChange}
                    />
                  </FormControl>
                  <FormControl margin="normal" required fullWidth>
                    <InputLabel htmlFor="password">Password</InputLabel>
                    <Input
                      name="password"
                      type="password"
                      id="password"
                      value={this.state.password}
                      onChange={this.handleChange}
                      autoComplete="current-password"
                    />
                  </FormControl>
                </>
              )}

              {need_two_factor && (
                <FormControl margin="normal" required fullWidth>
                  <InputLabel htmlFor="auth_code">
                    Authorisation code
                  </InputLabel>
                  <Input
                    name="two_factor_code"
                    type="password"
                    id="two_factor_code"
                    value={this.state.two_factor_code}
                    onChange={this.handleChange}
                  />
                </FormControl>
              )}
              <Button
                type="submit"
                fullWidth
                variant="contained"
                color="primary"
                className={classes.submit}
              >
                Sign in
              </Button>
            </form>
          </Paper>
        </main>
        <ErrorDialog
          open={this.state.hasError}
          message={this.state.error.message}
        />
      </React.Fragment>
    );
  }
}

Login.propTypes = {
  classes: PropTypes.object.isRequired
};

export default withStyles(styles)(Login);
