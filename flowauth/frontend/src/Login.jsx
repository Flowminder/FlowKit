/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import PropTypes from "prop-types";
import Avatar from "@material-ui/core/Avatar";
import Button from "@material-ui/core/Button";
import CssBaseline from "@material-ui/core/CssBaseline";
import Paper from "@material-ui/core/Paper";
import Typography from "@material-ui/core/Typography";
import withStyles from "@material-ui/core/styles/withStyles";
import { twoFactorlogin, isLoggedIn } from "./util/api";
import ErrorDialog from "./ErrorDialog";
import LoginBox from "./LoginBox";
import TwoFactorLoginBox from "./TwoFactorLoginBox";
import TwoFactorConfirm from "./TwoFactorConfirm";

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
  root: {
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
      require_two_factor_setup: false,
      hasError: false,
      error: { message: "" }
    };
  }

  postLogin = () => {
    const { setLoggedIn } = this.props;
    const { is_admin } = this.state;
    setLoggedIn(is_admin);
  };

  handleSubmit = async event => {
    event.preventDefault();
    const { username, password, two_factor_code } = this.state;
    try {
      const json = await twoFactorlogin(username, password, two_factor_code);
      if (json.require_two_factor_setup) {
        this.setState({
          require_two_factor_setup: true,
          is_admin: json.is_admin
        });
      } else {
        this.props.setLoggedIn(json.is_admin);
      }
    } catch (err) {
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
    }
  };

  handleChange = event => {
    this.setState({
      [event.target.id]: event.target.value,
      hasError: false,
      error: { message: "" }
    });
  };

  resetTwoFactor = () => {
    this.props.setLoggedOut();
    this.setState({ require_two_factor_setup: false });
  };

  async componentDidMount() {
    try {
      const json = await isLoggedIn();
      if (json.require_two_factor_setup) {
        this.setState({ require_two_factor_setup: true });
      } else {
        this.props.setLoggedIn(json.is_admin);
      }
    } catch (err) {
      if (err.code !== 401) {
        this.setState({ hasError: true, error: err });
      }
    }
  }

  render() {
    const { classes } = this.props;
    const {
      need_two_factor,
      two_factor_code,
      require_two_factor_setup,
      username,
      password
    } = this.state;

    return (
      <React.Fragment>
        <CssBaseline />
        <main className={classes.layout}>
          {!require_two_factor_setup && (
            <Paper className={classes.root}>
              <Avatar
                className={classes.avatar}
                src={require("./img/flowminder_logo.png")}
              />
              <Typography variant="h5">Sign in</Typography>
              <form className={classes.form} onSubmit={this.handleSubmit}>
                {!need_two_factor && (
                  <LoginBox
                    username={username}
                    password={password}
                    usernameChangeHandler={this.handleChange}
                    passwordChangeHandler={this.handleChange}
                  />
                )}

                {need_two_factor && (
                  <TwoFactorLoginBox
                    two_factor_code={two_factor_code}
                    handleChange={this.handleChange}
                  />
                )}
                <Button
                  type="submit"
                  fullWidth
                  variant="contained"
                  color="primary"
                  className={classes.submit}
                  id="signin-button"
                >
                  Sign in
                </Button>
              </form>
            </Paper>
          )}
          {require_two_factor_setup && (
            <TwoFactorConfirm
              classes={classes}
              finish={this.postLogin}
              cancel={this.resetTwoFactor}
            />
          )}
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
