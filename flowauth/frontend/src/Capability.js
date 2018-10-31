import React from "react";
import Grid from "@material-ui/core/Grid";
import Switch from "@material-ui/core/Switch";

class Capability extends React.Component {
  state = {
    checked: true
  };

  handleChange = event => {
    this.setState({ checked: event.target.checked });
  };

  render() {
    const { name } = this.props;
    return (
      <React.Fragment>
        <Grid container spacing={24}>
          <Grid item xs={6}>
            {name}
          </Grid>
          <Grid item xs>
            <Switch
              checked={this.state.checked}
              onChange={this.handleChange}
              value="checked"
            />
          </Grid>
        </Grid>
      </React.Fragment>
    );
  }
}

export default Capability;
