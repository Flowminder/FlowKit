import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import React from "react";
import PropTypes from "prop-types";
import { withStyles } from "@material-ui/core";
const styles = theme => ({
  button: {
    margin: theme.spacing.unit
  },
  codeBlock: {
    fontFamily: "Consolas, Monaco, 'Andale Mono', 'Ubuntu Mono', monospace"
  }
});
class BackupCodesBlock extends React.Component {
  renderBackupCode = code => {
    return (
      <Grid item xs={6}>
        <Typography
          variant="body2"
          align="center"
          className={this.props.classes.codeBlock}
        >
          <span className="trailing_dash">
            {code.slice(0, code.length / 2)}
          </span>
          {code.slice(code.length / 2, code.length)}
        </Typography>
      </Grid>
    );
  };

  render() {
    const { backup_codes } = this.props;
    return (
      <Grid item xs={8} container direction="row">
        {backup_codes.map(code => this.renderBackupCode(code))}
      </Grid>
    );
  }
}

BackupCodesBlock.propTypes = {
  classes: PropTypes.object.isRequired
};
export default withStyles(styles)(BackupCodesBlock);
