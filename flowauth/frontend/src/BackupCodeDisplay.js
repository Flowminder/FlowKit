import Grid from "@material-ui/core/Grid";
import Button from "@material-ui/core/Button";
import Typography from "@material-ui/core/Typography";
import React from "react";

class BackupCodeDisplay extends React.Component {
  copyToClipboard = event => {
    var textField = document.createElement("textarea");
    textField.style.whiteSpace = "pre-wrap";
    textField.value = this.state.backup_codes.join("\n");
    document.body.appendChild(textField);
    textField.select();
    document.execCommand("copy");
    textField.remove();
    this.setState({ backupsCollected: true });
  };
  downloadTxtFile = () => {
    const element = document.createElement("a");
    const file = new Blob([this.state.backup_codes.join("\n")], {
      type: "text/plain"
    });
    element.href = URL.createObjectURL(file);
    element.download = "two_factor_backups.txt";
    document.body.appendChild(element);
    element.click();
    this.setState({ backupsCollected: true });
  };

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
    const { backup_codes, cancel } = this.props;
    return (
      <>
        <Grid item xs={5} container direction="row">
          {backup_codes.map(code => this.renderBackupCode(code))}
        </Grid>
        <Grid item xs={5}>
          <Button
            type="submit"
            variant="contained"
            className={classes.button}
            onClick={this.downloadTxtFile}
          >
            Download
          </Button>

          <Button
            type="submit"
            variant="contained"
            className={classes.button}
            onClick={this.copyToClipboard}
          >
            Copy
          </Button>
        </Grid>

        <Grid item xs={12} container justify="space-between">
          <Grid item xs={2}>
            <Button
              type="submit"
              variant="contained"
              className={classes.button}
              onClick={this.advance}
              disabled={!backupsCollected && backup_codes.length > 0}
            >
              Next
            </Button>
          </Grid>
          <Grid item xs={2}>
            <Button
              type="submit"
              variant="contained"
              className={classes.button}
              onClick={this.cancel}
            >
              Cancel
            </Button>
          </Grid>
        </Grid>
      </>
    );
  }
}
