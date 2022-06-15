import { List, Typography } from "@material-ui/core"
import { Fragment } from "react"

function RoleDetails(props) {
  const {name, scopes} = props

  return (
    <Fragment>
      <Typography varient="h2">{name}</Typography>
      <List>
        {scopes.map((scope) =>
        <Typography varient = "body1" gutterBottom>{scope}</Typography>)}

      </List>
      <Typography>{scopes}</Typography>
    </Fragment>
  )

}
export default (RoleDetails)