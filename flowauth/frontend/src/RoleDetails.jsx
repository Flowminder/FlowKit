import { Divider, List, Typography, ListItem } from "@material-ui/core"
import { Fragment } from "react"

function RoleDetails(props) {
  const {role} = props


  

  return (
    <Fragment>
      <ListItem varient="h6">{role.name}
      <List>
        {role.scopes.map((scope) =>
        <ListItem>
        <Typography varient = "body1" gutterTop>
          {scope}
        </Typography>
        <Divider />
        </ListItem>)
        }
      </List>
      </ ListItem>
    </Fragment>
  )

}
export default (RoleDetails)