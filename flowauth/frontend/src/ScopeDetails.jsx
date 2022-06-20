import { Divider, List, Typography, ListItem } from "@material-ui/core"
import { Fragment } from "react"

function ScopeDetails(props) {
  const {scope} = props


  

  return (
    <Fragment>
      <ListItem varient="h6">{scope.name}
      <List>
        {scope.roles.map((role) =>
        <ListItem>
        <Typography varient = "body1" gutterTop>
          {role}
        </Typography>
        <Divider />
        </ListItem>)
        }
      </List>
      </ ListItem>
    </Fragment>
  )

}
export default (ScopeDetails)