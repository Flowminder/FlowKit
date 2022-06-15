import { KeyboardDateTimePicker } from "@material-ui/pickers"


function RoleDetails(props) {
  const {name, scopes} = props

  return (
    <Fragment>
      <p>{name}</p>
      <p>{scopes}</p>
    </Fragment>
  )

}