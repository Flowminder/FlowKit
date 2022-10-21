import { mount } from 'cypress/react'
import RoleDetails from '../src/RoleDetails'

describe('<RoleDetails>', () => {
	it('mounts', () => {
		mount(<RoleDetails />)
	})
})

