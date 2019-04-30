To provision the machine:

- Set the environment variables `HOST` and `SSH_USER` to the host and username
  of the machine to be provisioned (note that the provisioning user needs admin
  rights because it needs to be able to install packages on the system).

- Create a file called `ssh_keys/authorized_keys.txt` which contains the public
  SSH keys of the users who should be able to log into the `flowkit` account
  once the machine is provisioned.

- Finally, run the following command (make sure you don't forget the comma
  after `${HOST}` if you type it manually):
  ```
  pipenv install
  pipenv run ansible-galaxy install -p ./roles -r requirements.yml
  pipenv run ansible-playbook -i ${HOST}, --user=${SSH_USER} provision.yml
  ```

You can also replace `provision.yml` with `provision-dev.yml` in the last line.
This runs the standard provisioning (the same as in `provision.yml`) and will also
install Python 3 via `pyenv` and clone the FlowKit repository at `/home/flowkit/code/FlowKit`.

This has been tested with CentOS Linux release 7.5.1804.
