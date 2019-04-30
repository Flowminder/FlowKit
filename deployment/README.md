To provision the machine:

- Set up the pipenv environment and install auxiliary Ansible roles.
  ```bash
  pipenv install
  pipenv run ansible-galaxy install -p ./roles -r requirements.yml
  ```

- Create a file called `./ssh_keys/authorized_keys.txt` which contains the public
  SSH keys of the users who should be able to log into the `flowkit` account
  once the machine is provisioned.

- Set the environment variables `HOST` and `SSH_PROVISIONING_USER` to the host
  of the machine to be provisioned and the username of the provisioning user.
  Note that this user needs admin permissions because it needs to be able to
  install packages on the system. For a cloud VM the provisioning user will
  typically be the root user.

  You should also set `PROVISIONING_PLAYBOOK` to either `provision.yml` or
  `provision-dev.yml`. In addition to the standard provisioning tasks (the
  same as in `provision.yml`), the latter will also install Python 3 via
  `pyenv` and clone the FlowKit repository at `/home/flowkit/code/FlowKit`.

  Optionally, you can also set the environment variables `FLOWKIT_USER_NAME`
  and `FLOWKIT_USER_PASSWORD_SHA512`. These specify the username and (hashed)
  password of the user account that will install FlowKit. The default username
  is `flowkit` (with the password being the same). Note that the password env
  var must contain the password in _hashed_ form. You can determine this using
  the following command (which presents an interactive prompt where you can
  enter the password):
  ```bash
  pipenv run python -c "from passlib.hash import sha512_crypt; import getpass; print(sha512_crypt.using(rounds=5000).hash(getpass.getpass()))"
  ```
  (See the Ansible
  [FAQ](https://docs.ansible.com/ansible/latest/reference_appendices/faq.html#how-do-i-generate-crypted-passwords-for-the-user-module)
  for alternative methods to determine a SHA512 hash of the password.)

  Here is an example setting the relevant environment variables:
  ```bash
  # Required settings
  export HOST=<some_host_name>
  export SSH_PROVISIONING_USER=root
  export PROVISIONING_PLAYBOOK=provision.yml  # alternatively, use "provision-dev.yml"

  # Optional (only needed if you want to change the username or password).
  # See above how to determine the hashed password.
  export FLOWKIT_USER_NAME=flowkit
  export FLOWKIT_USER_PASSWORD='$6$YaOatFoRa91eOA06$cLJCvJCdd0sLKBEM01eQ2wJ7ZKkTZJz.YWGFK5r0bs4yqiwAz1Lw9pmExiS.PPBBJv13cuBpiHYU88ThX4TeG/'
  ```

- Finally, run the following command (make sure you don't forget the comma
  after `${HOST}` if you type it manually):
  ```bash
  # Option 1 (using the default username and password)
  pipenv run ansible-playbook -i ${HOST}, --user=${SSH_PROVISIONING_USER} ${PROVISIONING_PLAYBOOK}

  # Option 2 (run this line if you changed the username or password above)
  pipenv run ansible-playbook -i ${HOST}, --user=${SSH_PROVISIONING_USER} --extra-vars="username=${FLOWKIT_USER_NAME} password=${FLOWKIT_USER_PASSWORD_SHA512}" ${PROVISIONING_PLAYBOOK}
  ```

This has been tested with CentOS Linux release 7.5.1804.
