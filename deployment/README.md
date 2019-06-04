## Quick start

Make sure you have completed the initial setup steps below (this is only needed once).

Then run the following commands to provision the machine.
```bash
export HOST=<host_name_or_ip_address>
export SSH_PROVISIONING_USER=root
export PROVISIONING_PLAYBOOK=provision.yml  # alternatively, use "provision-dev.yml"

# Use the values below to keep the default username/password (`flowkit:flowkit`), or
# change them to use different values. See below how to determine the hashed password.
export FLOWKIT_USER_NAME=flowkit
export FLOWKIT_USER_PASSWORD='$6$YaOatFoRa91eOA06$cLJCvJCdd0sLKBEM01eQ2wJ7ZKkTZJz.YWGFK5r0bs4yqiwAz1Lw9pmExiS.PPBBJv13cuBpiHYU88ThX4TeG/'

# Run the provisioning playbook
pipenv run ansible-playbook -i ${HOST}, --user=${SSH_PROVISIONING_USER} \
    --extra-vars="username=${FLOWKIT_USER_NAME} password=${FLOWKIT_USER_PASSWORD_SHA512}" \
    ${PROVISIONING_PLAYBOOK}
```

See below for an explanation of the different environment variables and how to generate
a hashed password for the `flowkit` user account.

## First-time setup steps

The following steps only need to be done once.

- Set up the pipenv environment and install auxiliary Ansible roles.
  ```bash
  pipenv install
  pipenv run ansible-galaxy install -p ./roles -r requirements.yml
  ```

- Create a file called `./ssh_keys/authorized_keys.txt` which contains the public
  SSH keys of the users who should be able to log into the `flowkit` account
  once the machine is provisioned.

## Meaning of the environment variables

- `HOST`: the hostname (or IP address) of the machine to be provisioned.

- `SSH_PROVISIONING_USER`: the user as which the provisioning steps in the Ansible playbook are run.

  Note that this provisioning user needs admin permissions because it needs to be able to
  install packages on the system. For a cloud VM the provisioning user will
  typically be the root user.

- `PROVISIONING_PLAYBOOK`: the Ansible playbook used to provision the machine. There are two options:

   - `provision.yml`: this performs the standard provisioning tasks needed to get the machine into a state
     so that FlowKit can be installed and used on it. This mainly installs `docker` and `docker-compose`
     and sets up  a `flowkit` user account (also see the `FLOWKIT_USER_NAME` and `FLOWKIT_USER_PASSWORD_SHA512`
     environment variable below).

   - `provision-dev.yml`: in addition to the standard tasks performed by `provision.yml`, this performs
     additional steps which are useful to do development on FlowKit: it installs Python 3 via `pyenv`
     and clones the FlowKit repository at `/home/flowkit/code/FlowKit`.

- `FLOWKIT_USER_NAME` and `FLOWKIT_USER_PASSWORD_SHA512`: these specify the username and (hashed) password
   of the user account that will install and run FlowKit. The default values given in the "Quick Start"
   section above represent the username/password `flowkit:flowkit`, but this can be changed by setting these
   variables to different values. Note that the password env var must contain the password in _hashed_ form.
   You can determine this using the following command (which presents you with an interactive prompt to enter the password):
   ```bash
   pipenv run python -c "from passlib.hash import sha512_crypt; import getpass; print(sha512_crypt.using(rounds=5000).hash(getpass.getpass()))"
   ```
   See the Ansible
   [FAQ](https://docs.ansible.com/ansible/latest/reference_appendices/faq.html#how-do-i-generate-crypted-passwords-for-the-user-module)
   for alternative methods to determine a SHA512 hash of the password.

This has been tested with CentOS Linux release 7.5.1804.
