To provision the machine, set the environment variables `HOST` and `SSH_USER` and run the following command (make sure you don't forget the comma after `${HOST}` if you type it
manually):
```
pipenv install
pipenv run ansible-playbook -i ${HOST}, --user=${SSH_USER} provision.yml
```
This has been tested with CentOS Linux release 7.5.1804.
