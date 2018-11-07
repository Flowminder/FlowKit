This is a placeholder directory. Before running the ansible playbooks to
provision a given host, you should add a file called `authorized_keys.txt`
in this folder. This file should contain one (or multiple) lines with the
public SSH keys for all users that should be able to log into the `flowkit`
user account that will be created as part of the provisioning process.
These keys will be placed in the file `/home/flowkit/.ssh/authorized_keys`.
on the host during provisioning.
