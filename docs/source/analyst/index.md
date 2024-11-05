Title: Analyst

# FlowKit for Analysts

It is recommended to use the [FlowClient](#flowclient) provided with FlowKit. Advanced users may want to create their own client interfacing to [FlowAPI](#flowapi).

## FlowClient

FlowClient is a client to FlowAPI written in Python for use with the JupyterLabs Notebook Python Data Science Stack. It can be installed using pip:

```bash
pip install flowclient
```

### Example FlowClient usage

Documentation for FlowClient can be found [here](../flowclient/flowclient/). Example usage of FlowClient to run daily location, modal location and flow queries is provided [here](flowclient/example_usage.ipynb), and worked examples are available [here](worked_examples/index.md).

## FlowAuth

To connect FlowClient to FlowAPI, an access token must be generated using FlowAuth. Once an administrator has created a FlowAuth login for a user (see instructions [here](../administrator/index.md#granting-user-permissions-in-flowauth)), that user can follow these steps to generate a token:

1. Log into FlowAuth using the username and password created by the administrator.

2. Optionally, click on the person icon (top right) and reset password.

3. Select the server under "My Servers".

4. Click the '+' icon to add a token, and give it a name and the `viewer` and `runner` roles. And Save.

5. Click "COPY" to copy the token string, "DOWNLOAD" to download the token as a text file, or "VIEW" to display the token string.

## FlowAPI

Advanced users may wish to write their own clients that interface directly to FlowAPI. This is discussed in more detail in the [Developer](../developer/index.md) section of these documents.
