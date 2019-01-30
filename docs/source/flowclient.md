# FlowKit for Analysts

It is recommended to use the [FlowClient](#flowclient) provided with FlowKit. Advanced users may want to create their own client interfacing to [FlowAPI](#flowapi).

<a name="flowclient">

## FlowClient

FlowClient is a client to FlowAPI written in Python for use with the JupyterLabs Notebook Python Data Science Stack. It can be installed using pip:

```bash
pip install flowclient
```

### Example FlowClient usage

Example usage of FlowClient to run daily location, modal location and flow queries is provided [here](../analyst/example_usage/).


<a name="flowapi">

## FlowAPI

Advanced users may wish to write their own clients that interface directly to FlowAPI. This is discussed in more detail in the [Developer](developer/roadmap.md) section of these documents.
