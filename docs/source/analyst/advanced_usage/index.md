Title: Advanced usage

# Working with Flowmachine

In some scenarios, the aggregates provided through FlowAPI will not be sufficient to meet your analytical needs. When this happens it is possible to use Flowmachine _directly_, which allows you to work with individual level data, build or customize metrics, and run arbitrary SQL.

Just like Flowclient, you can install Flowmachine using pip:

```bash
pip install flowmachine
```

See the [mobile data usage example](worked_examples/mobile-data-usage.ipynb) for a case which uses Flowmachine directly to produce a metric.

As with the [other worked examples](../worked_examples), you can try this using the [quick start setup](../../../install.md#quickinstall) which includes JupyterLab. Alternatively, the Jupyter notebooks for this examples can be found [here](https://github.com/Flowminder/FlowKit/tree/master/docs/source/analyst/advanced_usage/worked_examples/).