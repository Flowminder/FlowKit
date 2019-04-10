# Documentation

FlowKit's documentation is built using [Mkdocs](http://mkdocs.org).

## Build

### Requirements

- Docker
- Pipenv

To build the docs, first install the necessary dependencies by running `pipenv install` (in the `docs/` subfolder). Once this completes, you can build the docs by running `pipenv run build`.

This will create any necessary Docker containers, run all notebooks, generate markdown for the APIs, and finally build all the resulting markdown to HTML. Built docs will be in the `flowkit-docs` directory.

For local development, you can also run `pipenv run serve`, which will build the docs, and then start a live reloading web server. You can view the docs at http://localhost:8000, and any changes you make to markdown files in the `build` directory will be reflected immediately.

### Editing

To edit the docs, you should make changes to the `source` directory.

While editing, you can use `pipenv run serve` to get an automatically rebuilding and reloading version of the docs, by default accessible at http://localhost:8000.

### Build Process

The `build.sh` script:

1. If not on CircleCI, runs docker-compose to bring up a redis container, and the FlowDB synthetic data container.
1. Uses `nbconvert` on all notebooks found in `source` and (optionally) executes them.
1. Generates markdown API documentation for FlowMachine at `build/components/flowmachine`
1. Generates markdown API documentation for FlowClient at `build/components/flowclient`
1. Builds HTML from the markdown using mkdocs

### Notes

#### nbconvert

By default, Pandas output is as HTML tables, which don't get styled nicely in the resulting markdown.  To avoid this, a modified nbconvert executor (`ExecuteWithPreamble`), from the [nbconvert-utils](https://pypi.org/project/nbconvert-utils/) package is used to inject the [notebook_preamble.py](/notebook_preamble.py) script at the start of each notebook, and hide that cell from the markdown. This filters warnings, makes pandas output appear as markdown tables (which then get rendered nicely by mkdocs), and generates an API access token.

#### API Documentation

Because `sphinx` does not have a markdown builder, the API documentation is generated using [mktheapidocs](https://github.com/greenape/mktheapidocs), which produces markdown from numpy formatted docstrings and uses [numpydoc](https://numpydoc.readthedocs.io) for parsing.

#### Notebooks

In addition to markdown files, Jupyter notebooks can be used. Any notebooks present in the docs directory will be executed and converted automatically as part of the build process.

#### Diagrams

In addition to code blocks, you can also include [mermaid](https://mermaidjs.github.io) diagrams in documentation pages, by enclosing the diagram in a code block:

```markdown

```mermaid
graph TD;
    A-->B;
    A-->C;
    B-->D;
    C-->D;
```

```