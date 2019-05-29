# Mapbox GL for map visualisations in the worked examples notebooks

Date: 14 May 2019

## Status

Accepted

## Context

We tried three different visualisation libraries for producing the maps in the worked examples notebooks (Folium, GeoViews and Mapbox). Another possibility is `deck.gl`/`kepler.gl` for Jupyter notebooks (https://github.com/uber/deck.gl/issues/2929, https://github.com/keplergl/kepler.gl/issues/331), but this has not yet been released at time of making the decision.

Below is a summary of the current pros/cons of each. All three packages are continuing to develop, so pros and cons are likely to change in the near future.

### [Folium](https://python-visualization.github.io/folium/)

Pros:

- Has been around longer than mapboxgl-jupyter, so there are more plugins and additional features available.

Cons:

- Poor performance for large datasets.
- Large choropleths do not appear in Chrome.

### [GeoViews](http://geo.holoviews.org/)

Pros:

- Simple to produce maps with drop-down selectors and time sliders, with minimal code.
- Part of the PyViz project, so designed to work nicely with many other packages in the Python data science ecosystem.

Cons:

- Dependency on `Cartopy`, which has non-Python dependencies `GEOS` and `proj` (and a potential DLL conflict with the GEOS version included with `shapely`), which makes it very difficult to install with pip/pipenv (although it's straightforward with conda).
- Doesn't have an equivalent of the "heatmap" visualisations offered by `Folium` and `Mapbox`.
- Very difficult to embed the resulting maps in `nbconvert` markdown output (although it works fine in `nbconvert` html output).

### [Mapbox GL](https://mapbox-mapboxgl-jupyter.readthedocs-hosted.com/en/latest/)

Pros:

- Better performance than Folium.
- Maps are easily embedded in the docs pages, and appear in all browsers checked (Chrome, Firefox, Safari).

Cons:

- Currently no support for multi-layered maps or time sliders.
- Requires an API token to produce visualisations.

## Decision

We will use Mapbox to produce visualisations in the worked examples. As all packages considered continue to develop, we may decide a different package better suits our needs in the future.

## Consequences

The lack of support for multi-layered maps or time sliders limits the visualisations that can be embedded in the docs pages, although users running the notebooks in the demo setup can still manually change parameters to view, e.g., heatmaps for different times of day.

The need to generate a Mapbox API token adds an additional step for users spinning up a demo system, if they want to be able to fully run the worked examples notebooks.
