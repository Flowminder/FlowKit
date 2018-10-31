# Test Data Images

Flowdb images which integrate test data. At this time, two images are available - the basic testdata image (`Dockerfile.testdata`), which contains 7 days of low volume calls and sms, Nepal admin boundaries, digital elevation model, worldpop raster layer, and open street map routing; and testdata_medium (`Dockerfile.synthetic_data`), which contains substantially higher call volumes and considerably more cell sites by default, and can be used to generate arbitrary volumes of call data.

The Dockerfiles and supplementary materials for these images live in this subdirectory, because they do not rely on any of the build context for the base image, and the base image does not rely on their build context.