# FlowKit - CDR Analytics Toolkit

[![CircleCI](https://circleci.com/gh/Flowminder/FlowKit.svg?style=shield)](https://circleci.com/gh/Flowminder/FlowKit) [![Build Status](https://img.shields.io/travis/com/Flowminder/FlowKit.svg?style=flat-square)](https://travis-ci.com/Flowminder/FlowKit) [![codecov](https://codecov.io/gh/Flowminder/FlowKit/branch/master/graph/badge.svg)](https://codecov.io/gh/Flowminder/FlowKit) [![Join the chat at https://gitter.im/Flowminder/FlowKit](https://badges.gitter.im/Flowminder/FlowKit.svg)](https://gitter.im/Flowminder/FlowKit?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![License: MPL 2.0](https://img.shields.io/github/license/Flowminder/FlowKit.svg?style=flat-square)](https://opensource.org/licenses/MPL-2.0) [![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/python/black)

## What is FlowKit ?

FlowKit is a platform for analysis of Call Detail Records (CDR) and other data. CDR data is created by mobile network operators (MNOs) primarily for generating subscriber bills and settling accounts with other carriers.

FlowKit is designed to extend CDR data analysis to meet many other applications beyond billing. Some examples include disaster response, precision epidimiology and transport and mobility, more examples can be found here.

CDRs constitute a highly sensitive data set, so FlowKit is designed with privacy protection in mind. It includes the FlowAuth framework to enable fine-grained authorization with extensive access logging, making it an important tool for deployment of a GDPR compliant CDR analysis system.

### Documentation

The FlowKit documentation is available [here](https://flowminder.github.io/FlowKit/).

### Development status and installation

FlowKit is under ongoing development. The list of releases can be found [here](https://github.com/Flowminder/FlowKit/releases). Until FlowKit reaches full stable release status, we recommend installing the latest version based on the Github master branch. For details see the installation instructions [here](https://flowminder.github.io/FlowKit/install/).

### Benchmarks

There is a suite of benchmarks for FlowKit at https://github.com/Flowminder/FlowKit-benchmarks.<br>
The benchmark results can be seen at https://flowminder.github.io/FlowKit-benchmarks.
