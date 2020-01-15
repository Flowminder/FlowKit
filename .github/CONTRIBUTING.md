# Contributing to FlowKit

Greetings, and thanks for your interest in contributing to FlowKit!

Here are some important resources:

  * [Flowminder](https://flowminder.org) is the originator of the FlowKit project
  * [Documentation](https://flowkit.xyz) is the online documentation
  * [Gitter](https://gitter.im/Flowminder/FlowKit) for chat and discussions

## Testing

We :green_heart: testing and CI. We mostly use [pytest](https://docs.pytest.org/en/latest/) for testing the Python parts of the codebase, and [Cypress.io](https://cypress.io) to test our Javascript components. You can take a look at our CI pipeline on [CircleCI](https://circleci.com/gh/Flowminder/workflows/FlowKit), [Travis](https://travis-ci.com/Flowminder/FlowKit), and [Cypress](https://dashboard.cypress.io/projects/67obxt/runs) to get a sense of how we test the complete project.

Because FlowKit includes several components, each one has it's own tests, which usually live in the `tests` subdirectory of the component under the main repo.

To run the tests yourself, or start hacking on the code, you'll want to follow the instructions for [setting up  a developer environment](https://flowkit.xyz/developer/dev_environment_setup/).

## Submitting changes :spanner:

For small changes to the code or documentation (e.g. correcting a typo), please feel free to open a pull request directly.

For anything more significant, it is really helpful to [open an issue](https://github.com/Flowminder/FlowKit/issues/new/choose) first, to put it on the project's radar and facilitate discussion.

Once you've opened an issue, make sure to reference the issue number liberally in your commit messages and any pull request to help join everything up :grinning:

Before any change gets merged into the codebase, it needs to:

- Pass CI
- Not reduce the test coverage (without a really good reason)
- Be up to date with master branch
- Be reviewed!
- Be labelled as 'ready-to-merge'
- Have license headers added if necessary
- Have a changelog entry

We try where possible to make sure pull requests are reviewed by two people, and ideally by three - including the person who's making the request.

Once a pull request is marked as ready, approved and all tests are passing, it will get merged automatically. The automatic merge process will also (try) to ensure the branch is up to date with master, but on occasion you may need to intervene to make sure.

When preparing your branch for a pull request, it would be awesome if you took the time to clean up the history, and in general we prefer that you have kept up to date with master by rebasing as often as necessary.
    
## Reporting bugs :bug:

We :heart: bug reports. Please have a quick look through the [issue tracker](https://github.com/Flowminder/FlowKit/issues?utf8=✓&q=is%3Aissue+is%3Aopen+label%3Abug+) to see if you're hitting something new, and [open up a new one](https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md) if you don't find anything.

It is super helpful if you follow the template provided, but don't worry to much if you can't fill out all the fields. 

## Requesting new features :rocket:

Flowminder actively supports FlowKit, and we're always interested to hear suggestions for new features or enhancements. Check the [issue tracker](https://github.com/Flowminder/FlowKit/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement) to see if something is already on our radar and add a :+1: as a reaction if you think we should focus on it.

You can also [request one](https://github.com/Flowminder/FlowKit/issues/new?template=feature_request.md) if nothing already listed matches what you have in mind, and get the conversation started.

## Coding style

For Python, we use [black](https://github.com/python/black), and for JS/JSX we use [prettier](https://github.com/prettier/prettier).


