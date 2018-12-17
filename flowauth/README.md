# FlowAuth

Documentation for FlowAuth can be found [here](https://flowminder.github.io/FlowKit/flowauth).


## Quick setup to run the Frontend tests interactively

For development purposes, it is useful to be able to run the Flowauth frontend tests interactively.

- As an initial step, ensure that all the relevant Python and Javascript dependencies are installed.
```
cd /path/to/flowkit/flowauth/
pipenv install

cd /path/to/flowkit/flowauth/frontend
npm install
```

- The following command sets both the flowauth backend and frontend running (and also opens the flowauth web interface at `http://localhost:3000/` in the browser).
```
cd /path/to/flowkit/flowauth/
pipenv run start-all
```

- To open the Cypress UI, run the following in a separate terminal session:
```
cd /path/to/flowkit/flowauth/frontend/
npm run cy:open
```

- You can then click the button "Run all specs", or select an individual spec to run only a subset of the tests.
