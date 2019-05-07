#!/usr/bin/env bash
eval $( fixuid )

/bin/bash /defaultentrypoint.sh "$@"