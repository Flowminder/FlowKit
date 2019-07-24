# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# !/usr/bin/env python

"""
This is a small script that will take the geom.dat file and create movement variants based on the data points it
contains. The geom.dat can be created using the following query: 

SELECT st_dumppoints(ST_GeneratePoints(geom, 10000)) AS p from geography.admin0

Which will create 10000 lines of points within the admin0 defined geom polygon.

The premise of this approach is that we're trying to model variance in subscribers daily behaviour, so the
we create four different daily patterns of movement (variants):

Type A - normal movement through two locations with a day broken in thirds, home/work/home.
Type B - minimal movement, all points in the same location.
Type C - greater movement, home/work/home but passing through multiple locations
Type D - maximum movement through up to n locations per day.

This data can then be assigned to subscribers so that when they are selected to create a call, the
location variation can be selected accordingly.
"""

import os
from math import floor

# Variant types
def type_a(rows, points, geom):
    """Variant A will be the modal type - simple movement between two cells, thirds per day home/work/home"""

    lines = []
    mult = floor(points / 3)
    for r in range(0, rows * 2, 2):
        line = []
        line.append(
            geom[r]
        )  # Start with on point so we get the total divided by a third

        home = [geom[r]] * mult  # Generate the home points

        line.extend(home)
        line.extend([geom[r + 1]] * mult)  # Generate the work points
        line.extend(home)

        lines.append(" ".join(line))

    return "a", lines


def type_b(rows, points, geom):
    """Variant B - all points are in the same location, each row will be in a different location"""
    lines = []

    for r in range(1000, 1000 + (rows * 10), 10):
        lines.append(" ".join([geom[r]] * points))

    return "b", lines


def type_c(rows, points, geom):
    """
    Variant C - distribute over a longer commute - passing through multiple locations in the day, but still based on the 
    third home/work/home division
    """
    lines = []
    mult = floor(points / 3)
    start_index = len(geom) - 1  # Start at the end to use different point data

    for r in range(start_index, start_index - (rows * 10), -10):
        line = []
        home = [geom[r]] * mult  # Generate the home points

        line.extend(home)

        # The remaining "work" time is completely variable
        work = []
        for p in range(r - 1, r - 1 - (points - (2 * mult)), -1):
            work.append(geom[p])

        # Add work, then home
        line.extend(work)
        line.extend(home)

        lines.append(" ".join(line))

    return "c", lines


def type_d(rows, points, geom):
    """
    Variant D - move through a lot of points over the course of the day, making each of the points different
    """
    lines = []
    start_index = 2000  # Start at this row to mix up the point mix
    for r in range(0, rows):
        line = []
        for p in range(start_index + (r * points), start_index + points + (points * r)):
            line.append(geom[p])

        lines.append(" ".join(line))

    return "d", lines

# The getType method aids selection of type
def getType(type, rows, points, geom):
    types = {0: type_a, 1: type_b, 2: type_c, 3: type_d}

    func = types.get(type)

    return func(rows, points, geom)

if __name__ == "__main__":
    dir = os.path.dirname(os.path.abspath(__file__))

    geom = [
        line.strip() for line in open(f"{dir}/../synthetic_data/data/geom.dat", "r")
    ]

    rows = 50
    points = 100

    for v in range(0, 4):
        name, lines = getType(v, rows, points, geom)
        path = f"{dir}/../synthetic_data/data/variations/{name}.dat"

        # Remove any existing files
        if os.path.exists(path):
            os.remove(path)

        # Loop through the lines to input into file
        if len(lines) > 0:
            with open(path, "w+") as f:
                for l in lines:
                    f.write(f"{l}\n")

            f.close()
