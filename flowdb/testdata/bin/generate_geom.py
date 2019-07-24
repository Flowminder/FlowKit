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
    for r in range(0, rows * 2, 2):
        multiplier = floor(points / 3)
        
        line = []
        line.append(geom[r])    # Start with on point so we get the total divided by a third
        line.extend([geom[r]] * multiplier)
        line.extend([geom[r+1]] * multiplier)
        line.extend([geom[r]] * multiplier)
        
        lines.append(' '.join(line))
        
    return "a", lines
 
def type_b(rows, points, geom):
    """Variant B - all points are in the same location, each row will be in a different location"""
    lines = []
    
    for r in range(1000, 1000 + (rows * 10), 10):
        lines.append(' '.join([geom[r]] * points))
    
    return "b", lines
 
# Distribute over a longer commute - passing through multiple locations., but still home/work balance
def type_c(rows, points, geom):
    lines = []
    return "c", lines

# Over the course of a day go through multiple cells at one (e.g. delivery driver)
def type_d(rows, points, geom):
    lines = []
    return "d", lines
 
def getType(type, rows, points, geom):
    types = {
        0: type_a,
        1: type_b,
        2: type_c,
        3: type_d
    }
    
    func = types.get(type)
    
    return func(rows, points, geom)

if __name__ == "__main__":
    dir = os.path.dirname(os.path.abspath(__file__))
        
    geom = [line.strip() for line in open(f"{dir}/../synthetic_data/data/geom.dat", "r")]
    
    rows = 50
    points = 100
    
    for v in range(0, 4):
        name, lines = getType(v, rows, points, geom)
        path = f"{dir}/../synthetic_data/data/variations/{name}.dat"
        
        # Remove any existing files
        if os.path.exists(path): os.remove(path)
        
        # Loop through the lines to input into file
        if (len(lines) > 0):
            with open(path, "w+") as f:
                for l in lines:
                    f.write(f"{l}\n")
            
            f.close()
