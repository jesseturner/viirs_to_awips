#!/bin/bash
# NOTE: If you add or subtract products, then you also need to modify the script on the ldm server (cira-ldm1)
#--- Copied from data2/mniznik on jpss-cloud4 machine
/local/polar2grid_v_3_1/bin/polar2grid.sh viirs scmi \
    --sector-id LCC \
    -g us_viirs2awips_m \
    -f $1 \
    -p m08 m11 m13 m14 m15 m16 \
    --grid-configs grids.conf niznik_grids.conf \
    --tiles 5 7