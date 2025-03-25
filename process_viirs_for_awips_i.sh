#!/bin/bash
# NOTE: If you add or subtract products, then you also need to modify the script on the ldm server (cira-ldm1)
/home/mniznik/polar2grid_v_2_3/bin/polar2grid.sh viirs scmi \
    --sector-id LCC \
    -g us_viirs2awips_i \
    -f $1 \
    -p i01 i02 i03 i04 i05 \
    --grid-configs grids.conf ~/niznik_grids.conf \
    --tiles 10 14