#!/bin/bash
# NOTE: If you add or subtract products, then you also need to modify the script on the ldm server (cira-ldm1)

#--- More info on tiles and AWIPS-specific script:
#------ https://www.ssec.wisc.edu/software/polar2grid/writers/awips_tiled.html

/local/polar2grid_v_3_1/bin/polar2grid.sh \
    -r viirs_sdr \
    -w awips_tiled \
    --sector-id LCC \
    -g us_viirs2awips_m \
    -f $1 \
    -p m08 m10 m11 m12 m13 m14 m15 m16 \
    --grid-configs grids.conf ~/VIIRS_to_AWIPS/niznik_grids.conf \
    --tiles 5 7 \
    --filter-day-products 0 \