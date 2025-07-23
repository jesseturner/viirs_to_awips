#!/bin/bash
# NOTE: If you add or subtract products, then you also need to modify the script on the ldm server (cira-ldm1)

/local/polar2grid_v_3_1/bin/polar2grid.sh \
    -r viirs_sdr \
    -w awips_tiled \
    --sector-id LCC \
    -g us_viirs2awips_i \
    -f $1 \
    -p i01 i02 i03 i04 i05 \
    --grid-configs /mnt/data1/jturner/niznik_grids.conf \
    --tiles 10 14 \
    --filter-day-products 0 \
    --grid-coverage 0.0001 \

#--- None of these worked to get I01, I02, I03 at night
    # --check-categories False \
    # --no-check-categories \
    # --sza-threshold 180 \
    # --filter-night-products 0 \