import gzip
import shutil

with gzip.open('input/grids_germany_annual_air_temp_max_202017.asc.gz', 'rb') as f_in:
    with open('output/grids_germany_annual_air_temp_max_202017.asc', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)