

#!/bin/bash

# Initial string with the placeholder
url="https://cw3e-datashare.ucsd.edu/Rutz_AR_Catalog/Rutz_ARCatalog_MERRA2_{yyyy}.nc"

# Loop from 2004 to 2024
for year in {2004..2024}
do
    # Replace {yyyy} with the current year
    edited_url=${url//\{yyyy\}/$year}
    
    # Print the edited string
    echo "$edited_url"

    wget --no-check-certificate "$edited_url"
done