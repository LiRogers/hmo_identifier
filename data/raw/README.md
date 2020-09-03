# HMO Identifier Raw Data

## Reference

Reference data on different London geographies (boroughs, wards and output areas) to help with gathering and joining data.
Contains geography name and code, parent geography code if relevant, and geometry.
This data is gathered from [ONS Geography Linked Data](http://statistics.data.gov.uk).
It can be reproduced using `hmo_identier.data.reference`.

## Open

Open data which can be gathered through APIs or otherwise on the web.
Each dataset is broadly as provided with little cleaning and processing.
It is likely that borough held data can replace or supplement these datasets.
The datasets can be reproduced using `hmo_identifier.data.open`.

#### Airbnb

This data is scraped from property listings on [Airbnb](https://www.airbnb.co.uk/)
and published by [Inside Airbnb](http://insideairbnb.com).
Disclaimers about this data can be found [here](http://insideairbnb.com/about.html#disclaimers),

#### Census

This is data from the 2011 census including:
* Household composition by number of bedrooms
* Household composition by occupancy rating
* Tenure by occupancy rating
At the OA level. The data is gathered from [nomis](https://www.nomisweb.co.uk/).

#### Crime

Street level crime data for the previous 12 months gathered from the [Police API](https://data.police.uk/).


#### Index of Multiple Deprivation (IMD)

IMD 2019 at LSOA level gathered from [here](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019).

#### Energy Performance Certificates (EPC)

Domestic EPC data gathered from [here](https://epc.opendatacommunities.org/), an API key is required.

#### Land Registry

Land Registry Price Paid data, available from [here](https://landregistry.data.gov.uk/).

## Local

Data available locally. As examples of some relevant datasets, code to fetch data on 
social housing and registered HMOs in Camden from the
[Camden Open Data Portal](https://opendata.camden.gov.uk/) is included in `hmo_identifier.data.local`.
These functions can be replaced by functions to fetch data available locally to your team.
Other useful datasets include:
* Gazatteer/Address Base of residential properties.
* Housing benefit/Universal credit
* Electoral roll
* Environment Services
* Council tax
* Tenancy Deposit schemes

Any other data held that would help determine or give an indication of:
* Property structure
* Property tenure
* Number of residents
* Turnover of residents
* Evidence of a negligent landlord.
* Housing pressure.

## GLA

In addition to the datasets detailed above, the GLA is able to share the 
[UK Buildings](https://www.geomni.co.uk/ukbuildings) dataset with London Boroughs,
please contact us for more details.
This dataset contains information on building structure and can be matched to UPRN.

The code to fetch this data, along with the GLA's copy of the gazatteer is included in `hmo_identifier.data.gla`.
This will need to be adjusted for your local set up.




 
