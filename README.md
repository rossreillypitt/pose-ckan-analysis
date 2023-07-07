Hello! 

This code was used to perform the analysis of CKAN instances found here: 
https://civicdataecosystem.org/2023/06/19/analysis.html

A few notes:
* The analysis was conducted earlier this year and represents a snapshot in time. Sites that were down at the time of the analysis may have since been restored, or sites that were active at the time may not currently be live. 
* Additionally, the analysis uses elements in the metadata as proxies for different attributes of the data portals (as discussed in the article, e.g., oldest metadata creation date as a proxy for age). Changes in metadata could impact the results of future analysis using the same code.  
* The URLs analyzed from datashades.info/portals were copied manually from the site. The copied text is included in this repository ('shades.csv'), and a function in the code cleans up extraneous characters that prevent the URLs from being read in python. 
* The URLs from dataportals.org are taken directly from a CSV link provided by the site (url in code). 
