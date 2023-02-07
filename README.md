# Harvest data from Te Papa's collections API

These scripts query Te Papa's API, aggregate search results or multiple record queries, and write out transformed data to a CSV file.

## Files
- askCO.py creates queries from provided parameters, sends the query, and returns results as an Python-friendly object
-- This script can be used on its own to make search and record queries, if you want
- TePapaHarvester.py calls askCO, and pulls specified fields from the returned record, preparing them for mapping and writing
-- co_harvest_mode.json contains the list of specified fields, and how to treat them (single value, list, dict)
- writeapidata.py calls the harvester, applies mapping and transformations to the returned values, and writes each record to a CSV
- writegoogledata.py does the same, but specifically for loading new items to Google Arts and Culture
-- mapping-apitogoogle.txt describes the mapping between the API and Google Arts

## Set up
Add the scripts to a directory, and install:
- Requests

Create two subdirectories:
- input_files
- output_files

Go to https://data.tepapa.govt.nz/docs/register.html and register for an API key. Add the API key to an environment variable called 'TE-PAPA-KEY'.

You need to give the **write** script some search parameters, or a source file with records to look up. Define these in the **Script config** section at the top of the script.

You can set the script to skip records you don't want to harvest. Set "skipuploads" to True, and add a textfile to the directory that contains the records' IRNs. Point to this file using "skipfile" under config.

Set "quiet" to False if you want to print various progress messages.

## Search config
Currently, you can only search for collection objects, not other endpoints like agents or places.

- Set "mode" to "search"
- Set "source" to None
- Set "collection" to the name of the Te Papa collection you want to search. This needs to be the name used in the API's **collection** field, so "PacificCultures", not "Pacific Cultures"

The other search parameters are:
- query - use "*" for a wildcard search
- fields - if you want to return only a subset of fields, list them like so: "id,title,production,_meta"
- q_from - used in paginating results, keep at 0
- size - number of records returned in each page of results
- sort - set the order in which records are returned. For example, to return newest records first, set to [{"field": "_meta.created", "order": "desc"}]

Filters are a list of dictionaries that let you narrow down your result set in a lot of ways, but especially help ensure you are only returning object records, and not things like publications and topics. When searching, make sure you include:
- (for humanities): {"field": "type", "keyword": "Object"}, {"field": "additionalType", "keyword": "PhysicalObject"}
- (for sciences): {"field": "type", "keyword": "Specimen"}

If searching a specific collection, include the filter {"field": "collection", "keyword": "{}".format(self.collection)}.

To get just records with images that can be downloaded, include {"field": "hasRepresentation.rights.allowsDownload", "keyword": "True"}.

## List config
You can provide the script a list of IRNs in a textfile, or a CSV that has columns for record and media IRNs.

CSVs should use the column names "record_irn" and "media_irn".

- Set "mode" to "list"
- Put the source file in the /input_files directory, and set "source" to the filename, including "/input_files/"
- Set "collection" to None

## Run
Run the script from the command line. Querying the API take a little time to avoid sending too many requests.

The write file will be created in **/output_files** with a datestamp in the filename.