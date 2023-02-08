# Config options for harvesting and writing data from Te Papa API

mode = "list" # Can be "list" or "search"
quiet = True

skipuploads = True
skipfile = "20231601-existinguploads.txt"
input_dir = "/input_files"
output_dir = "/output_files"

# if mode == "list"
list_source = "Birds_pickeroutput.csv"

# if mode == "search"
collection = None
filter_freedownloads = True
filter_other = None
q = "*"
fields = None
start = 0
size = 500
sort = [{"field": "id", "order": "asc"}]