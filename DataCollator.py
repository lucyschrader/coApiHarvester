# -*- coding: utf-8 -*-

import csv
import TePapaHarvester
import harvestconfig as hc

quiet = hc.quiet
input_dir = hc.input_dir
output_dir = hc.output_dir

harvester = TePapaHarvester.Harvester(quiet=quiet, sleep=0.1)

# Object calling the TePapaHarvester script and holding the aggregated data
class RecordData():
	def __init__(self):
		self.mode = hc.mode
		self.source = input_dir + "/" + hc.list_source
		self.collection = hc.collection
		self.skipuploads = hc.skipuploads
		self.skiplist = []

		self.source_rows = None
		self.harvest_all_media = False
		self.records = None

		if self.skipuploads == True:
			self.populate_skiplist()

		if self.mode == "list":
			self.collection = "fromlist"
			self.harvest_list()
		elif self.mode == "search":
			self.harvest_search()

		self.qual_range = self.qual_range()

	def populate_skiplist(self):
		with open(input_dir + "/" + hc.skipfile, 'r', encoding="utf-8") as f:
			lines = f.readlines()

		for line in lines:
			self.skiplist.append(int(line.strip()))

		if quiet == False:
			print("Skiplist populated")

	# Builds a list of queriable IRNs from source file and sends to harvester script
	def harvest_list(self):
		resource_type = "object"
		irns = []

		# For CSV files
		# Record IRN column must be titled "record_irn"
		# Media IRN column must be titled "media_irn"
		# Can include identifier of the item on Google Arts, column titled "itemid"
		if self.source.endswith(".csv"):
			self.source_rows = []
			with open(self.source, newline="", encoding="utf-8") as f:
				reader = csv.DictReader(f, delimiter=",")
				for row in reader:
					if "itemid" in row:
						this_itemid = row["itemid"].strip()
					else:
						this_itemid = None
					this_record_irn = int(row["record_irn"].strip())
					this_media_irn = int(row["media_irn"].strip())

					# Checks to see if the image has been excluded using the Picker tool
					if row.get("media_include") != "n":
						self.source_rows.append({"itemid": this_itemid, "record_irn": this_record_irn, "media_irn": this_media_irn})

			for row in self.source_rows:
				this_irn = row.get("record_irn")
				# Checks if the record should be skipped, otherwise includes row's record IRN in irns list
				if self.skiplist == True:
					if this_irn in self.skiplist:
						break
				if this_irn not in irns:
					irns.append(this_irn)

		# For TXT files
		# Record IRNs only, one per line
		elif self.source.endswith(".txt"):
			self.harvest_all_media = True
			with open(self.source, 'r', encoding="utf-8") as f:
				lines = f.readlines()

				for line in lines:
					irns.append(int(line.strip()))

		if quiet == False:
			print("Harvesting data for {} records".format(len(irns)))

		self.records = harvester.harvest_from_list(resource_type=resource_type, irns=irns)
		
	def harvest_search(self):
		self.harvest_all_media = True

		filters = [{"field": "collection", "keyword": "{}".format(self.collection)}]

		if self.collection in ["Art", "CollectedArchives", "History", "MuseumArchives", "PacificCultures", "Philatelic", "Photography", "RareBooks", "TaongaMāori"]:
			filters.append({"field": "type", "keyword": "Object"})
			filters.append({"field": "additionalType", "keyword": "PhysicalObject"})
		elif self.collection in ["Archaeozoology", "Birds", "Crustacea", "Fish", "FossilVertebrates", "Geology", "Insects", "LandMammals", "MarineInvertebrates", "MarineMammals", "Molluscs", "Plants", "ReptilesAndAmphibians"]:
			filters.append({"field": "type", "keyword": "Specimen"})

		if hc.filter_freedownloads == True:
			filters.append({"field": "hasRepresentation.rights.allowsDownload","keyword": "True"})

		if hc.filter_other is not None:
			filters.append(hc.filter_other)

		harvester.set_params(q=hc.q, fields=hc.fields, filters=filters, facets=None, start=hc.start, size=hc.size, sort=hc.sort)
		harvester.count_results()

		self.records = harvester.harvest_records()

	def qual_range(self):
		lowest = 0
		highest = 0
		record_scores = []
		for key in self.records.keys():
			score = self.records[key]["qualityScore"]
			record_scores.append(score)
		
		sorted_records = sorted(record_scores, reverse=False)
		lowest = sorted_records[0]

		sorted_records = sorted(record_scores, reverse=True)
		highest = sorted_records[0]

		if quiet == False:
			print("Quality range for these records")
			print("Lowest: {}".format(str(lowest)))
			print("Highest: {}".format(str(highest)))

		return (lowest, highest)