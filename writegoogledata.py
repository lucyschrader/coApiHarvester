# -*- coding: utf-8 -*-

import os
from datetime import datetime
import time
import csv
import re
from math import floor
import random
from collections import Counter
import TePapaHarvester

# Script config
# To configure search mode, use RecordData.harvest_search() below

mode = "list"									# Can be "list" or "search"
source = "/input_files/Birds_pickeroutput.csv"				# Use if mode is list
collection = None								# Use if mode is search
skipuploads = True								# Prevents harvest of data for records already uploaded
skipfile = "20231601-existinguploads.txt"
output_dir = "/output_files"

quiet = True
harvester = TePapaHarvester.Harvester(quiet=False, sleep=0.1)

def write_google_data():
	if quiet == False:
		if mode == "list":
			print("Harvesting data for items in", source)
		else:
			print("Harvesting data for items in", collection)

	# Harvests and aggregates data returned by API
	record_data = RecordData(mode=mode, source=source, collection=collection, skipuploads=True)

	# Transforms, maps, and writes data to CSV
	CSV = CSVWriter(record_data=record_data)
	CSV.write_data()

# Object calling the TePapaHarvester script and holding the aggregated data
class RecordData():
	def __init__(self, mode=None, source=None, collection=None, skipuploads=True):
		self.mode = mode
		self.source = source
		self.collection = collection
		self.skipuploads = skipuploads
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
		with open(skipfile, 'r', encoding="utf-8") as f:
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
		q = "*"
		fields = None
		q_from = 0
		size = 500
		sort = [{"field": "id", "order": "asc"}]
#		facets = [{"field": "production.spatial.title", "size": 3}]
#		facets = [{"field": "evidenceFor.atEvent.atLocation.country", "size": 3}]
#		filters = [{"field": "hasRepresentation.rights.allowsDownload", "keyword": "True"}, {"field": "collection", "keyword": "{}".format(collection)}, {"field": "type", "keyword": "Object"}, {"field": "additionalType", "keyword": "PhysicalObject"}]
		filters = [{"field": "hasRepresentation.rights.allowsDownload","keyword": "True"}, {"field": "collection", "keyword": "{}".format(self.collection)}, {"field": "type", "keyword": "Specimen"}]

		harvester.set_params(q=q, fields=fields, filters=filters, facets=None, q_from=q_from, size=size, sort=sort)
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

class CSVWriter():
	def __init__(self, record_data):
		self.record_data = record_data
		self.records = record_data.records

		self.saved_places = {}

		current_time = datetime.now.strftime("%d-%m-%Y_%H-%M")

		filename = "/output_files/" + current_time + "_" + self.record_data.collection + "_" + "googledata.csv"

		heading_row = ["itemid", "subitemid", "orderid", "customtext:registrationid", "title/en", "description/en", "creator/en", "location:placename", "location:lat", "location:long", "dateCreated:start", "dateCreated:display", "rights", "format", "medium", "subject", "art=support", "art=depictedLocation:placename", "art=depictedPerson", "art=genre", "customtext:specimenType", "customtext:dateCollected", "customtext:creator.collector", "customtext:locationCollected", "customtext:dateIdentified", "customtext:creator.identifier", "customtext:qualifiedName", "customtext:commonName", "provenance", "priority", "filetype", "filespec", "relation:url", "relation:text"]

		self.write_file = open(filename, 'w', newline='', encoding='utf-8')
		self.writer = csv.writer(self.write_file, delimiter = ',')
		self.writer.writerow(heading_row)

	def write_data(self):
		for key in self.records.keys():
			record = self.records[key]
			record_irn = record.get("IRN")
			media_irns = []
			itemid = None
			count = 0

			if self.record_data.source_rows:
				this_source_rows = filter(lambda row: row["record_irn"] == record_irn, self.record_data.source_rows)
				for row in this_source_rows:
					if itemid is None:
						if row["itemid"] is not None:
							itemid = row["itemid"]
					media_irns.append(row["media_irn"])

			elif "media" in record:
				for media in record["media"]:
					if self.record_data.harvest_all_media == True:
						if media.get("media_width") >= 2500 and media.get("media_height") >= 2500:
							if media.get("downloadable") == True:
								media_irns.append(media["media_irn"])

			if len(media_irns) > 1:
				row = CSVRow(record_irn=record_irn, record=record, itemid=itemid, count=count, sequence=True, qual_range=self.record_data.qual_range, saved_places=self.saved_places)
				self.writer.writerow(row.line_data)
				count += 1

			for media_irn in media_irns:
				row = CSVRow(record_irn=record_irn, media_irn=media_irn, record=record, itemid=itemid, count=count, sequence=False, qual_range=self.record_data.qual_range, saved_places=self.saved_places)
				self.writer.writerow(row.line_data)
				count += 1

		self.write_file.close()

class CSVRow():
	def __init__(self, record_irn=None, media_irn=None, record=None, itemid=None, count=0, sequence=False, qual_range=None, saved_places=None):
		self.record_irn = record_irn
		self.media_irn = media_irn
		self.record = record
		self.itemid = itemid
		self.count = count
		self.sequence = sequence
		self.qual_range_lower = qual_range[0]
		self.qual_range_upper = qual_range[1]
		self.saved_places = saved_places

		self.line_data = self.line_data()

		if quiet == False:
			if sequence == True:
				print("Writing sequence row for record", record_irn)
			else:
				print("Writing row for record", record_irn, "and media", media_irn)
	
	def line_data(self):
		value_list = []

		if self.count == 0 or self.sequence == True:

			# itemid
			if self.itemid == None:
				value_list.append(self.record["pid"])
			else:
				value_list.append(self.itemid)

			# subitemid - not applicable if count == 0
			value_list.append("")
			
			# orderid - not applicable if count == 0
			value_list.append("")

			#customtext:registrationid
			if "identifier" in self.record:
				value_list.append(self.record["identifier"])
			else:
				value_list.append("")

			# title - shortened or lengthened if needed
			if "title" in self.record:
				title = self.record["title"]
				if len(title) > 100 or len(title) < 5:
					title = self.process_title(title)
				value_list.append(self.record["title"])
			else:
				value_list.append("")

			# description - cleaned of html
			if "description" in self.record:
				description = self.process_description(self.record["description"])
				value_list.append(description)
			else:
				value_list.append("")

			# creator, location:placename, location:lat, location:long, dateCreated:start, dateCreated:display
			if "production" in self.record:
				values = self.compile_production(self.record["production"])
				for value in values:
					value_list.append(value)
			else:
				value_list.append("")
				value_list.append("")
				value_list.append("")
				value_list.append("")
				value_list.append("")
				value_list.append("")

			# rights - not applicable if sequence == True
			if self.sequence == False:
				if "media" in self.record:
					images_data = self.record["media"]
					rights = None

					for image in images_data:
						if image["media_irn"] == self.media_irn:
							if "rights_title" in image:
								rights_statement = image["rights_title"]
								if "rights_irn" in image:
									rights_irn = image["rights_irn"]
									rights = "<a href=\"{irn}\">{state}</a>".format(irn=rights_irn, state=rights_statement)
								else:
									rights = rights_statement
					if rights:
						value_list.append(rights)
					else:
						value_list.append("")
				else:
					value_list.append("")
			else:
				value_list.append("")

			# format
			if "dimensions" in self.record:
				measures_list = []
				for measure in self.record["dimensions"]:
					if "mm" not in measure:
						dims = measure.split(", ")
						new_dims = []
						for inc in dims:
							try:
								mms = int(floor(float(inc) * 25))
								s_mms = "{}mm".format(mms)
								new_dims.append(s_mms)
							except:
								pass
						if len(new_dims) == 2:
							measure = ", ".join(new_dims)
					measures_list.append(measure)

				value_list.append(", ".join(measures_list))
			else:
				value_list.append("")

			# medium
			if "isMadeOfSummary" in self.record:
				value_list.append(self.record["isMadeOfSummary"])
			else:
				value_list.append("")

			# subject
			subjects = []
			if "depicts" in self.record:
				for term in self.record["depicts"]:
					if isinstance(term, tuple):
						subject = term[0]
					else:
						subject = term
					subjects.append(subject)
			if "influencedBy" in self.record:
				for term in self.record["influencedBy"]:
					if isinstance(term, tuple):
						subject = term[0]
					else:
						subject = term
					subjects.append(subject)
			if len(subjects) > 0:
				value_list.append(", ".join(subjects))
			else:
				value_list.append("")

			# art=support
			if "isMadeOf" in self.record:
				mappings = ["canvas", "paper", "plaster", "cardboard", "ceramic", "wood", "clay"]
				terms = []
				for term in self.record["isMadeOf"]:
					for mapping in mappings:
						if mapping == term.lower() or mapping in term or mapping in term.lower():
							if mapping not in terms:
								terms.append(mapping)

				if len(terms) > 0:
					value_list.append(", ".join(terms))
				else:
					value_list.append("")
			else:
				value_list.append("")

			# art=depictedLocation.placename
			if "depicts" in self.record:
				locations = []
				for term in self.record["depicts"]:
					if isinstance(term, tuple):
						value = term[0]
						value_type = term[1]
						if value_type == "Place":
							locations.append(value)
				if len(locations) > 0:
					value_list.append(", ".join(locations))
				else:
					value_list.append("")
			else:
				value_list.append("")

			# art=depictedPerson
			if "depicts" in self.record:
				people = []
				for term in self.record["depicts"]:
					if isinstance(term, tuple):
						value = term[0]
						value_type = term[1]
						if value_type == "Person":
							people.append(value)
				if len(people) > 0:
					value_list.append(", ".join(people))
				else:
					value_list.append("")
			else:
				value_list.append("")

			# art=genre
			if "depicts" in self.record:
				genres = []
				genre_mappings = ["landscape", "portrait", "still life"]
				for term in self.record["depicts"]:
					if isinstance(term, tuple):
						genre = term[0]
					else:
						genre = term

					for mapping in genre_mappings:
						if mapping == genre.lower() or mapping in genre or mapping in genre.lower():
							if mapping not in genres:
								genres.append(mapping)

				if len(genres) > 0:
					value_list.append(", ".join(genres))
				else:
					value_list.append("")
			else:
				value_list.append("")

			# customtext:specimenType
			if "specimenType" in self.record:
				value_list.append(self.record["specimenType"])
			else:
				value_list.append("")

			# customtext:dateCollected
			if "dateCollected" in self.record:
				value_list.append(self.record["dateCollected"])
			else:
				value_list.append("")

			# customtext:collectedBy
			collected_by = []
			if "collectors" in self.record:
				for coll in self.record["collectors"]:
					if "collectedBy" in coll:
						collected_by.append(coll["collectedBy"])
			if len(collected_by) > 0:
				colls = ", ".join(collected_by)
				value_list.append(colls)
			else:
				value_list.append("")

			# customtext:locationCollected
			loc_value = None
			loc_fields = ["locationCollected", "preciseLocalityCollected", "stateProvinceCollected", "countryCollected"]
			for loc in loc_fields:
				if loc in self.record:
					loc_value = self.record[loc]
					break
			value_list.append(loc_value)

			# identification
			if "identification" in self.record:
				values = self.compile_identification(self.record["identification"])
				for value in values:
					value_list.append(value)
			else:
				value_list.append("")
				value_list.append("")
				value_list.append("")
				value_list.append("")

			# provenance
			if "creditLine" in self.record:
				value_list.append(self.record["creditLine"])
			else:
				value_list.append("")

			# priority
			if "qualityScore" in self.record:
				score = self.record["qualityScore"] - self.qual_range_lower
				upper = self.qual_range_upper - self.qual_range_lower

				# Get the percentage then invert it to prioritise higher quality records
				score_percentage = score / upper * 100
				score_percentage = 100 - score_percentage

				priority = floor(score_percentage * 3)
				modifier = random.randint(-5, 5)
				priority = priority + modifier

				if priority <= 0:
					priority = random.randint(1, 100)

				value_list.append(priority)
			else:
				value_list.append("")

			# filetype
			if self.sequence == False:
				value_list.append("image")
			else:
				value_list.append("sequence")

			# filespec
			if self.sequence == False:
				if "title" in self.record:
					title = self.record["title"]
					if len(title) > 100 or len(title) < 5:
						title = self.process_title(title)
				else:
					title = ""

				filename = "Te_Papa_{irn}_{media_irn}_{title}".format(irn=self.record_irn, media_irn=self.media_irn, title=title)

				filespec = self.clean_filename(filename)

				value_list.append(filespec)
			else:
				value_list.append("")

			# relation:url and relation:text
			value_list.append("https://collections.tepapa.govt.nz/object/{}".format(str(self.record_irn)))
			value_list.append("Te Papa Collections Online")

		else:
			# itemid
			value_list.append(self.record["pid"])

			#subitemid
			subitemid = self.record["pid"] + "." + str(self.media_irn)
			value_list.append(subitemid)

			# orderid
			value_list.append(self.count)

			# customtext:registrationid
			value_list.append("")

			# title
			value_list.append("")

			# description
			value_list.append("")

			# creator
			value_list.append("")

			# location:placename
			value_list.append("")

			# location:lat
			value_list.append("")

			# location:long
			value_list.append("")

			# dateCreated:start
			value_list.append("")

			# dateCreated:display
			value_list.append("")

			# rights
			images_data = self.record["media"]
			rights = None

			for image in images_data:
				if image["media_irn"] == self.media_irn:
					if "rights_title" in image:
						rights_statement = image["rights_title"]
						if "rights_irn" in image:
							rights_irn = image["rights_irn"]
							rights = "<a href=\"{irn}\">{state}</a>".format(irn=rights_irn, state=rights_statement)
						else:
							rights = rights_statement
			if rights:
				value_list.append(rights)
			else:
				value_list.append("")

			# format
			value_list.append("")

			# medium
			value_list.append("")

			# subject
			value_list.append("")

			# art=support
			value_list.append("")

			# art=depictedLocation:placename
			value_list.append("")

			# art=depictedPerson
			value_list.append("")

			# art=genre
			value_list.append("")

			# customtext:specimenType
			value_list.append("")

			# customtext:dateCollected
			value_list.append("")

			# customtext:collectedBy
			value_list.append("")

			# customtext:locationCollected
			value_list.append("")

			# identification
			value_list.append("")
			value_list.append("")
			value_list.append("")
			value_list.append("")

			# provenance
			value_list.append("")

			# priority
			value_list.append("")

			# filetype
			value_list.append("image")

			# filespec
			if "title" in self.record:
				title = self.record["title"]
				if len(title) > 100 or len(title) < 5:
					title = self.process_title(title)
			else:
				title = ""

			filename = "Te_Papa_{irn}_{media_irn}_{title}".format(irn=self.record_irn, media_irn=self.media_irn, title=title)

			filespec = self.clean_filename(filename)

			value_list.append(filespec)

			# relation:url
			value_list.append("")

			# relation:text
			value_list.append("")

		return value_list

	def process_title(self, title):
		if len(title) < 5:
			title = "{title} ({add})".format(title=title, add="...")
		elif len(title) > 100:
			title = "{}...".format(title[0:96])
		return title

	def process_description(self, description):
		clean = re.compile("<.*?>")
		clean_desc = re.sub(clean, "", description)
		clean_desc.replace("&nbsp;", " ")
		return clean_desc

	def compile_production(self, production_data):
		creators = []
		date_start = []
		date_display = []
		places = []
		lat_value = None
		long_value = None
		for prod in production_data:
			if "producer_name" in prod:
				name = prod["producer_name"]
			else:
				name = None
			if "producer_role" in prod:
				role = prod["producer_role"]
				if name:
					if role == "follower of" or role == "after":
						creator = "{role} {name}".format(role=role, name=name)
					else:
						creator = "{name} ({role})".format(name=name, role=role)
				else:
					creator = "Unknown {}".format(role)
			else:
				creator = name
			
			if creator is not None:
				if creator not in creators:
					creators.append(creator)

			if "production_date_start" in prod:
				if prod["production_date_start"] not in date_start:
					date_start.append(prod["production_date_start"])

			if "production_date" in prod:
				if prod["production_date"] not in date_display:
					date_display.append(prod["production_date"])

			if "production_place" in prod:
				if "production_place_id" in prod:
					this_place_id = prod["production_place_id"]
					if this_place_id in self.saved_places.keys():
						if lat_value == None:
							lat_value = self.saved_places[this_place_id]["lat"]
						if long_value == None:
							long_value = self.saved_places[this_place_id]["long"]
					else:
						lat_long = self.get_spatial(this_place_id)
						time.sleep(0.1)
						if lat_long:
							if lat_value == None:
								lat_value = lat_long[0]
							if long_value == None:
								long_value = lat_long[1]
							self.saved_places.update({this_place_id: {"lat": lat_value, "long": long_value}})
				if prod["production_place"] not in places:
					places.append(prod["production_place"])

		if len(creators) > 0:
			creator_values = ", ".join(creators)
		else:
			creator_values = ""

		if len(date_start) > 0:
			date_start_value = date_start[0]
		else:
			date_start_value = ""

		if len(date_display) > 0:
			date_display_values = ", ".join(date_display)
		else:
			date_display_values = ""

		if len(places) > 0:
			places_values = ", ".join(places)
		else:
			places_values = ""

		if lat_value and long_value:
			prod_values = [creator_values, places_values, lat_value, long_value, date_start_value, date_display_values]
		else:
			prod_values = [creator_values, places_values, "", "", date_start_value, date_display_values]

		return prod_values

	def get_spatial(self, irn):
		resource_type = "place"
		response = harvester.API.view_resource(resource_type=resource_type, irn=irn).resource
		if "geoLocation" in response:
			p_lat = None
			p_long = None
			if "lat" in response["geoLocation"]:
				p_lat = response["geoLocation"]["lat"]
			if "lon" in response["geoLocation"]:
				p_long = response["geoLocation"]["lon"]
			if p_lat and p_long:
				return (p_lat, p_long)
		else:
			return None

	def compile_identification(self, identification_data):
		date_identified = []
		identified_by = []
		qualified_name = []
		vernacular_name = []
		for ident in identification_data:
			if "dateIdentified" in ident:
				date = ident["dateIdentified"]
				if date not in date_identified:
					date_identified.append(date)

			if "identifiedBy" in ident:
				identifier = ident["identifiedBy"]
				if identifier not in identified_by:
					identified_by.append(identifier)

			if "qualifiedName" in ident:
				name = ident["qualifiedName"]
				if name not in qualified_name:
					qualified_name.append(name)

			if "vernacularName" in ident:
				for name in ident["vernacularName"]:
					if name not in vernacular_name:
						vernacular_name.append(name)

		if len(date_identified) > 0:
			date_values = ", ".join(date_identified)
		else:
			date_values = ""

		if len(identified_by) > 0:
			id_values = ", ".join(identified_by)
		else:
			id_values = ""

		if len(qualified_name) > 0:
			qual_values = ", ".join(qualified_name)
		else:
			qual_values = ""

		if len(vernacular_name) > 0:
			vern_values = ", ".join(vernacular_name)
		else:
			vern_values = ""

		identification_values = [date_values, id_values, qual_values, vern_values]

		return identification_values

	def clean_filename(self, filename):
		filename = filename.replace(" ", "_")
		filename = filename.replace("?", "")
		filename = filename.replace("\"", "")
		filename = filename.replace(":", "")
		filename = filename.replace(";", "")
		filename = filename.replace(".", "")
		filename = filename.replace(",", "")
		filename = filename.replace("#", "")
		filename = filename.replace("*", "")
		filename = filename.replace("\'", "")
		filename = filename.replace("\\", "")
		filename = filename.replace("/", "")

		return "{}.jpg".format(filename)

write_google_data()