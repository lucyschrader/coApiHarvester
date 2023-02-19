# -*- coding: utf-8 -*-

from datetime import datetime
import time
import csv
import re
from math import floor
import random
import TePapaHarvester
from DataCollator import RecordData
import harvestconfig as hc

quiet = hc.quiet
input_dir = hc.input_dir
output_dir = hc.output_dir

harvester = TePapaHarvester.Harvester(quiet=quiet, sleep=0.1)

def write_google_data():
	if quiet == False:
		if hc.mode == "list":
			print("Harvesting data for items in", hc.list_source)
		else:
			print("Harvesting data for items in", hc.collection)

	# Harvests and aggregates data returned by API
	record_data = RecordData()

	# Transforms, maps, and writes data to CSV
	CSV = CSVWriter(record_data=record_data)
	CSV.write_data()

class CSVWriter():
	def __init__(self, record_data):
		self.record_data = record_data
		self.records = record_data.records

		self.saved_places = {}

		current_time = datetime.now().strftime("%d-%m-%Y_%H-%M")

		filename = "output_files/" + current_time + "_" + self.record_data.collection + "_" + "googledata.csv"

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
				value_list.append(self.return_standard_value("pid"))
			else:
				value_list.append(self.itemid)

			# subitemid - not applicable if count == 0
			value_list.append("")
			
			# orderid - not applicable if count == 0
			value_list.append("")

			#customtext:registrationid
			value_list.append(self.return_standard_value("identifier"))

			# title - shortened or lengthened if needed
			title = self.record.get("title")
			if title is not None:
				if len(title) > 100 or len(title) < 5:
					title = self.process_title(title)
				value_list.append(title)
			else:
				value_list.append("")

			# description - cleaned of html
			description = self.record.get("description")
			if description is not None:
				description = self.process_description(self.record["description"])
				value_list.append(description)
			else:
				value_list.append("")

			# creator, location:placename, location:lat, location:long, dateCreated:start, dateCreated:display
			production = self.record.get("production")
			if production is not None:
				values = self.compile_production(production)
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
				media = self.record.get("media")
				if media is not None:
					rights = None

					for image in media:
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
			dimensions = self.record.get("dimensions")
			if dimensions is not None:
				measures_list = []
				for measure in dimensions:
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
			value_list.append(self.return_standard_value("isMadeOfSummary"))

			# subject
			subjects = []
			depicts = self.record.get("depicts")
			if depicts is not None:
				for term in depicts:
					if isinstance(term, tuple):
						subject = term[0]
					else:
						subject = term
					subjects.append(subject)
			influencedby = self.record.get("influencedBy")
			if influencedby is not None:
				for term in influencedby:
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
			ismadeof = self.record.get("isMadeOf")
			if ismadeof is not None:
				mappings = ["canvas", "paper", "plaster", "cardboard", "ceramic", "wood", "clay"]
				terms = []
				for term in ismadeof:
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
			if depicts is not None:
				locations = []
				for term in depicts:
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
			if depicts is not None:
				people = []
				for term in depicts:
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
			if depicts is not None:
				genres = []
				genre_mappings = ["landscape", "portrait", "still life"]
				for term in depicts:
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
			value_list.append(self.return_standard_value("specimenType"))

			# customtext:dateCollected
			value_list.append(self.return_standard_value("dateCollected"))

			# customtext:collectedBy
			collected_by = []
			collectors = self.record.get("collectors")
			if collectors is not None:
				for coll in collectors:
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
			identification = self.record.get("identification")
			if identification is not None:
				values = self.compile_identification(identification)
				for value in values:
					value_list.append(value)
			else:
				value_list.append("")
				value_list.append("")
				value_list.append("")
				value_list.append("")

			# provenance
			value_list.append(self.return_standard_value("creditLine"))

			# priority
			qualityscore = self.record.get("qualityScore")
			if qualityscore is not None:
				score = qualityscore - self.qual_range_lower
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
				title = self.record.get("title")
				if title is not None:
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
			value_list.append(self.record.get("pid"))

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
			images_data = self.record.get("media")
			rights = None

			for image in images_data:
				if image.get("media_irn") == self.media_irn:
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
			title = self.record.get("title")
			if title is not None:
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

	def return_standard_value(self, field):
		try:
			value = self.record.get(field)
			return value
		except:
			return ""

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