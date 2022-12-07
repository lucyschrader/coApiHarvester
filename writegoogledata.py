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

# Used when writing files with unique filenames
working_folder = os.getcwd() + "/"
now = datetime.now()
current_time = now.strftime("%H-%M-%S")

class OutputCSV():
	# Creates and writes stored data to a CSV
	# Writes a full new line for each image attached to a record
	def __init__(self, filename, heading_row, quality_score_range):
		self.filename = filename
		self.heading_row = heading_row
		self.quality_score_range = quality_score_range

		self.write_file = open(self.filename, 'w', newline='', encoding='utf-8')
		
		self.writer = csv.writer(self.write_file, delimiter = ',')
		self.writer.writerow(self.heading_row)

	def write_line(self, irn=None, data_dict=None, media_irn=None, count=0, sequence=False):
		self.irn = irn
		self.data_dict = data_dict
		self.media_irn = media_irn
		self.count = count

		value_list = []

		if self.count == 0 or sequence == True:

			# itemid
			value_list.append(self.data_dict["pid"])

			# subitemid - not applicable if count == 0
			value_list.append("")
			
			# orderid - not applicable if count == 0
			value_list.append("")

			#customtext:registrationid
			if "identifier" in self.data_dict:
				value_list.append(self.data_dict["identifier"])
			else:
				value_list.append("")

			# title - shortened or lengthened if needed
			if "title" in self.data_dict:
				title = self.data_dict["title"]
				if len(title) > 100 or len(title) < 5:
					title = self.process_title(title)
				value_list.append(self.data_dict["title"])
			else:
				value_list.append("")

			# description - cleaned of html
			if "description" in self.data_dict:
				description = self.process_description(self.data_dict["description"])
				value_list.append(description)
			else:
				value_list.append("")

			# creator, location:placename, location:lat, location:long, dateCreated:start, dateCreated:display
			if "production" in self.data_dict:
				values = self.compile_production(self.data_dict["production"])
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
			if sequence == False:
				if "media" in self.data_dict:
					images_data = self.data_dict["media"]
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
			if "dimensions" in self.data_dict:
				measures_list = []
				for measure in self.data_dict["dimensions"]:
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
			if "isMadeOfSummary" in self.data_dict:
				value_list.append(self.data_dict["isMadeOfSummary"])
			else:
				value_list.append("")

			# subject
			subjects = []
			if "depicts" in self.data_dict:
				for term in self.data_dict["depicts"]:
					if isinstance(term, tuple):
						subject = term[0]
					else:
						subject = term
					subjects.append(subject)
			if "influencedBy" in self.data_dict:
				for term in self.data_dict["influencedBy"]:
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
			if "isMadeOf" in self.data_dict:
				mappings = ["canvas", "paper", "plaster", "cardboard", "ceramic", "wood", "clay"]
				terms = []
				for term in self.data_dict["isMadeOf"]:
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
			if "depicts" in self.data_dict:
				locations = []
				for term in self.data_dict["depicts"]:
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
			if "depicts" in self.data_dict:
				people = []
				for term in self.data_dict["depicts"]:
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
			if "depicts" in self.data_dict:
				genres = []
				genre_mappings = ["landscape", "portrait", "still life"]
				for term in self.data_dict["depicts"]:
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
			if "specimenType" in self.data_dict:
				value_list.append(self.data_dict["specimenType"])
			else:
				value_list.append("")

			# customtext:dateCollected
			if "dateCollected" in self.data_dict:
				value_list.append(self.data_dict["dateCollected"])
			else:
				value_list.append("")

			# customtext:collectedBy
			collected_by = []
			if "collectors" in self.data_dict:
				for coll in self.data_dict["collectors"]:
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
				if loc in self.data_dict:
					loc_value = self.data_dict[loc]
					break
			value_list.append(loc_value)

			# identification
			if "identification" in self.data_dict:
				values = self.compile_identification(self.data_dict["identification"])
				for value in values:
					value_list.append(value)
			else:
				value_list.append("")
				value_list.append("")
				value_list.append("")
				value_list.append("")

			# provenance
			if "creditLine" in self.data_dict:
				value_list.append(self.data_dict["creditLine"])
			else:
				value_list.append("")

			# priority
			if "qualityScore" in self.data_dict:
				score = self.data_dict["qualityScore"] - self.quality_score_range[0]
				upper = self.quality_score_range[1] - self.quality_score_range[0]

				# Get the percentage then invert it to prioritise higher quality records
				score_percentage = score / upper * 100
				score_percentage = 100 - score_percentage
#				print("Score percentage: {}".format(score_percentage))

				priority = floor(score_percentage * 3)
				modifier = random.randint(-5, 5)
				priority = priority + modifier
#				print("Priority: {}".format(priority))

				if priority <= 0:
					priority = random.randint(1, 100)

				value_list.append(priority)
			else:
				value_list.append("")

			# filetype
			if sequence == False:
				value_list.append("image")
			else:
				value_list.append("sequence")

			# filespec
			if sequence == False:
				if "title" in self.data_dict:
					title = self.data_dict["title"]
					if len(title) > 100 or len(title) < 5:
						title = self.process_title(title)
				else:
					title = ""

				filename = "Te_Papa_{irn}_{media_irn}_{title}".format(irn=irn, media_irn=media_irn, title=title)

				filespec = self.clean_filename(filename)

				value_list.append(filespec)
			else:
				value_list.append("")

			# relation:url and relation:text
			value_list.append("https://collections.tepapa.govt.nz/object/{}".format(str(self.irn)))
			value_list.append("Te Papa Collections Online")

		else:
			# itemid
			value_list.append(self.data_dict["pid"])

			#subitemid
			subitemid = self.data_dict["pid"] + "." + str(self.media_irn)
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
			images_data = self.data_dict["media"]
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
			if "title" in self.data_dict:
				title = self.data_dict["title"]
				if len(title) > 100 or len(title) < 5:
					title = self.process_title(title)
			else:
				title = ""

			filename = "Te_Papa_{irn}_{media_irn}_{title}".format(irn=irn, media_irn=media_irn, title=title)

			filespec = self.clean_filename(filename)

			value_list.append(filespec)

			# relation:url
			value_list.append("")

			# relation:text
			value_list.append("")

		self.writer.writerow(value_list)

	def process_title(self, title):
		if len(title) < 5:
			title = "{title} ({add})".format(title=title, add="...")
		elif len(title) > 100:
			title = "{}...".format(title[0:96])
		return title

	def process_description(self, description):
		clean = re.compile("<.*?>")
		return re.sub(clean, "", description)

	# dateCreated:start needs more processing - has to be YYYY, YYYY-MM or YYYY-MM-DD
	def compile_production(self, production_data):
		creators = []
		dates = []
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

			if "production_date" in prod:
				if prod["production_date"] not in dates:
					dates.append(prod["production_date"])

			if "production_place" in prod:
				if "production_place_id" in prod:
					lat_long = self.get_spatial(prod["production_place_id"])
					time.sleep(0.1)
					if lat_long:
						if lat_value == None:
							lat_value = lat_long[0]
						if long_value == None:
							long_value = lat_long[1]
				if prod["production_place"] not in places:
					places.append(prod["production_place"])

		if len(creators) > 0:
			creator_values = ", ".join(creators)
		else:
			creator_values = ""

		if len(dates) > 0:
			date_values = ", ".join(dates)
		else:
			date_values = ""

		if len(places) > 0:
			places_values = ", ".join(places)
		else:
			places_values = ""

		if lat_value and long_value:
			prod_values = [creator_values, places_values, lat_value, long_value, date_values, date_values]
		else:
			prod_values = [creator_values, places_values, "", "", date_values, date_values]

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
			return False

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

	def process_description(self, description):
		clean = re.compile("<.*?>")
		return re.sub(clean, "", description)

	def clean_filename(self, filename):
		filename = filename.replace(" ", "_")
		filename = filename.replace("?", "")
		filename = filename.replace("\"", "")
		filename = filename.replace(":", "")
		filename = filename.replace(";", "")
		filename = filename.replace(".", "")
		filename = filename.replace("#", "")

		return "{}.jpg".format(filename)

def write_data_to_csv(record_data_dict, collection=None, media_irns=None, harvest_all_media=False):
	# Complete structured CSV with all records and all images
	csv_filename = working_folder + current_time + "_" + collection + ".csv"

	heading_row = ["itemid", "subitemid", "orderid", "customtext:registrationid", "title/en", "description/en", "creator/en", "location:placename", "location:lat", "location:long", "dateCreated:start", "dateCreated:display", "rights", "format", "medium", "subject", "art=support", "art=depictedLocation:placename", "art=depictedPerson", "art=genre", "customtext:specimenType", "customtext:dateCollected", "customtext:creator.collector", "customtext:locationCollected", "customtext:dateIdentified", "customtext:creator.identifier", "customtext:qualifiedName", "customtext:commonName", "provenance", "priority", "filetype", "filespec", "relation:url", "relation:text"]

	quality_score_range = qual_range(record_data_dict)

	output_csv = OutputCSV(filename=csv_filename, heading_row=heading_row, quality_score_range=quality_score_range)

	all_irns = record_data_dict.keys()
	#print(all_irns)

	for irn in all_irns:
		writable_image_irns = []
		irn_dat = record_data_dict[irn]
		
		if "media" in irn_dat:
			attached_images = irn_dat["media"]

			for image in attached_images:
				if harvest_all_media == True:
					if "media_width" in image:
						if image["media_width"] >= 2500 and image["media_height"] >= 2500:
							if "downloadable" in image:
								if image["downloadable"] == True:
									writable_image_irns.append(image["media_irn"])
				else:
					if image["media_irn"] in media_irns:
						writable_image_irns.append(image["media_irn"])

			if len(writable_image_irns) > 1:
				sequence = True
				output_csv.write_line(irn=irn, data_dict=irn_dat, sequence=True)
			else:
				sequence = False

			if sequence == True:
				count = 1
			else:
				count = 0
			for media_irn in writable_image_irns:
				output_csv.write_line(irn=irn, data_dict=irn_dat, media_irn=media_irn, count=count, sequence=False)
				count += 1
	
		else:
			output_csv.write_line(irn=irn, data_dict=irn_dat, count=0, sequence=False)

	output_csv.write_file.close()

def search_API():
	q = "*"
	fields = None
	q_from = 0
	size = 500
	collection = "Birds"
	sort = [{"field": "id", "order": "asc"}]
#	facets = [{"field": "production.spatial.title", "size": 3}]
	facets = [{"field": "evidenceFor.atEvent.atLocation.country", "size": 3}]
#	filters = [{"field": "hasRepresentation.rights.allowsDownload", "keyword": "True"}, {"field": "collection", "keyword": "{}".format(collection)}, {"field": "type", "keyword": "Object"}, {"field": "additionalType", "keyword": "PhysicalObject"}]
	filters = [{"field": "hasRepresentation.rights.allowsDownload","keyword": "True"}, {"field": "collection", "keyword": "{}".format(collection)}, {"field": "type", "keyword": "Specimen"}]

	harvester.set_params(q=q, fields=fields, filters=filters, facets=None, q_from=q_from, size=size, sort=sort)
	harvester.count_results()
	record_data_dict = harvester.harvest_records()

	write_data_to_csv(record_data_dict, collection=collection, harvest_all_media=True)

def list_API(source=None):
	collection = "list"
	resource_type = "object"
	harvest_all_media = False

	irns = []
	media_irns = []

	if source.endswith(".csv"):
		with open(source, newline="", encoding="utf-8") as f:
			reader = csv.DictReader(f, delimiter=",")
			for row in reader:
				if "record_irn" in row:
					if row["record_irn"] not in irns:
						irns.append(int(row["record_irn"].strip()))
				if "media_irn" in row:
					media_irns.append(int(row["media_irn"].strip()))

	elif source.endswith(".txt"):
		with open(source, 'r', encoding="utf-8") as f:
			lines = f.readlines()
			for line in lines:
				irns.append(int(line.strip()))
		harvest_all_media = True

	record_data_dict = harvester.harvest_from_list(resource_type=resource_type, irns=irns)

	write_data_to_csv(record_data_dict, collection=collection, media_irns=media_irns, harvest_all_media=harvest_all_media)

def qual_range(record_data_dict):
	lowest = 0
	highest = 0
	record_scores = []
	for key in record_data_dict.keys():
		score = record_data_dict[key]["qualityScore"]
		record_scores.append(score)
	
	sorted_records = sorted(record_scores, reverse=False)
	lowest = sorted_records[0]
#	print("Lowest: {}".format(str(lowest)))

	sorted_records = sorted(record_scores, reverse=True)
	highest = sorted_records[0]
#	print("Highest: {}".format(str(highest)))

	return (lowest, highest)

harvester = TePapaHarvester.Harvester(quiet=True, sleep=0.1)

list_API(source="20220831-reharvestuploads.txt")
#search_API()