# -*- coding: utf-8 -*-

import os
from datetime import datetime
import time
import csv
import re
from math import floor
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

		if self.count == 0:

			# itemid
			value_list.append(self.data_dict["pid"])

			# subitemid - not applicable if count == 1
			value_list.append("")
			
			# orderid - not applicable if count == 1
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

			# rights - not applicable if sequence == True
			if sequence == False:
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

			# format
			if "dimensions" in self.data_dict:
				value_list.append(", ".join(self.data_dict["dimensions"]))
			else:
				value_list.append("")

			# medium
			if "isMadeOfSummary" in self.data_dict:
				value_list.append(self.data_dict["isMadeOfSummary"])
			else:
				value_list.append("")

			# subject
			if "depicts" in self.data_dict:
				value_list.append(self.data_dict["depicts"])
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

			# Gotta redo the harvest model so we can separate out depicts etc by type
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
				print("Score percentage: {}".format(score_percentage))

				priority = floor(score_percentage * 3)
				print("Priority: {}".format(priority))

				value_list.append(priority)
			else:
				value_list.append("")

			# filetype
			if sequence == "False":
				value_list.append("image")
			else:
				value_list.append("sequence")

			# filespec
			if sequence == "False":
				filespec = "TePapa_{irn}_{media_irn}.jpg".format(irn=self.irn, media_irn=self.media_irn)
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

			# provenance
			value_list.append("")

			# priority
			value_list.append("")

			# filetype
			value_list.append("image")

			# filespec
			filespec = "TePapa_{irn}_{media_irn}.jpg".format(irn=self.irn, media_irn=self.media_irn)
			value_list.append(filespec)

			# relation:url
			value_list.append("")

			# relation:text
			value_list.append("")

		self.writer.writerow(value_list)

	def process_title(self, title):
		if len(title) < 5:
			title = "{title} ({add})".format(title=title, add="Ummm")
		elif len(title) > 100:
			title = "{}...".format(title[0:96])
		return title

	def process_description(self, description):
		clean = re.compile("<.*?>")
		return re.sub(clean, "", description)

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
				if self.count == 1:
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

def write_data_to_csv(record_data_dict, collection=None):
	# Complete structured CSV with all records and all images
	csv_filename = working_folder + current_time + "_" + collection + ".csv"

	heading_row = ["itemid", "subitemid", "orderid", "customtext:registrationid", "title/en", "description/en", "creator/en", "location:placename", "location:lat", "location:long", "dateCreated:start", "dateCreated:display", "rights", "format", "medium", "subject", "art=support", "art=depictedLocation.placename", "art=depictedPerson", "art=genre", "provenance", "priority", "filetype", "filespec", "relation:url", "relation:text"]

	quality_score_range = qual_range(record_data_dict)

	output_csv = OutputCSV(filename=csv_filename, heading_row=heading_row, quality_score_range=quality_score_range)

	all_irns = record_data_dict.keys()
	#print(all_irns)

	# Need to mark a specific row as the first for that irn so we can avoid duplication
	for irn in all_irns:
		sequence = False
		writable_image_irns = []
		irn_dat = record_data_dict[irn]
		
		attached_images = irn_dat["media"]

		for image in attached_images:
			if "media_width" in image:
				if image["media_width"] >= 2500 and image["media_height"] >= 2500:
					if "downloadable" in image:
						if image["downloadable"] == True:
							writable_image_irns.append(image["media_irn"])
		
		print(writable_image_irns)
		if len(writable_image_irns) > 1:
			sequence = True

		count = 0
		for media_irn in writable_image_irns:
			if count > 0:
				sequence
			output_csv.write_line(irn=irn, data_dict=irn_dat, media_irn=media_irn, count=count, sequence=sequence)
			count += 1

	output_csv.write_file.close()

def search_API():
	q = "*"
	fields = None
	q_from = 0
	size = 500
	collection = "Photography"
	sort = [{"field": "id", "order": "asc"}]
	facets = [{"field": "production.spatial.title", "size": 3}]
	filters = [{"field": "hasRepresentation.rights.allowsDownload", "keyword": "True"}, {"field": "collection", "keyword": "{}".format(collection)}, {"field": "type", "keyword": "Object"}, {"field": "additionalType", "keyword": "PhysicalObject"}]

	harvester.set_params(q=q, fields=fields, filters=filters, facets=facets, q_from=q_from, size=size, sort=sort)
	harvester.count_results()
	record_data_dict = harvester.harvest_records()

	write_data_to_csv(record_data_dict, collection=collection)

def list_API():
	collection = "list"
	resource_type = "object"
	irns = []

	with open("irns_to_harvest.txt", "r", encoding="utf-8") as f:
		lines = f.readlines()
		for line in lines:
			irns.append(line.strip())

	record_data_dict = harvester.harvest_from_list(resource_type=resource_type, irns=irns)

	write_data_to_csv(record_data_dict, collection=collection)

def qual_range(record_data_dict):
	lowest = 0
	highest = 0
	record_scores = []
	for key in record_data_dict.keys():
		score = record_data_dict[key]["qualityScore"]
		record_scores.append(score)
	
	sorted_records = sorted(record_scores, reverse=False)
	lowest = sorted_records[0]
	print("Lowest: {}".format(str(lowest)))

	sorted_records = sorted(record_scores, reverse=True)
	highest = sorted_records[0]
	print("Highest: {}".format(str(highest)))

	return (lowest, highest)

harvester = TePapaHarvester.Harvester(quiet=True, sleep=0.2)

list_API()
#search_API()