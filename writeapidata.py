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
	def __init__(self, filename, heading_row):
		self.filename = filename
		self.heading_row = heading_row

		self.write_file = open(self.filename, 'w', newline='', encoding='utf-8')
		
		self.writer = csv.writer(self.write_file, delimiter = ',')
		self.writer.writerow(self.heading_row)

	def write_line(self, irn=None, data_dict=None, media_irn=None, no_of_images=None):
		self.irn = irn
		self.data_dict = data_dict
		self.media_irn = media_irn

		value_list = []
		
		value_list.append(self.irn)

		if "identifier" in self.data_dict:
			value_list.append(self.data_dict["identifier"])
		else:
			value_list.append("")

		if "title" in self.data_dict:
			value_list.append(self.data_dict["title"])
			value_list.append(len(self.data_dict["title"]))
		else:
			value_list.append("")
			value_list.append("")

		value_list.append(no_of_images)

		value_list.append(self.media_irn)

		image_fields = ["media_title", "media_type", "contentUrl", "thumbnailUrl", "rights_title", "media_width", "media_height"]

		if "media" in self.data_dict:
			images_data = self.data_dict["media"]

			for image in images_data:
				if image["media_irn"] == self.media_irn:
					for field in image_fields:
						if field in image:
							write_value = image[field]
							value_list.append(write_value)
						else:
							value_list.append("")
		else:
			for field in image_fields:
				value_list.append("")

		if "description" in self.data_dict:
			description = self.data_dict["description"]
			value_list.append(description)
			proc_description = self.process_description(description)
			value_list.append(proc_description)
		else:
			value_list.append("")
			value_list.append("")

		# This is set up to work for photographs, may need changes
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
		
		if "productionUsedTechnique" in self.data_dict:
			value_list.append(", ".join(self.data_dict["productionUsedTechnique"]))
		else:
			value_list.append("")

		if "isMadeOfSummary" in self.data_dict:
			value_list.append(self.data_dict["isMadeOfSummary"])
		else:
			value_list.append("")

		if "isMadeOf" in self.data_dict:
			value_list.append(", ".join(self.data_dict["isMadeOf"]))
		else:
			value_list.append("")

		if "isTypeOf" in self.data_dict:
			value_list.append(", ".join(self.data_dict["isTypeOf"]))
		else:
			value_list.append("")

		influencedBy = []
		if "influencedBy" in self.data_dict:
			for term in self.data_dict["influencedBy"]:
				if isinstance(term, tuple):
					influenced_term = term[0]
				else:
					influenced_term = term
				influencedBy.append(influenced_term)
		if len(influencedBy) > 0:
			value_list.append(", ".join(influencedBy))
		else:
			value_list.append("")

		depicts = []
		if "depicts" in self.data_dict:
			for term in self.data_dict["depicts"]:
				if isinstance(term, tuple):
					depicts_term = term[0]
				else:
					depicts_term = term
				depicts.append(depicts_term)
		if len(depicts) > 0:
			value_list.append(", ".join(depicts))
		else:
			value_list.append("")

		refersTo = []
		if "refersTo" in self.data_dict:
			for term in self.data_dict["refersTo"]:
				if isinstance(term, tuple):
					refers_term = term[0]
				else:
					refers_term = term
				refersTo.append(refers_term)
		if len(refersTo) > 0:
			value_list.append(", ".join(refersTo))
		else:
			value_list.append("")

		if "specimenType" in self.data_dict:
			value_list.append(self.data_dict["specimenType"])
		else:
			value_list.append("")

		if "basisOfRecord" in self.data_dict:
			value_list.append(self.data_dict["basisOfRecord"])
		else:
			value_list.append("")

		if "dateCollected" in self.data_dict:
			value_list.append(self.data_dict["dateCollected"])
		else:
			value_list.append("")

		if "collectedBy" in self.data_dict:
			value_list.append(self.data_dict["collectedBy"])
		else:
			value_list.append("")

		if "locationCollected" in self.data_dict:
			value_list.append(self.data_dict["locationCollected"])
		else:
			value_list.append("")

		if "countryCollected" in self.data_dict:
			value_list.append(self.data_dict["countryCollected"])
		else:
			value_list.append("")

		if "stateProvinceCollected" in self.data_dict:
			value_list.append(self.data_dict["stateProvenceCollected"])
		else:
			value_list.append("")

		if "preciseLocalityCollected" in self.data_dict:
			value_list.append(self.data_dict["preciseLocalityCollected"])
		else:
			value_list.append("")

		if "institutionCode" in self.data_dict:
			value_list.append(self.data_dict["institutionCode"])
		else:
			value_list.append("")

		if "identification" in self.data_dict:
			values = self.compile_identification(self.data_dict["identification"])
			for value in values:
				value_list.append(value)
		else:
			value_list.append("")
			value_list.append("")
			value_list.append("")
			value_list.append("")
			value_list.append("")
			value_list.append("")

		if "creditLine" in self.data_dict:
			value_list.append(self.data_dict["creditLine"])
		else:
			value_list.append("")

		if "qualityScore" in self.data_dict:
			value_list.append(self.data_dict["qualityScore"])
		else:
			value_list.append("")

		if "CO_url" in self.data_dict:
			value_list.append(self.data_dict["CO_url"])
			value_list.append("Te Papa Collections Online")
		else:
			value_list.append("")
			value_list.append("")

		self.writer.writerow(value_list)

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

			# Need to update this to consistantly format date
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
			prod_values = [creator_values, date_values, places_values, lat_value, long_value]
		else:
			prod_values = [creator_values, date_values, places_values, "", ""]

		return prod_values

	def get_spatial(self, irn):
		resource_type = "place"
		response = harvester.API.view_resource(resource_type=resource_type, irn=irn)
		if response is not None:
			response = response.resource
		else:
			return None
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
		family = []
		vernacular_name = []
		type_status = []
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

			if "family" in ident:
				fam = ident["family"]
				if fam not in family:
					family.append(fam)

			if "vernacularName" in ident:
				for name in ident["vernacularName"]:
					if name not in vernacular_name:
						vernacular_name.append(name)

			if "typeStatus" in ident:
				status = ident["typeStatus"]
				if status not in type_status:
					type_status.append(status)

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

		if len(family) > 0:
			family_values = ", ".join(family)
		else:
			family_values = ""

		if len(vernacular_name) > 0:
			vern_values = ", ".join(vernacular_name)
		else:
			vern_values = ""

		if len(type_status) > 0:
			type_values = ", ".join(type_status)
		else:
			type_values = ""

		identification_values = [date_values, id_values, qual_values, family_values, vern_values, type_values]

		return identification_values

	def process_description(self, description):
		clean = re.compile("<.*?>")
		return re.sub(clean, "", description)

def write_data_to_csv(record_data_dict, collection=None, media_irns=None, harvest_all_media=False):
	# Complete structured CSV with all records and all images
	csv_filename = working_folder + current_time + "_" + collection + ".csv"

	heading_row = ["irn", "identifier", "title", "title_length", "number_of_images", "media_irn", "media_title", "media_type", "contentUrl", "thumbnailUrl", "rights_title", "width", "height", "description", "cleanDescription", "observedDimension", "creator", "createdDate", "createdPlace", "lat", "long", "productionUsedTechnique", "isMadeOfSummary", "isMadeOf", "isTypeOf", "influencedBy", "depicts", "refersTo", "specimenType", "basisOfRecord", "dateCollected", "collectedBy", "locationCollected", "countryCollected", "stateProvinceCollected", "preciseLocalityCollected", "institutionCode", "dateIdentified", "identifiedBy", "qualifiedName", "family", "vernacularName", "typeStatus", "creditLine", "qualityScore", "CO_url", "CO_url_text"]

	output_csv = OutputCSV(filename=csv_filename, heading_row=heading_row)

	all_irns = record_data_dict.keys()
	#print(all_irns)

	for irn in all_irns:
		writable_image_irns = []
		irn_dat = record_data_dict[irn]

		if "specimenType" in irn_dat:
			if irn_dat["specimenType"] == "mount":
		
				if "media" in irn_dat:
					attached_images = irn_dat["media"]

					for image in attached_images:
						if harvest_all_media == True:
							if "media_width" in image:
								# Checking image size for Google
								if image["media_width"] >= 2500 and image["media_height"] >= 2500:
									if "downloadable" in image:
										if image["downloadable"] == True:
											writable_image_irns.append(image["media_irn"])
						else:
							if image["media_irn"] in media_irns:
								writable_image_irns.append(image["media_irn"])

					for media_irn in writable_image_irns:
						output_csv.write_line(irn=irn, data_dict=irn_dat, media_irn=media_irn)
			
				else:
					output_csv.write_line(irn=irn, data_dict=irn_dat)

			else:
				pass

	output_csv.write_file.close()

def just_print_titles(record_data_dict, collection=None, cutoff=0):
	# Textfile with list of titles above a specified length
	output_txt = working_folder + current_time + "_" + collection + "_titles.txt"
	all_irns = record_data_dict.keys()

	with open(output_txt, 'w', encoding="utf-8") as f:
		if cutoff != 0:
			for irn in all_irns:
				if int(record_data_dict[irn]["title_length"]) > cutoff:
					title_length = record_data_dict[irn]["title_length"]
					title = record_data_dict[irn]["title"]
					f.write(title_length + ", " + title + "\n")
				else: pass

		else:
			for irn in all_irns:
				title_length = record_data_dict[irn]["title_length"]
				title = record_data_dict[irn]["title"]
				f.write(title_length + ", " + title + "\n")

	f.close()

def just_print_subjects(record_data_dict, collection=None):
	# Textfiles with lists of several Object terms, with counts across full harvest
	# Unordered
	all_irns = record_data_dict.keys()
	lists = {"prod_tech_terms":[], "made_summary_terms":[], "made_of_terms":[], "type_of_terms":[], "influenced_terms":[], "depicts_terms":[], "refers_terms":[]}

	for irn in all_irns:
		if "productionUsedTechnique" in record_data_dict[irn]:
			for new_term in record_data_dict[irn]["productionUsedTechnique"]:
				lists["prod_tech_terms"].append(new_term)
		else: pass
		if "isMadeOfSummary" in record_data_dict[irn]:
			new_term = record_data_dict[irn]["isMadeOfSummary"]
			lists["made_summary_terms"].append(new_term)
		if "isMadeOf" in record_data_dict[irn]:
			for new_term in record_data_dict[irn]["isMadeOf"]:
				lists["made_of_terms"].append(new_term)
		else: pass
		if "isTypeOf" in record_data_dict[irn]:
			for new_term in record_data_dict[irn]["isTypeOf"]:
				lists["type_of_terms"].append(new_term)
		else: pass
		if "influencedBy" in record_data_dict[irn]:
			for new_term in record_data_dict[irn]["influencedBy"]:
				lists["influenced_terms"].append(new_term)
		else: pass
		if "depicts" in record_data_dict[irn]:
			for new_term in record_data_dict[irn]["depicts"]:
				lists["depicts_terms"].append(new_term)
		else: pass
		if "refersTo" in record_data_dict[irn]:
			for new_term in record_data_dict[irn]["refersTo"]:
				lists["refers_terms"].append(new_term)
		else: pass

	for each_list in lists:
		terms_and_numbers = Counter(lists[each_list])
#        for term in terms_and_numbers:
#            print(term, terms_and_numbers[term])
#        print(terms_and_numbers)
		with open(each_list+".txt", "w", encoding="utf-8") as f:
			for term in terms_and_numbers:
				f.write(term + ", " + str(terms_and_numbers[term]) + "\n")
		f.close()

def just_print_roles(record_data_dict, collection=None):
	# Textfile with list of roles, along with counts across full harvest
	# Unordered
	all_irns = record_data_dict.keys()
	roles = []
	
	for irn in all_irns:
		if "prod_0" in record_data_dict[irn]:
			if "prod_role_0" in record_data_dict[irn]["prod_0"]:
				roles.append(record_data_dict[irn]["prod_0"]["prod_role_0"])
			else: pass
		else: pass

	count_roles = Counter(roles)

	with open("roles.txt", "w", encoding="utf-8") as f:
		for role in count_roles:
			f.write(role + ", " + str(count_roles[role]) + "\n")
	f.close()

def just_print_irns(record_data_dict, collection=None):
	# Textfile with list of irns
	all_irns = record_data_dict.keys()

	with open("{}_irns.txt".format(collection), "w", encoding="utf-8") as f:
		for irn in all_irns:
			f.write(irn + "\n")
	f.close()

def search_API():
	q = "*"
	fields = None
	q_from = 0
	size = 500
	collection = "Birds"
	sort = [{"field": "id", "order": "asc"}]
	facets = [{"field": "evidenceFor.atEvent.atLocation.country", "size": 3}]
	filters = [{"field": "hasRepresentation.rights.allowsDownload", "keyword": "True"}, {"field": "collection", "keyword": "{}".format(collection)}, {"field": "type", "keyword": "Specimen"}]

	harvester.set_params(q=q, fields=fields, filters=filters, facets=facets, q_from=q_from, size=size, sort=sort)
	harvester.count_results()
	record_data_dict = harvester.harvest_records()

	write_data_to_csv(record_data_dict, collection=collection, harvest_all_media=True)

def list_API(source=None):
	collection = "list"
	resource_type = "object"
	harvest_all_media = False
	irns = []

	if source.endswith(".csv"):
		with open(source, newline="", encoding="utf-8") as f:
			reader = csv.DictReader(f, delimiter=",")
			for row in reader:
				if "irn" in row:
					if row["irn"] not in irns:
						irns.append(int(row["irn"].strip()))
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

harvester = TePapaHarvester.Harvester(quiet=True, sleep=0.1)

#list_API()
search_API()