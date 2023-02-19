# -*- coding: utf-8 -*-

from datetime import datetime
import time
import csv
import re
from math import floor
import TePapaHarvester
from DataCollator import RecordData
import harvestconfig as hc

quiet = hc.quiet
input_dir = hc.input_dir
output_dir = hc.output_dir

harvester = TePapaHarvester.Harvester(quiet=quiet, sleep=0.1)

def write_api_data():
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

		current_time = datetime.now.strftime("%d-%m-%Y_%H-%M")

		filename = "/output_files/" + current_time + "_" + self.record_data.collection + "_" + "apidata.csv"

		heading_row = ["irn", "identifier", "title", "title_length", "number_of_images", "media_irn", "media_title", "media_type", "contentUrl", "thumbnailUrl", "rights_title", "width", "height", "description", "cleanDescription", "observedDimension", "creator", "createdDate", "createdPlace", "lat", "long", "productionUsedTechnique", "isMadeOfSummary", "isMadeOf", "isTypeOf", "influencedBy", "depicts", "refersTo", "specimenType", "basisOfRecord", "dateCollected", "collectedBy", "locationCollected", "countryCollected", "stateProvinceCollected", "preciseLocalityCollected", "institutionCode", "dateIdentified", "identifiedBy", "qualifiedName", "family", "vernacularName", "typeStatus", "creditLine", "qualityScore", "CO_url", "CO_url_text"]

		self.write_file = open(filename, 'w', newline='', encoding='utf-8')
		self.writer = csv.writer(self.write_file, delimiter = ',')
		self.writer.writerow(heading_row)

	def write_data(self):
		for key in self.records.keys():
			record = self.records[key]
			record_irn = record.get("IRN")
			media_irns = []
			count = 0

			for media in record["media"]:
				if self.record_data.harvest_all_media == True:
					if media.get("media_width") >= 2500 and media.get("media_height") >= 2500:
						if media.get("downloadable") == True:
							media_irns.append(media["media_irn"])

			for media_irn in media_irns:
				row = CSVRow(record_irn=record_irn, media_irn=media_irn, record=record, count=count, qual_range=self.record_data.qual_range, saved_places=self.saved_places)
				self.writer.writerow(row.line_data)
				count += 1

		self.write_file.close()

class CSVRow():
	def __init__(self, record_irn=None, media_irn=None, record=None, count=0, qual_range=None, saved_places=None):
		self.record_irn = record_irn
		self.media_irn = media_irn
		self.record = record
		self.count = count
		self.qual_range_lower = qual_range[0]
		self.qual_range_upper = qual_range[1]
		self.saved_places = saved_places

		self.line_data = self.line_data()

		if quiet == False:
			print("Writing row for record", record_irn, "and media", media_irn)
		
	def line_data(self):
		value_list = []

		# irn
		value_list.append(self.record_irn)

		# identifier
		value_list.append(self.return_standard_value("identifier"))

		# title, title_length
		title = self.record.get("title")
		if title is not None:
			value_list.append(title)
			value_list.append(len(title))
		else:
			value_list.append("")
			value_list.append(0)

		# number_of_images
		value_list.append(len(self.record.get("media")))

		# media_irn
		value_list.append(self.media_irn)

		# media_title, media_type, contentUrl, thumbnailUrl, rights_title, width, height
		media = self.record.get("media")
		media_title = ""
		media_type = ""
		content_url = ""
		thumbnail_url = ""
		rights_title = ""
		media_width = ""
		media_height = ""

		if media is not None:
			for image in filter(lambda image: image["media_irn"] == self.media_irn, media):
				media_title = media.get("media_title")
				media_type = media.get("media_type")
				content_url = media.get("contentUrl")
				thumbnail_url = media.get("thumbnailUrl")
				rights_title = media.get("rights_title")
				media_width = media.get("media_width")
				media_height = media.get("media_height")
		
		value_list.append(media_title)
		value_list.append(media_type)
		value_list.append(content_url)
		value_list.append(thumbnail_url)
		value_list.append(rights_title)
		value_list.append(media_width)
		value_list.append(media_height)

		# description, cleanDescription
		description = self.record.get("description")
		if description:
			value_list.append(description)
			value_list.append(self.process_description(description))
		else:
			value_list.append("")
			value_list.append("")

		# observedDimension
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

		# creator, createdDate, createdPlace, lat, long
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

		# productionUsedTechnique
		production_tech = self.record.get("productionUsedTechnique")
		if production_tech is not None:
			value_list.append(", ".join(production_tech))
		else:
			value_list.append("")

		# isMadeOfSummary
		value_list.append(self.return_standard_value("isMadeOfSummary"))

		# isMadeOf
		is_made_of = self.record.get("isMadeOf")
		if is_made_of is not None:
			value_list.append(", ".join(is_made_of))
		else:
			value_list.append("")

		# isTypeOf
		is_type_of = self.record.get("isTypeOf")
		if is_type_of is not None:
			value_list.append(", ".join(is_type_of))
		else:
			value_list.append("")

		# influencedBy
		influenced_by = self.record.get("influencedBy")
		if influenced_by is not None:
			influenced_terms = []
			for term in influenced_by:
				if isinstance(term, tuple):
					influenced_term = term[0]
				else:
					influenced_term = term
				influenced_terms.append(influenced_term)
			if len(influenced_terms) > 0:
				value_list.append(", ".join(influenced_terms))
		else:
			value_list.append("")

		# depicts
		depicts = self.record.get("depicts")
		if depicts is not None:
			depicts_terms = []
			for term in depicts:
				if isinstance(term, tuple):
					depicts_term = term[0]
				else:
					depicts_term = term
				depicts_terms.append(depicts_term)
			if len(depicts_terms) > 0:
				value_list.append(", ".join(depicts_terms))
		else:
			value_list.append("")

		# refersTo
		refers_to = self.record.get("refersTo")
		if refers_to is not None:
			refers_terms = []
			for term in refers_to:
				if isinstance(term, tuple):
					refers_term = term[0]
				else:
					refers_term = term
				refers_terms.append(refers_term)
			if len(refers_terms) > 0:
				value_list.append(", ".join(refers_terms))
		else:
			value_list.append("")

		# specimenType
		value_list.append(self.return_standard_value("specimenType"))

		# basisOfRecord
		value_list.append(self.return_standard_value("basisOfRecord"))

		# dateCollected
		value_list.append(self.return_standard_value("dateCollected"))

		# collectedBy
		collected_by = self.record.get("collectedBy")
		if collected_by is not None:
			collectors = []
			for coll in collected_by:
				if "collectedBy" in coll:
					collectors.append(coll["collectedBy"])
			if len(collectors) > 0:
				colls = ", ".join(collectors)
				value_list.append(colls)
			else:
				value_list.append("")

		# locationCollected
		value_list.append(self.return_standard_value("locationCollected"))

		# countryCollected
		value_list.append(self.return_standard_value("countryCollected"))

		# stateProvinceCollected
		value_list.append(self.return_standard_value("stateProvinceCollected"))

		# preciseLocalityCollected
		value_list.append(self.return_standard_value("preciseLocalityCollected"))

		# institutionCode
		value_list.append(self.return_standard_value("institutionCode"))

		# dateIdentified, identifiedBy, qualifiedName, family, vernacularName, typeStatus
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
			value_list.append("")
			value_list.append("")

		# creditLine
		value_list.append(self.return_standard_value("creditLine"))

		# qualityScore
		value_list.append(self.return_standard_value("qualityScore"))

		# CO_url
		value_list.append("https://collections.tepapa.govt.nz/object/{}".format(str(self.record_irn)))

		# CO_url_text
		value_list.append("Te Papa Collections Online")


	def return_standard_value(self, field):
		try:
			value = self.record.get(field)
			return value
		except:
			return ""

	def process_description(self, description):
		clean = re.compile("<.*?>")
		clean_desc = re.sub(clean, "", description)
		clean_desc.replace("&nbsp;", " ")
		return clean_desc
	
	def compile_production(self, production_data):
		creators = []
		created_date = []
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
				if prod["production_date_start"] not in created_date:
					created_date.append(prod["production_date_start"])

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

		if len(created_date) > 0:
			created_date_value = created_date[0]
		else:
			created_date_value = ""

		if len(places) > 0:
			places_values = ", ".join(places)
		else:
			places_values = ""

		if lat_value and long_value:
			prod_values = [creator_values, places_values, lat_value, long_value, created_date_value]
		else:
			prod_values = [creator_values, places_values, "", "", created_date_value]

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
		family_terms = []
		vernacular_name = []
		type_status_terms = []
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
				family = ident["family"]
				if family not in family_terms:
					family_terms.append(family)

			if "vernacularName" in ident:
				for name in ident["vernacularName"]:
					if name not in vernacular_name:
						vernacular_name.append(name)

			if "typeStatus" in ident:
				type_status = ident["typeStatus"]
				if type_status not in type_status_terms:
					type_status_terms.append(type_status)

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

		if len(family_terms) > 0:
			family_values = ", ".join(family_terms)
		else:
			family_values = ""

		if len(vernacular_name) > 0:
			vern_values = ", ".join(vernacular_name)
		else:
			vern_values = ""

		if len(type_status_terms) > 0:
			type_status_values = ", ".join(type_status_terms)
		else:
			type_status_values = ""

		identification_values = [date_values, id_values, qual_values, family_values, vern_values, type_status_values]

		return identification_values

write_api_data()