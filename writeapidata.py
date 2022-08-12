# -*- coding: utf-8 -*-

import os
from datetime import datetime
import time
import csv
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

    def write_line(self, irn=None, data_dict=None, image_irn=None, no_of_images=None, first=False):
        self.irn = irn
        self.data_dict = data_dict
        self.image_irn = image_irn
        self.first = first

        value_list = []
        
        value_list.append(self.irn)

        if "identifier" in self.data_dict:
            value_list.append(self.data_dict["identifier"])

        if "title" in self.data_dict:
            value_list.append(self.data_dict["title"])
            value_list.append(len(self.data_dict["title"]))
        else:
            value_list.append("")
            value_list.append("")

        value_list.append(no_of_images)

        value_list.append(self.image_irn)

        images_data = self.data_dict["media"]
        image_fields = ["media_title", "media_type", "contentUrl", "thumbnailUrl", "rights_title", "media_width", "media_height"]

        for image in images_data:
            if image["media_irn"] == self.image_irn:
                for field in image_fields:
                    if field in image:
                        write_value = image[field]
                        value_list.append(write_value)
                    else:
                        value_list.append("")

        if "description" in self.data_dict:
            value_list.append(self.data_dict["description"])
        else:
            value_list.append("")

        if "dimensions" in self.data_dict:
            value_list.append(", ".join(self.data_dict["dimensions"]))
        else:
            value_list.append("")

        if "production" in self.data_dict:
            values = self.compile_production(self.data_dict["production"])
            for value in values:
                value_list.append(value)
        
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

        if "isTypeOf" in self.data_dict:
            value_list.append(", ".join(self.data_dict["isTypeOf"]))
        else:
            value_list.append("")

        if "influencedBy" in self.data_dict:
            value_list.append(", ".join(self.data_dict["influencedBy"]))
        else:
            value_list.append("")

        if "depicts" in self.data_dict:
            value_list.append(", ".join(self.data_dict["depicts"]))
        else:
            value_list.append("")

        if "refersTo" in self.data_dict:
            value_list.append(", ".join(self.data_dict["refersTo"]))
        else:
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

            if "production_date" in prod:
                if prod["production_date"] not in dates:
                    dates.append(prod["production_date"])

            if "production_place" in prod:
#                if self.first == True:
#                    if "production_place_id" in prod:
#                        lat_long = self.get_spatial(prod["production_place_id"])
#                        time.sleep(0.1)
#                        if lat_long:
#                            if lat_value == None:
#                                lat_value = lat_long[0]
#                            if long_value == None:
#                                long_value = lat_long[1]
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

    heading_row = ["irn", "identifier", "title", "title_length", "number_of_images", "media_irn", "media_title", "media_type", "contentUrl", "thumbnailUrl", "rights_title", "width", "height", "description", "observedDimension", "creator", "createdDate", "createdPlace", "lat", "long", "productionUsedTechnique", "isMadeOfSummary", "isMadeOf", "art=support", "isTypeOf", "influencedBy", "depicts", "refersTo", "creditLine", "qualityScore", "CO_url", "CO_url_text"]

    output_csv = OutputCSV(filename=csv_filename, heading_row=heading_row)

    all_irns = record_data_dict.keys()
    #print(all_irns)

    # Need to mark a specific row as the first for that irn so we can avoid duplication
    for irn in all_irns:
        first = True
        irn_dat = record_data_dict[irn]

        no_of_images = len(irn_dat["media"])
        
        attached_images = irn_dat["media"]
        for image in attached_images:
            if "media_width" in image:
                if image["media_width"] >= 2500 and image["media_height"] >= 2500:
                    if "downloadable" in image:
                        if image["downloadable"] == True:
                            writable_image_irn = image["media_irn"]
                            output_csv.write_line(irn=irn, data_dict=irn_dat, image_irn=writable_image_irn, no_of_images=no_of_images, first=first)
                            first = False

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

google_harvest_fields = ["itemid", "subitemid", "orderid", "customtext:registrationid", "title/en", "description/en", "creator/en", "location:placename", "location:lat", "location:long", "dateCreated:start", "dateCreated:display", "rights", "format", "medium", "subject", "art=support", "art=depictedLocation.placename", "art=depictedPerson", "art=genre", "provenance", "priority", "filetype", "filespec", "relation:url", "relation:text"]

harvester = TePapaHarvester.Harvester(quiet=True, sleep=0.2)

list_API()
#search_API()