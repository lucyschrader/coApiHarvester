# -*- coding: utf-8 -*-

import os
from datetime import datetime
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

    def write_line(self, irn=None, data_dict=None, image_irn=None):
        self.irn = irn
        self.data_dict = data_dict
        self.image_irn = image_irn

        value_list = []
        value_list.append(self.irn)
        value_list.append(self.data_dict["title"])
        value_list.append(self.data_dict["title_length"])

        if "number_of_images" in self.data_dict:
            value_list.append(self.data_dict["number_of_images"])
        else:
            value_list.append("")

        value_list.append(self.image_irn)

        images_data = self.data_dict["images"]
        image_fields = ["image_w", "image_h","image_url", "image_rights"]

        for image in images_data:
            if image["image_irn"] == self.image_irn:
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

        if "observedDimension" in self.data_dict:
            value_list.append(self.data_dict["observedDimension"])
        else:
            value_list.append("")

        no_of_prods = 0
        for k in self.data_dict.keys():
            if "prod_" in k:
                no_of_prods += 1

        if "prod_0" in self.data_dict:
            creator_titles = []
            creator_roles = []
            for n in range(0,no_of_prods):
                field_prod = "prod_{}".format(n)
                field_prod_creator = "prod_{}_creator".format(n)
                field_prod_role = "prod_{}_role".format(n)
                if field_prod_creator in self.data_dict[field_prod]:
                    creator_titles.append(self.data_dict[field_prod][field_prod_creator])
                else:
                    creator_titles.append("")
                if field_prod_role in self.data_dict[field_prod]:
                    creator_roles.append(self.data_dict[field_prod][field_prod_role])
                else:
                    creator_roles.append("")
            value_list.append(",".join(creator_titles))
            value_list.append(",".join(creator_roles))
            if "prod_0_date" in self.data_dict["prod_0"]:
                value_list.append(self.data_dict["prod_0"]["prod_0_date"])
            else:
                value_list.append("")
            if "prod_0_place" in self.data_dict["prod_0"]:
                value_list.append(self.data_dict["prod_0"]["prod_0_place"])
            else:
                value_list.append("")
        else:
             value_list.append(["", "", "", ""])
        
        if "productionUsedTechnique" in self.data_dict:
            value_list.append(",".join(self.data_dict["productionUsedTechnique"]))
        else:
            value_list.append("")
        if "isMadeOfSummary" in self.data_dict:
            value_list.append(self.data_dict["isMadeOfSummary"])
        else:
            value_list.append("")
        if "isMadeOf" in self.data_dict:
            value_list.append(",".join(self.data_dict["isMadeOf"]))
        else:
            value_list.append("")
        if "isTypeOf" in self.data_dict:
            value_list.append(",".join(self.data_dict["isTypeOf"]))
        else:
            value_list.append("")
        if "influencedBy" in self.data_dict:
            value_list.append(",".join(self.data_dict["influencedBy"]))
        else:
            value_list.append("")
        if "depicts" in self.data_dict:
            value_list.append(",".join(self.data_dict["depicts"]))
        else:
            value_list.append("")
        if "refersTo" in self.data_dict:
            value_list.append(",".join(self.data_dict["refersTo"]))
        else:
            value_list.append("")
        if "creditLine" in self.data_dict:
            value_list.append(self.data_dict["creditLine"])
        else:
            value_list.append("")

        self.writer.writerow(value_list)
#       print(value_list)

def write_data_to_csv(record_data_dict, collection=None):
	# Complete structured CSV with all records and all images
    csv_filename = working_folder + current_time + "_" + collection + ".csv"

    heading_row = ["irn", "title", "title_length", "number_of_images", "image_irn", "width", "height", "contentUrl", "rights", "description", "observedDimension", "creator", "role", "createdDate", "createdPlace", "productionUsedTechnique", "isMadeOfSummary", "isMadeOf", "isTypeOf", "influencedBy", "depicts", "refersTo", "creditLine"]

    output_csv = OutputCSV(filename=csv_filename, heading_row=heading_row)

    all_irns = record_data_dict.keys()
    #print(all_irns)

    for irn in all_irns:
        irn_dat = record_data_dict[irn]
        no_of_images = irn_dat["number_of_images"]

        attached_images = irn_dat["images"]
        for image in attached_images:
            if "image_w" in image:
                if image["image_w"] >= 2500 and image["image_h"] >= 2500:
                    if "image_download" in image:
                        if image["image_download"] == True:
                            writable_image_irn = image["image_irn"]
                            output_csv.write_line(irn=irn, data_dict=irn_dat, image_irn=writable_image_irn)

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

collection = "PacificCultures"
per_page = 500

harvester = TePapaHarvester.HarvestDict(collection=collection, per_page=per_page)

record_data_dict = harvester.harvest_records()

write_data_to_csv(record_data_dict, collection=collection)
#just_print_titles(record_data_dict, collection=collection, cutoff=100)
#just_print_subjects(record_data_dict, collection=collection)
#just_print_roles(record_data_dict, collection=collection)
#just_print_irns(record_data_dict, collection=collection)