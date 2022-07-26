# -*- coding: utf-8 -*-

import os
import requests
import json
import math
import time
import askCO
from PIL import Image
from io import BytesIO

class Harvester():

    # Harvest from the Collections Online API

    def __init__(self, quiet=True):
        self.quiet = quiet
        self.count = 0
        self.record_data_dict = {}

        self.API = askCO.CoApi()

    # Establish the parameters for this operation so you can build queries
    def set_params(self, q=None, fields=None, filters=None, facets=None, q_from=0, size=0, sort=None):
        self.q = q
        self.fields = fields
        self.filters = filters
        self.facets = facets
        self.q_from = q_from
        self.size = size
        self.sort = sort

    def count_results(self):
        # Find out how many results there are and how many pages to query
        api_call = self.API.search(q=self.q, fields=self.fields, filters=self.filters, facets=self.facets, q_from=self.q_from, size=1, sort=self.sort)

        self.count = api_call.result_count
        
        return self.count

    def harvest_records(self):
        page_count = math.ceil(self.count/self.size)

        # Query each page to allow harvest
        for i in range(0, page_count):
            page_response = self.API.search(q=self.q, fields=self.fields, filters=self.filters, facets=self.facets, q_from=self.q_from, size=self.size, sort=self.sort)
            for record in page_response.records:
                try:
                    irn = str(record["id"])

                    new_record = ApiRecord(irn=irn, record=record)

                    new_data = new_record.add_data()

                    self.record_data_dict.update({irn:new_data})
                except: pass

            self.q_from += self.size
            time.sleep(0.2)
        print(self.count)

        return self.record_data_dict

class ApiRecord():
    # Stores the data for each record in a dict
    def __init__(self, irn=None, resource_type=None, record=None, get_thumbs=False, image_folder=None):
        self.irn = irn
        self.fields = {"IRN":self.irn}
        self.resource_type = resource_type
        self.record = record
        self.get_thumbs = get_thumbs
        self.image_folder = image_folder
        self.object_type = None

    def add_data(self):
        self.get_combined_data()
#        print(self.fields)
        if self.object_type == "Object":
            self.get_object_data()
        else: pass
        
        self.fields.update({"CO_url":"https://collections.tepapa.govt.nz/object/{}".format(self.irn)})
#        print(self.fields)
        
        return self.fields

    def get_combined_data(self):
        # Stores the data needed by both Objects and Specimens
        # Particularly: pid, title, description, and image metadata
        pid = self.record["pid"]
        self.fields.update({"pid":pid})
#        print(self.fields["IRN"])

        self.object_type = self.record["type"]
        self.fields.update({"type":self.object_type})

        if "title" in self.record:
            title = self.record["title"]
            title_length = str(len(title))
            self.fields.update({"title":title})
            self.fields.update({"title_length":title_length})
        else:
            self.fields.update({"title":"None"})
            self.fields.update({"title_length":0})

#        print(self.fields["IRN"])

        if "description" in self.record:
            description = self.record["description"]
            self.fields.update({"description":description})
        else: pass

#        print(self.fields["IRN"])

        if "observedDimension" in self.record:
            observedDimension = self.record["observedDimension"][0]["title"]
            self.fields.update({"observedDimension":observedDimension})
        else: pass

        if "hasRepresentation" in self.record:
            images = self.record["hasRepresentation"]
            image_list_length = len(images)
            images_data_dict = {"images":[]}
            for i in range(0,image_list_length):
                this_data_set = {}
                image_irn = None
                if images[i]:
                    i_dat = images[i]
                    if "id" in i_dat:
                        image_irn = str(i_dat["id"])
                        this_data_set.update({"image_irn":image_irn})
                    else: pass
                    if "type" in i_dat:
                        image_type = i_dat["type"]
                        this_data_set.update({"image_type":image_type})
                        if image_type == "ImageObject":
                            if "width" in i_dat:
                                image_w = i_dat["width"]    
                            else:
                                image_w = "n/a"
                            this_data_set.update({"image_w":image_w})
                            if "height" in i_dat:
                                image_h = i_dat["height"]
                            else:
                                image_h = "n/a"
                            this_data_set.update({"image_h":image_h})
                        else: pass
                    else: pass
                    if "contentUrl" in i_dat:
                        image_url = i_dat["contentUrl"]
                        this_data_set.update({"image_url":image_url})
                    else: pass
                    if "thumbnailUrl" in i_dat:
                        image_thumbnail_url = i_dat["thumbnailUrl"]
                        this_data_set.update({"image_thumb_url":image_thumbnail_url})
                        
                        if self.get_thumbs == True:
                            image_filename = self.irn + "_" + image_irn + ".jpg"
                            self.get_thumbnails(image_thumbnail_url=image_thumbnail_url, image_filename=image_filename)
                        else: pass
                    else: pass
                    if "rights" in i_dat:
                        if "title" in i_dat["rights"]:
                            image_rights = i_dat["rights"]["title"]
                            this_data_set.update({"image_rights":image_rights})
                        else: pass
                        if "iri" in i_dat["rights"]:
                            image_rights_url = i_dat["rights"]["iri"]
                            this_data_set.update({"image_rights_url":image_rights_url})
                        else: pass  
                        if "allowsDownload" in i_dat["rights"]:
                            image_download = i_dat["rights"]["allowsDownload"]
                            this_data_set.update({"image_download":image_download})
                        else: pass                   

                    images_data_dict["images"].append(this_data_set)
                else: pass
#                print(images_data_dict)

                self.fields.update(images_data_dict)
#                print(self.fields)

            self.fields.update({"number_of_images": image_list_length})

        else: pass

    def get_object_data(self):
        # Stores the data that only Objects have
        # Particularly: production, topical, and representational metadata
        if "production" in self.record:
            prod_section = self.record["production"]
            prod_list_length = len(prod_section)
            prod_num = 0
            prod_data_dict = {}
            for i in range(0,prod_list_length):
                prod_data_set = {}
                if prod_section[prod_num]:
                    p_dat = prod_section[prod_num]
                    if "contributor" in p_dat:
                        prod_creator = p_dat["contributor"]["title"]
                        prod_data_set.update({"prod_{}_creator".format(prod_num):prod_creator})
                    else: pass
                    if "role" in p_dat:
                        prod_role = p_dat["role"]
                        prod_data_set.update({"prod_{}_role".format(prod_num):prod_role})
                    else: pass
                    if "verbatimCreatedDate" in p_dat:
                        prod_date = p_dat["verbatimCreatedDate"]
                        prod_data_set.update({"prod_{}_date".format(prod_num):prod_date})
                    else: pass
                    if "spatial" in p_dat:
                        prod_place = p_dat["spatial"]["title"]
                        prod_data_set.update({"prod_{}_place".format(prod_num):prod_place})
                    else: pass

                    prod_data_dict.update({"prod_{}".format(prod_num):prod_data_set})
                else: pass
                self.fields.update(prod_data_dict)

                prod_num += 1
        else: pass

        if "productionUsedTechnique" in self.record:
            prod_tech_section = self.record["productionUsedTechnique"]
            prod_tech = []
            for prod_used in prod_tech_section:
                if "title" in prod_used:
                    prod_tech.append(prod_used["title"])
                else: pass

            self.fields.update({"productionUsedTechnique": prod_tech})
        else: pass

        if "isMadeOfSummary" in self.record:
            made_of = self.record["isMadeOfSummary"]
            self.fields.update({"isMadeOfSummary": made_of})
        else: pass

        if "isMadeOf" in self.record:
            made_of_section = self.record["isMadeOf"]
            made_of_cats = []
            for made_of in made_of_section:
                if "title" in made_of:
                    made_of_cats.append(made_of["title"])
                else: pass

            self.fields.update({"isMadeOf": made_of_cats})
        else: pass

        if "isTypeOf" in self.record:
            type_of_section = self.record["isTypeOf"]
            type_of_cats = []
            for type_of in type_of_section:
                # poss run against whitelist and only select matches
                if "title" in type_of:
                    type_of_cats.append(type_of["title"])
                else: pass

            self.fields.update({"isTypeOf": type_of_cats})
        else: pass

        if "influencedBy" in self.record:
            influence_section = self.record["influencedBy"]
            inf_cats = []
            for influencer in influence_section:
                if "title" in influencer:
                    inf_cats.append(influencer["title"])
                else: pass

            self.fields.update({"influencedBy": inf_cats})
        else: pass

        if "depicts" in self.record:
            depicts_section = self.record["depicts"]
            depicts_cats = []
            for depicted in depicts_section:
                if "prefLabel" in depicted:
                    depicts_cats.append(depicted["prefLabel"])
                else: pass

            self.fields.update({"depicts": depicts_cats})
        else: pass

        if "refersTo" in self.record:
            refers_section = self.record["refersTo"]
            refers_cats = []
            for referent in refers_section:
                if "prefLabel" in referent:
                    refers_cats.append(referent["prefLabel"])
                else: pass

            self.fields.update({"refersTo": refers_cats})
        else: pass

        if "creditLine" in self.record:
            credit_line = self.record["creditLine"]
            self.fields.update({"creditLine":credit_line})
        else: pass

    def get_thumbnails(self, image_thumbnail_url, image_filename):
        r = requests.get(image_thumbnail_url, headers=headers, stream=True)
        print(image_thumbnail_url)
        thumb = Image.open(BytesIO(r.content))
        thumb = thumb.save("{image_folder}{image_filename}".format(image_folder=self.image_folder, image_filename=image_filename))

# Might try and do this when I have all the fields figured out
'''
    def harvest_data_list(self, section_name, field_name):
        try:
            harvest_section = self.record[section_name]
            harvest_list = []
            harvest_num = 0
            for item in harvest_section[harvest_num]:
                harvest_list.append(item[field_name])
                harvest_num += 1

            self.fields.update({section_name:str(harvest_list)})

    def harvest_data_single(self, field_name):
        try:
            harvest_item = self.record[field_name]

            self.fields.update({field_name:harvest_item})
'''



# Silly little function to see if I can choose to harvest a single record
#def single_record(doc_type, irn):
#    api_call = CoApi(headers=headers, doc_type=doc_type, irn=irn)
#    response = api_call.view_record()
#    print(response)
#    harvested_data = ApiRecord(irn=irn, record_type=doc_type, record=response).add_data()
#    print(harvested_data)

#single_record(doc_type="object", irn="35270")