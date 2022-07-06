# -*- coding: utf-8 -*-

import os
import requests
import json
import math
import time
from PIL import Image
from io import BytesIO

auth_key = "x-api-key"
auth_value = os.environ.get('TE-PAPA-KEY')

headers = {auth_key: auth_value, "Accept": "application/json"}

class CoApi():
    # Creates and executes a query to the API
    def __init__(self, headers=headers, pagination_from=None, pagination_size=None, collection=None, doc_type=None, irn=None):
        self.headers = headers
        self.collection = collection
        self.pagination_from = pagination_from
        self.pagination_size = pagination_size
        self.doc_type = doc_type
        self.irn = irn

    def search(self):
        # To add: error handling and retrying
        request = Request(pagination_from=self.pagination_from, pagination_size=self.pagination_size, collection=self.collection)
        print("Requesting {}".format(request.url))
        response = json.loads(requests.get(request.url, headers=self.headers).text)
        return Results(response, request)

    def view_record(self):
        request = Request(doc_type=self.doc_type, irn=self.irn)
        print("Requesting {}".format(request.url))
        response = json.loads(requests.get(request.url, headers=self.headers).text)
        print(response)
        return response

class Request():
    # Builds the query url from supplied parts
    def __init__(self, pagination_from=None, pagination_size=None, collection=None, doc_type=None, irn=None):
        url_parts = []
        
        self.base_url = "https://data.tepapa.govt.nz/collection/"
        
        if collection:
            self.do_search = "search/"
            self.query = "&q=*"
            self.collection = " AND collection:{}".format(collection)
            self.types = " AND type:Object AND additionalType:PhysicalObject"
            self.downloadable = " AND hasRepresentation.rights.allowsDownload:true"
            self.sort = "&sort=id"
            self.pagination_from = pagination_from
            self.pagination_size = pagination_size

            url_parts.append(self.do_search)
            url_parts.append("?")
            url_parts.append("from={}&size={}".format(self.pagination_from, self.pagination_size))
            if self.sort:
                url_parts.append("{}".format(self.sort))
            if self.query:
                url_parts.append("{}".format(self.query))
            if self.collection:
                url_parts.append("{}".format(self.collection))
            if self.types:
                url_parts.append("{}".format(self.types))
            if self.downloadable:
                url_parts.append("{}".format(self.downloadable))
        
        elif doc_type:
            self.doc_type = doc_type
            self.irn = irn

            url_parts.append(self.doc_type)
            url_parts.append("/")
            url_parts.append(self.irn)

        self.url = self.buildUrl(url_parts)
        
    def buildUrl(self, url_parts=None):
        url = [
            self.base_url,
            "".join(url_parts)
        ]
        return ''.join(url)

class Results():
    # Turns the response data into an iterable object
    def __init__(self, response, request):
        self.request = request
        self.result_count = 0
        self.records = []

        self.result_count = response['_metadata']['resultset']['count']
        self.records = [result for result in response['results']]

class ApiRecord():
    # Stores the data for each record in a dict
    def __init__(self, irn=None, doc_type=None, record=None, get_thumbs=False, image_folder=None):
        self.irn = irn
        self.fields = {"IRN":self.irn}
        self.doc_type = doc_type
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

class HarvestDict():
    # Creates and populates a dict for the entire harvest operation
    def __init__(self, collection=None, per_page=None):
        self.collection = collection
        self.per_page = per_page
        self.count = None

        self.count_results()

    def count_results(self):
        api_call = CoApi(headers=headers, pagination_from=0, pagination_size=10, collection=self.collection)

        # Find out how many results there are and how many pages to query
        count_response = api_call.search()
        self.count = count_response.result_count
        print(self.count)

    def harvest_records(self):
        page_start = 0
        page_count = math.ceil(self.count/self.per_page)

        record_data_dict = {}

        # Query each page to allow harvest
        for i in range(0, page_count):
            page_call = CoApi(headers=headers, pagination_from = page_start, pagination_size = self.per_page, collection=self.collection)
            page_reponse = page_call.search()
            for record in page_reponse.records:
                try:
                    irn = str(record["id"])

                    new_record = ApiRecord(irn=irn, record=record)

                    new_data = new_record.add_data()

                    record_data_dict.update({irn:new_data})
                except: pass

            page_start += self.per_page
            time.sleep(1)

        return record_data_dict

# Silly little function to see if I can choose to harvest a single record
#def single_record(doc_type, irn):
#    api_call = CoApi(headers=headers, doc_type=doc_type, irn=irn)
#    response = api_call.view_record()
#    print(response)
#    harvested_data = ApiRecord(irn=irn, record_type=doc_type, record=response).add_data()
#    print(harvested_data)

#single_record(doc_type="object", irn="35270")