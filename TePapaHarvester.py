# -*- coding: utf-8 -*-

import os
HARVEST_MODEL_PATH = os.environ.get("HARVEST-MODEL-PATH")

import requests
import json
import math
import time
import askCO
from PIL import Image
from io import BytesIO

class Harvester():

    # Harvest from the Collections Online API

    def __init__(self, quiet=False, sleep=1):
        self.quiet = quiet
        self.sleep = sleep
        self.count = 0
        self.record_data_dict = {}

        self.API = askCO.CoApi(quiet=self.quiet)

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
        # Removed facets=self.facets parameter for now
        api_call = self.API.search(q=self.q, fields=self.fields, filters=self.filters, q_from=self.q_from, size=1, sort=self.sort)

        self.count = api_call.result_count
        print("Record count: {}".format(self.count))
        
        return self.count

    def harvest_records(self):
        page_count = math.ceil(self.count/self.size)

        # Query each page to allow harvest
        for i in range(0, page_count):
            # Removed facets=self.facets parameter for now
            page_response = self.API.search(q=self.q, fields=self.fields, filters=self.filters, q_from=self.q_from, size=self.size, sort=self.sort)
            for record in page_response.records:
                if "id" in record:
                    irn = str(record["id"])

                    resource_type = None
                    if record["type"] == "Object" or record["type"] == "Specimen":
                        resource_type = "object"

                    new_record = ApiRecord(irn=irn, resource_type=resource_type, record=record)

                    new_data = new_record.add_data()

                    self.record_data_dict.update({irn:new_data})
                else:
                    pass

            self.q_from += self.size
            time.sleep(self.sleep)

        return self.record_data_dict

    def harvest_from_list(self, resource_type, irns):
        for irn in irns:
            response = self.API.view_resource(resource_type=resource_type, irn=irn)
            if response.errors == None:
                record = response.resource

                new_record = ApiRecord(irn=irn, resource_type=resource_type, record=record)

                new_data = new_record.add_data()

                self.record_data_dict.update({irn:new_data})

                time.sleep(self.sleep)
            else:
                pass

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
        self.object_type = self.record["type"]

        with open(HARVEST_MODEL_PATH, 'r', encoding="utf-8") as f:
            self.harvest_model = json.load(f)

    def add_data(self):
        self.get_resource_data()
#        print(self.fields)
        
        self.fields.update({"CO_url":"https://collections.tepapa.govt.nz/object/{}".format(self.irn)})
#        print(self.fields)

        if self.get_thumbs == True:
            self.get_thumbnails()

        return self.fields

    def get_resource_data(self):
        for key in self.harvest_model.keys():
            field_model = self.harvest_model[key]
            if self.object_type in field_model["object"]:
                data = None
                if field_model["values"] == "single":
                    data = self.single_field_harvest(self.record, field_model)

                elif field_model["values"] == "list" and field_model["value_type"] == "dict":
                    data = self.dict_field_harvest(self.record, field_model)

                elif field_model["values"] == "list":
                    data = self.list_field_harvest(self.record, field_model)
                
                else:
                    pass
                
                if data is not None:
                    self.fields.update(data)
            else:
                pass

    def single_field_harvest(self, record, field_model):
        field_label = field_model["api_label"]
#        print(field_label)
        harvester_label = field_model["default_harvester_label"]
        field_value = None

        if len(field_model["path"]) > 0:
            record = self.step_through(record, field_model["path"])

        if record is not None:

            if field_label in record:
                field_value = record[field_label]

            if field_value is not None:
                return {harvester_label: field_value}
            else:
                return None

        else:
            return None

    def list_field_harvest(self, record, field_model):
        field_label = field_model["api_label"]
        harvester_label = field_model["default_harvester_label"]
        field_value = []

        if isinstance(field_label, str):
            labels = [field_label]
        else:
            labels = field_label

        if len(field_model["path"]) > 0:
            record = self.step_through(record, field_model["path"])

        if record is not None:
            for label in labels:
                if isinstance(record, dict):
                    if label in record:
                        for value in record[label]:
                            field_value.append(value)
                elif isinstance(record, list):
                    for i in record:
                        if label in i:
                            if "check_against" in field_model:
                                check_field = field_model["check_against"]
                                if check_field in i:
                                    value = i[label]
                                    check_value = i[check_field]
                                    value_tuple = (value, check_value)
                                    field_value.append(value_tuple)
                            else:
                                field_value.append(i[label])
                else:
                    if label in record:
                        field_value.append(record[label])

                if len(field_value) > 0:
                    return {harvester_label: field_value}
                else:
                    return None

        else:
            return None

    def dict_field_harvest(self, record, field_model):
        field_label = field_model["api_label"]
        harvester_label = field_model["default_harvester_label"]

        block_data = {harvester_label: []}

        if isinstance(field_label, str):
            labels = [field_label]
        else:
            labels = field_label

        if len(field_model["path"]) > 0:
            record = self.step_through(record, field_model["path"])

        if record is not None:

            for label in labels:
                if label in record:
                    for iter_record in record[label]:
                        iteration_data = {}
                        for key in field_model["children"].keys():
                            child_field = field_model["children"][key]
                            if field_model["children"][key]["values"] == "single":
                                data = self.single_field_harvest(iter_record, child_field)
                            elif field_model["children"][key]["values"] == "list":
                                data = self.list_field_harvest(iter_record, child_field)
                                
                            if data is not None:
                                iteration_data.update(data)
                
                        block_data[harvester_label].append(iteration_data)

            if len(block_data[harvester_label]) > 0:
                return block_data
            else:
                return None

        else:
            return None

    def step_through(self, step, path):
        stepped = False
        for i in path:
            if i in step:
                step = step[i]
                stepped = True
            else:
                stepped = False
        if stepped == True:
            return step
        else:
            return None

    def get_thumbnails(self):
        image_filename = None
        thumbnail_url = None

        for image in self.record["hasRepresentation"]:
            if "id" in image:
                media_irn = image["id"]
                image_filename = str(self.record["irn"] + 
                str(media_irn))
            if "thumbnailUrl" in image:
                thumbnail_url = image["thumbnailUrl"]

            if image_filename and thumbnail_url:
                r = requests.get(image_thumbnail_url, headers=headers, stream=True)
                thumb = Image.open(BytesIO(r.content))
                thumb = thumb.save("{image_folder}{image_filename}".format(image_folder=self.image_folder, image_filename=image_filename))