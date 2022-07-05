# Dang it this needs so much work
    def harvest_combined(self):
        harvestable_fields = harvest_model.keys()
        for field in harvestable_fields:
            if field["for"] == "combined":
                if field["type"] == "single":
                    field_label = field["field_label"]
                    if self.record[field_label]:
                        field_value = self.record[field_label]
                        self.fields.update({field_label:field_value})
                        if field_label == "title":
                            title_length = str(len(self.record[field_label]))
                            self.fields.update("title_length":title_length)
                elif field["type"] == "list":
                    no_of_steps = len(field["field_labels"])
                    for f in field["field_labels"]:
                    if self.record[field_label]:
                        field_list = []
                        field_num = 0
                        for i in self.record[field_label]:
                            field_value = i[field_num][field_path]
            if self.record["productionUsedTechnique"]:
            prod_tech_section = self.record["productionUsedTechnique"]
            prod_tech = []
            prod_num = 0
            for prod_used in prod_tech_section[prod_num]:
                prod_tech.append(prod_used["title"])
                prod_num += 1

            self.fields.update({"productionUsedTechnique": prod_tech})