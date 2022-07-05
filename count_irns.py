# -*- coding: utf-8 -*-

irn_file = "art_irns.txt"
with open(irn_file, 'r', encoding="utf-8") as f:
	lines = f.readlines()
	deduped = list(dict.fromkeys(lines))
	deduped_len = len(deduped)
	print("Number of IRNS: {}".format(deduped_len))

f.close()