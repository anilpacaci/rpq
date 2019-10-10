#!/usr/bin/python

import csv
import sys
import os

# update filename
fileNameTemplate = "updateStream_0_X_forum.csv";

# read the command line arguments
if len(sys.argv) != 3:
	print "Provide input folder and output files"
	sys.exit()

# second argument is the input folder file
input_folder = sys.argv[1]
output_folder = sys.argv[2]

for i in range(8):
	# replace x with the actual range
	inputFileName = os.path.join(input_folder, fileNameTemplate.replace("X", str(i)))
	outputFileName = os.path.join(output_folder, fileNameTemplate.replace("X", str(i)))

	with open(outputFileName, 'w') as outputcsvfile:
		csvwriter = csv.writer(outputcsvfile, delimiter='\t')
		# open the input csv file
		with open(inputFileName, 'rb') as inputcsvfile:
			csvreader = csv.reader(inputcsvfile, delimiter='|')
			for row in csvreader:
				if(row[2] == "2"):
					csvwriter.writerow([row[3], "likes", row[4],row[5]])
				elif(row[2] == "3"):
					csvwriter.writerow([row[3], "likes", row[4], row[5]])
				elif(row[2] == "5"):
					csvwriter.writerow([row[3], "memberOf", row[4], row[5]])
					csvwriter.writerow([row[4], "hasMember", row[3], row[5]])
				elif(row[2] == "6"):
					csvwriter.writerow([row[3], "createdBy", row[11], row[5]])
					csvwriter.writerow([row[11], "creatorOf", row[3], row[5]])
					csvwriter.writerow([row[3], "container", row[12], row[5]])
					csvwriter.writerow([row[12], "containerOf", row[3], row[5]])
				elif(row[2] == "7"):
					csvwriter.writerow([row[3], "created", row[9], row[4]])
					csvwriter.writerow([row[9], "creatorOf", row[3], row[4]])
					if(row[11] == "-1"):
						csvwriter.writerow([row[3], "replyOf", row[12], row[4]])
						csvwriter.writerow([row[12], "reply", row[3], row[4]])
					else:
						csvwriter.writerow([row[3], "replyOf", row[11], row[4]])
						csvwriter.writerow([row[11], "reply", row[3], row[4]])
				elif(row[2] == "8"):
					csvwriter.writerow([row[3], "knows", row[4], row[5]])
					csvwriter.writerow([row[4], "knows", row[3], row[5]])


