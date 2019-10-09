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
					csvwriter.writerow([row[3], row[4], "likes", row[5]])
				elif(row[2] == "3"):
					csvwriter.writerow([row[3], row[4], "likes", row[5]])
				elif(row[2] == "5"):
					csvwriter.writerow([row[3], row[4], "member", row[5]])
					csvwriter.writerow([row[4], row[3], "hasMember", row[5]])
				elif(row[2] == "6"):
					csvwriter.writerow([row[3], row[11], "created", row[5]])
					csvwriter.writerow([row[11], row[3], "creatorOf", row[5]])
					csvwriter.writerow([row[3], row[12], "container", row[5]])
					csvwriter.writerow([row[12], row[3], "containerOf", row[5]])
				elif(row[2] == "7"):
					csvwriter.writerow([row[3], row[9], "created", row[4]])
					csvwriter.writerow([row[9], row[3], "creatorOf", row[4]])
					if(row[11] == "-1"):
						csvwriter.writerow([row[3], row[12], "replyOf", row[4]])
						csvwriter.writerow([row[12], row[3], "reply", row[4]])
					else:
						csvwriter.writerow([row[3], row[11], "replyOf", row[4]])
						csvwriter.writerow([row[11], row[3], "reply", row[4]])
				elif(row[2] == "8"):
					csvwriter.writerow([row[3], row[4], "knows", row[5]])
					csvwriter.writerow([row[4], row[3], "knows", row[5]])


