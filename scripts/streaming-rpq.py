#!/usr/bin/python

import json
import os
import sys
import subprocess
import time

class RPQRun:
    name        =   ""
    input       =   ""
    input_type  =   ""
    report      =   ""
    buffer_size =   100000000
    semantics   =   ""
    labels      =   []

    def __init__(self, name, input, input_type, report, buffer_size, semantics, labels):
        self.name = name
        self.input = input
        self.input_type = input_type
        self.report_file = report
        self.buffer_size = buffer_size
        self.semantics = semantics
        self.labels = labels

    def produceCommandString(self):
        command = "-f {} -t {} -s {} -n {} -ps {} -r {} -l {} ".format(
            self.input,
            self.input_type,
            str(self.buffer_size),
            self.name,
            self.semantics,
            self.report_file,
            " ".join(labels)
        )

        return command

# read the command line arguments
if len(sys.argv) != 2:
    print "Provide configuration file as an argument"
    sys.exit()

# second argument is the parameters file
parameters = sys.argv[1]

# list to hold all the objects for this set of experiments
run_list = []
heap_size = 128
timeout = 600
executable = ""

# parse json files and populate Run objects
with open(parameters, 'rb') as parameters_handle:
    parameters_json = json.load(parameters_handle)
    run_configs = parameters_json["runs"]

    # global parameters
    dataset_location = parameters_json["dataset"]
    report_folder = parameters_json["report-folder"]
    buffer_size = parameters_json["buffer-size"]
    heap_size = parameters_json["heap-size"]
    timeout = parameters_json["timeout"]
    executable = parameters_json["executable"]
    input_type = parameters_json["input-type"]

# iterate overs run specific parameters
    for run_config in run_configs:
        query_name = run_config["query-name"]
        index = run_config["index"]
        semantics = run_config["semantics"]
        labels = run_config["labels"]

        # reporting folder
        report_csv_path = os.path.join(report_folder, query_name + "-" + str(index) + "-" +  semantics)

        # create the run object
        run_list.append(RPQRun(query_name, dataset_location, input_type, report_csv_path, buffer_size, semantics, labels))


# iterate over runs and run the experiments
for run in run_list:
    commandString = run.produceCommandString()
    javaCommand = "java -Xms{}g -Xmx{}g -jar {} {}".format(heap_size, heap_size, executable, commandString)

    print "Executing command {} ".format(javaCommand)

    # proc = subprocess.Popen(javaCommand, shell=True)
    proc = subprocess.Popen(javaCommand.split())
    time.sleep(timeout)

    # kill after timeout if process is still alive
    if proc.poll() is None:
        print "Killing pid {} after timeout {}".format(str(proc.pid), str(timeout))
        proc.kill()

print "All runs are completed"

