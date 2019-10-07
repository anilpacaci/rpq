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
    window_size =   50000000
    slide_size  =   10000000
    semantics   =   ""
    labels      =   []

    def __init__(self, name, input, input_type, report, buffer_size, window_size, slide_size, thread_count, semantics, labels):
        self.name = name
        self.input = input
        self.input_type = input_type
        self.report_file = report
        self.buffer_size = buffer_size
        self.window_size = window_size
        self.slide_size = slide_size
        self.semantics = semantics
        self.labels = labels
        self.thread_count = thread_count

    def produceCommandString(self):
        command = "-f {} -t {} -tc {} -s {} -ws {} -ss {} -n {} -ps {} -r {} -l {}".format(
            self.input,
            self.input_type,
            str(self.thread_count),
            str(self.buffer_size),
            str(self.window_size),
            str(self.slide_size),
            self.name,
            self.semantics,
            self.report_file,
            " ".join(self.labels)
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
        window_size = run_config["window-size"]
        slide_size = run_config["slide-size"]
        thread_count = run_config["thread-count"]
        # reporting folder
        report_csv_path = os.path.join(report_folder, query_name + "-" + str(index) + "-" +  semantics + "-ws:" + str(window_size) + "-ss:" + str(slide_size))

        # create the run object
        run_list.append(RPQRun(query_name, dataset_location, input_type, report_csv_path, buffer_size, window_size, slide_size, thread_count, semantics, labels))


# iterate over runs and run the experiments
for run in run_list:
    commandString = run.produceCommandString()
    javaCommand = "java -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=32768 -Xms{}g -Xmx{}g -jar {} {}".format(heap_size, heap_size, executable, commandString)

    print "Executing command {} ".format(javaCommand)
    sys.stdout.flush()

    elapsedTime = 0
    interval = 5
    # proc = subprocess.Popen(javaCommand, shell=True)
    proc = subprocess.Popen(javaCommand.split())

    while True:
        time.sleep(interval)
        elapsedTime += interval

        # kill after timeout if process is still alive
        if elapsedTime > timeout and proc.poll() is None:
            print "Killing pid {} after timeout {}".format(str(proc.pid), str(timeout))
            sys.stdout.flush()
            proc.kill()
            break

        if proc.poll() is not None:
            break

print "All runs are completed"

