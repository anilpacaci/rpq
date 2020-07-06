#!/usr/bin/python

import json
import os
import sys
import subprocess
import time


# parse a range of integer from a given string
def range_parser(x):
    result = []
    for part in x.split(','):
        if '-' in part:
            a, b = part.split('-')
            a, b = int(a), int(b)
            result.extend(range(a, b + 1))
        else:
            a = int(part)
            result.append(a)
    return result

class RPQRun:
    name        =   ""
    input       =   ""
    query_directory = ""
    input_type  =   ""
    report      =   ""
    buffer_size =   100000000
    window_size =   50000000
    slide_size  =   10000000
    semantics   =   ""

    def __init__(self, name, input, query_directory, input_type, report, buffer_size, window_size, slide_size, thread_count, delete_ratio, semantics):
        self.name = name
        self.input = input
        self.query_directory = query_directory
        self.input_type = input_type
        self.report_file = report
        self.buffer_size = buffer_size
        self.window_size = window_size
        self.slide_size = slide_size
        self.semantics = semantics
        self.thread_count = thread_count
        self.delete_ratio = delete_ratio

    def produceCommandString(self):
        command = "-f {} -q {} -t {} -tc {}  -dr {} -s {} -ws {} -ss {} -n {} -ps {} -r {} ".format(
            self.input,
            self.query_directory,
            self.input_type,
            str(self.thread_count),
            str(self.delete_ratio),
            str(self.buffer_size),
            str(self.window_size),
            str(self.slide_size),
            self.name,
            self.semantics,
            self.report_file
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
    query_directory = parameters_json["query-directory"]
    report_folder = parameters_json["report-folder"]
    buffer_size = parameters_json["buffer-size"]
    heap_size = parameters_json["heap-size"]
    timeout = parameters_json["timeout"]
    executable = parameters_json["executable"]
    input_type = parameters_json["input-type"]

# iterate overs run specific parameters
    for run_config in run_configs:
        query_range = run_config["query-range"]
        semantics = run_config["semantics"]
        window_size = run_config["window-size"]
        slide_size = run_config["slide-size"]
        thread_count = run_config["thread-count"]
        delete_ratio = run_config.get("delete-ratio", 0)

        for query_num in range_parser(query_range):
            query_name = "query-" + str(query_num)
            # reporting folder
            report_csv_path = os.path.join(report_folder, query_name + "-" +  semantics + "-ws:" + str(window_size) + "-ss:" + str(slide_size) + "-tc:" + str(thread_count) + "-dr:" + str(delete_ratio))

            # create the run object
            run_list.append(RPQRun(query_name, dataset_location, query_directory, input_type, report_csv_path, buffer_size, window_size, slide_size, thread_count, delete_ratio, semantics))


# iterate over runs and run the experiments
for run in run_list:
    commandString = run.produceCommandString()
    javaCommand = "java -XX:+UnlockExperimentalVMOptions -XX:+UseZGC -Xms{}g -Xmx{}g -cp {} ca.uwaterloo.cs.streamingrpq.runtime.VirtuosoQueryRunner {}".format(heap_size, heap_size, executable, commandString)

    print "Executing command {} ".format(javaCommand)
    sys.stdout.flush()

    elapsedTime = 0
    interval = 10
    # wait before firing up the job
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
            # sleep before starting new job for java to release the memory
            time.sleep(interval)
            break

        if proc.poll() is not None:
            break

print "All runs are completed"

