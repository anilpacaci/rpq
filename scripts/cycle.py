#!/usr/bin/python

import json
import os
import sys
import subprocess
import time

class RPQRun:
    name        =   ""
    input       =   ""
    labels      =   []

    def __init__(self, name, input, labels):
        self.name = name
        self.input = input
        self.labels = labels

    def produceCommandString(self):
        command = " {} {} {} ".format(
            self.input,
            self.name,
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
    heap_size = parameters_json["heap-size"]
    timeout = parameters_json["timeout"]
    executable = parameters_json["executable"]

# iterate overs run specific parameters
    for run_config in run_configs:
        index = str(run_config["index"])
        labels = run_config["labels"]

     # create the run object
        run_list.append(RPQRun(index, dataset_location, labels))


# iterate over runs and run the experiments
for run in run_list:
    commandString = run.produceCommandString()
    javaCommand = "java -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=32768 -Xms{}g -Xmx{}g -cp {} ca.uwaterloo.cs.streamingrpq.transitiontable.util.cycle.SimpleCycleDFS {}".format(heap_size, heap_size, executable, commandString)

    print "Executing command {} ".format(javaCommand)

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
            proc.kill()
            break

        if proc.poll() is not None:
            break

print "All runs are completed"

