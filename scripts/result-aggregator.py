import csv
import sys
import os

def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]

# read the command line arguments
if len(sys.argv) != 3:
    print "Provide input folder and output files"
    sys.exit()

# second argument is the input folder file
results_folder = sys.argv[1]
aggregated_results_file = sys.argv[2]

log_folders = get_immediate_subdirectories(results_folder)

with open(aggregated_results_file, 'w') as csv_file:
    fieldnames = ['query', 'semantics', 'binding', 'full-edges', 'edges', 'delta', 'mean', 'median', 'p99']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    for log_folder in log_folders:
        edges = ""
        dfst = ""
        full_edges = ""
        mean = ""
        median = ""
        p99 = ""

        full_histogram = os.path.join(results_folder, log_folder, "full-histogram.csv")
        dfst_counter = os.path.join(results_folder, log_folder, "dfst-counter.csv")
        processed_histogram = os.path.join(results_folder, log_folder, "processed-histogram.csv")
        edge_counter = os.path.join(results_folder, log_folder, "edge-counter.csv")

        with open(full_histogram, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            full_edges = row[1]

        with open(dfst_counter, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            dfst = row[1]

        with open(edge_counter, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            edges = row[1]

        with open(processed_histogram, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            mean = row[3]
            median = row[6]
            p99 = row[10]

        query = log_folder.split("-")[0]
        predicates = log_folder.split("-")[1]
        semantics = log_folder.split("-")[2]

        writer.writerow({
            'query' : query,
            'semantics' : semantics,
            'binding' : predicates,
            'full-edges' : full_edges,
            'edges' : edges,
            'delta' : dfst,
            'mean' : mean,
            'median' : median,
            'p99' : p99
        })

        print "Result aggregated for {}".format(log_folder)

print "Aggregated Results written into {}".format(aggregated_results_file)

