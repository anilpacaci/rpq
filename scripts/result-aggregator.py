import csv
import sys
import os
import re

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
    fieldnames = ['query', 'semantics', 'binding', 'window-size', 'slide-size', 'containing-tree-mean', 'results',
                  'tree-count', 'tree-size-mean', 'tree-size-max', 'window-mean', 'window-p99', 'slide-count-mean',
                  'processed-edge-count', 'delete-ratio' , 'delete-mean', 'delete-p99', 'processed-mean', 'processed-min', 'processed-p50', 'processed-p75', 'processed-p95',
                  'processed-p98', 'processed-p99', 'processed-p999', 'time']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    for log_folder in log_folders:
        containing_tree_counter = os.path.join(results_folder, log_folder, "containing-tree-counter.csv")
        edgecount_histogram = os.path.join(results_folder, log_folder, "edgecount-histogram.csv")
        processed_histogram = os.path.join(results_folder, log_folder, "processed-histogram.csv")
        result_counter = os.path.join(results_folder, log_folder, "result-counter.csv")
        tree_counter = os.path.join(results_folder, log_folder, "tree-counter.csv")
        treesize_histogram = os.path.join(results_folder, log_folder, "tree-size-histogram.csv")
        window_histogram = os.path.join(results_folder, log_folder, "window-histogram.csv")
        deletion_histogram = os.path.join(results_folder, log_folder, "explicit-deletion-histogram.csv")

        print "Opening {}".format(log_folder)

        with open(containing_tree_counter, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            containing_tree_mean = row[3]

        with open(result_counter, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            results = row[1]

        with open(tree_counter, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            tree_count = row[1]

        with open(treesize_histogram, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            tree_size_mean = row[3]
            tree_size_max = row[2]

        with open(window_histogram, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            window_mean = row[3]
            window_p99 = row[10]

        with open(edgecount_histogram, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            slide_count_mean = row[3]

        with open(processed_histogram, 'r') as f:
            row_list = list(csv.reader(f))
            row = reversed(row_list).next()
            processed_edge_count = row[1]
            processed_mean = row[3]
            processed_min = row[4]
            processed_p50 = row[6]
            processed_p75 = row[7]
            processed_p95 = row[8]
            processed_p98 = row[9]
            processed_p99 = row[10]
            processed_p999 = row[11]
            time = len(row_list)

        query = log_folder.split("-")[0]
        predicates = log_folder.split("-")[1]
        semantics = log_folder.split("-")[2]
        window_size = re.search('ws:(.*)-ss', log_folder).group(1)
        slide_size = re.search('ss:(.*)-tc', log_folder).group(1)

        if "-dr" in log_folder:
            delete_ratio = re.search('-dr:(.*)', log_folder).group(1)
            with open(deletion_histogram, 'r') as f:
                row = reversed(list(csv.reader(f))).next()
                delete_mean_mean = row[3]
                delete_p99_p99 = row[10]
            print delete_ratio
        else:
            delete_ratio = 0
            delete_mean = 0
            delete_p99 = 0

        writer.writerow({
            'query' : query,
            'semantics' : semantics,
            'binding' : predicates,
            'window-size' : window_size,
            'slide-size' : slide_size,
            'containing-tree-mean' : containing_tree_mean,
            'results' : results,
            'tree-count' : tree_count,
            'tree-size-mean' : tree_size_mean,
            'tree-size-max' : tree_size_max,
            'window-mean' : window_mean,
            'window-p99' : window_p99,
            'slide-count-mean' : slide_count_mean,
            'delete-ratio' : delete_ratio,
            'delete-mean' : delete_mean,
            'delete-p99' : delete_p99,
            'processed-edge-count' : processed_edge_count,
            'processed-mean' : processed_mean,
            'processed-min' : processed_min,
            'processed-p50' : processed_p50,
            'processed-p75' : processed_p75,
            'processed-p95' : processed_p95,
            'processed-p98' : processed_p98,
            'processed-p99' : processed_p99,
            'processed-p999' : processed_p999,
            'time' : time
        })

        print "Result aggregated for {}".format(log_folder)

print "Aggregated Results written into {}".format(aggregated_results_file)

