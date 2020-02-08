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
    fieldnames = ['query', 'semantics', 'binding', 'window-size', 'slide-size', 'results',
                  'window-mean', 'window-p99','delete-ratio' ,
                  'insert-mean', 'insert-p99', 'parse-mean', 'parse-p99'
                  'processed-mean', 'processed-min', 'processed-p50', 'processed-p75', 'processed-p95',
                  'processed-p98', 'processed-p99', 'processed-p999', 'time']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    for log_folder in log_folders:
        result_counter = os.path.join(results_folder, log_folder, "result-counter.csv")
        window_histogram = os.path.join(results_folder, log_folder, "window-histogram.csv")
        insert_histogram = os.path.join(results_folder, log_folder, "insert-histogram.csv")
        query_execution = os.path.join(results_folder, log_folder, "query-execution.csv")
        result_parsing = os.path.join(results_folder, log_folder, "result_parsing.csv")

        print "Opening {}".format(log_folder)

        with open(result_counter, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            results = row[1]

        with open(window_histogram, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            window_mean = row[3]
            window_p99 = row[10]

        with open(insert_histogram, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            insert_mean = row[3]
            insert_p99 = row[10]

        with open(result_parsing, 'r') as f:
            row = reversed(list(csv.reader(f))).next()
            parse_mean = row[3]
            parse_p99 = row[10]

        with open(query_execution, 'r') as f:
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

        delete_ratio = 0
        delete_mean = 0
        delete_p99 = 0

        if "-dr" in log_folder:
            delete_ratio = re.search('-dr:(.*)', log_folder).group(1)

        writer.writerow({
            'query' : query,
            'semantics' : semantics,
            'binding' : predicates,
            'window-size' : window_size,
            'slide-size' : slide_size,
            'results' : results,
            'window-mean' : window_mean,
            'window-p99' : window_p99,
            'delete-ratio' : delete_ratio,
            'insert-mean' : insert_mean,
            'insert-p99' : insert_p99,
            'parse-mean' : parse_mean,
            'parse-p99' : parse_p99,
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

