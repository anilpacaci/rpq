package ca.uwaterloo.cs.streamingrpq.stree.util;

public class Constants {
    // average number of neighbours per node
    public static final int EXPECTED_NEIGHBOURS = 12;

    // expected number of nodes in the productGraph
    public static final int EXPECTED_NODES = 50000000;

    // expected number of tree a single edge is in
    public static final int EXPECTED_TREES = 65536;

    // expected number of labels in a productGraph
    public static final int EXPECTED_LABELS = 8;

    /**
     * Total number of SpanningTreeExpansion jobs to be assigned to a thread
     */
    public static final int EXPECTED_BATCH_SIZE = 64;

    public static final int EXPECTED_TREE_SIZE = 8192;

    public static final int HISTOGRAM_BUCKET_SIZE = 526336;

    public static final char REVERSE_LABEL_SYMBOL = '^';

    public static final String EPSILON_TRANSITION = "EPSILON";
}
