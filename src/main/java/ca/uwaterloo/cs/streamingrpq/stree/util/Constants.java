package ca.uwaterloo.cs.streamingrpq.stree.util;

public class Constants {
    // average number of neighbours per node
    public static final int EXPECTED_NEIGHBOURS = 12;

    // expected number of nodes in the productGraph
    public static final int EXPECTED_NODES = 50000000;

    // expected number of tree a single edge is in
    public static final int EXPECTED_TREES = 1000;

    // expected number of labels in a productGraph
    public static final int EXPECTED_LABELS = 8;

    /**
     * Total number of SpanningTreeExpansion jobs to be assigned to a thread
     */
    public static final int EXPECTED_BATCH_SIZE = 64;
}
