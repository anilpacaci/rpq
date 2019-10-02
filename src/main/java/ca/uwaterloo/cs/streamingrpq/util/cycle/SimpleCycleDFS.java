package ca.uwaterloo.cs.streamingrpq.util.cycle;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SimpleCycleDFS<N, L> {

    private static Logger logger = LoggerFactory.getLogger(SimpleCycleDFS.class);

    private Graph<N,L> graph;
    private List<List<L>> cycles;


    public SimpleCycleDFS(Graph<N,L> graph)  {
        this.graph = graph;
        this.cycles = new ArrayList<>();
    }


    public void findCycles() {
        HashSet<N> visited = new HashSet<>();
        List<N> path = new ArrayList<>();

        Collection<N> vertices = graph.getVertices();

        for(N startVertex : vertices) {
            visit(startVertex, visited, path);
        }
    }

    private void visit(N vertex, HashSet<N> visited, List<N> path) {
        if(path.contains(vertex)) {
            // cycle is detected, report it
            addCycle(vertex, path);
        }

        if(visited.contains(vertex)) {
            // vertex has been visited before, so simply continue with the next element
            return;
        }

        visited.add(vertex);
        path.add(vertex);

        Collection<Edge<N, L>> children = graph.getChildren(vertex);
        for(Edge<N, L> child : children) {
            visit(child.getTarget(), visited, path);
        }

        path.remove(vertex);

    }

    private void addCycle(N vertex, List<N> path) {
        int firstIndex = path.indexOf(vertex);
        List<L> cycleLabels = new ArrayList<>();
        if(firstIndex < 0) {
            logger.error("Element {} is not in the path, no cycle formed", vertex);
            return;
        }

        ListIterator<N> iterator = path.listIterator(firstIndex);
        N source = iterator.next();
        N firstVertex = source;
        while(iterator.hasNext()) {
            N target = iterator.next();
            L label = graph.getLabel(source, target);
            cycleLabels.add(label);
            source = target;
        }

        L lastLabel = graph.getLabel(source, firstVertex);
        cycleLabels.add(lastLabel);

        cycles.add(cycleLabels);
    }

    public List<List<L>> getCycles() {
        return cycles;
    }

    public static void main(String[] argv) {
        String inputFilePath = argv[0];
        String index = argv[1];
        FilteredSimpleTextStream stream = new FilteredSimpleTextStream(Arrays.copyOfRange(argv, 2, argv.length));
        stream.open(inputFilePath);

        Graph<Integer, String> graph = new Graph<>();
        SimpleCycleDFS<Integer, String> cycleDFS = new SimpleCycleDFS<>(graph);

        InputTuple<Integer, Integer, String> input = stream.next();


        while(input != null) {
            graph.addEdge(input.getSource(), input.getTarget(), input.getLabel());
            input = stream.next();
        }
        logger.info("Dataset loaded for predicate set {}", index);
        logger.info("Number of vertices is {}", graph.getVertexCount());
        cycleDFS.findCycles();

        logger.info("Cycles for index {}:", index);
        List<List<String>> result = cycleDFS.getCycles();

        for(List<String> cycle : result) {
            logger.info(cycle.toString());
        }
        logger.info("All cycles for {} completed !!!", index);
    }
}
