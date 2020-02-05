package ca.uwaterloo.cs.streamingrpq.virtuoso;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;

public class VirtuosoTriple {

    private Triple triple;
    private Statement statement;

    private long timestamp;

    public VirtuosoTriple(InputTuple<Integer, Integer, String> inputTuple, Model model) {
        Integer subject = inputTuple.getSource();
        Integer object = inputTuple.getTarget();
        String predicate = inputTuple.getLabel();
        long timestamp = inputTuple.getTimestamp();

        triple = new Triple(NodeFactory.createURI(subject.toString()), NodeFactory.createURI(predicate), NodeFactory.createURI(object.toString()));

        statement = model.asStatement(triple);
    }

    public Triple getTriple() {
        return triple;
    }

    public Statement getStatement() {
        return statement;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
