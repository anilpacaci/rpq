package ca.uwaterloo.cs.streamingrpq.stree.query;

import ca.uwaterloo.cs.streamingrpq.stree.query.sparql.LinearARQOpVisitor;
import com.google.common.collect.Maps;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicAutomata;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;

import java.nio.file.Path;
import java.util.Map;

/**
 * Created by anilpacaci on 2020-01-21.
 *
 * It constructs a Brics Automaton using the operations of Thompson's construction algorithm.
 * As Brics Automaton uses char for transitions
 */
public class BricsAutomataBuilder implements AutomataBuilder<Automaton, String> {

    private Map<String, Character> labelMappings;
    private Character nextChar;

    public BricsAutomataBuilder() {
        this.labelMappings = Maps.newHashMap();
        this.nextChar = Character.MIN_VALUE;
    }

    /**
     * Constructs a minimal automaton from a given SPARQL query string with linear property paths
     * @param queryString
     * @return
     */
    public BricsAutomata fromSPARQL(String queryString) {
        // parse the query and generate Algebra representation
        Query query = QueryFactory.create(queryString, Syntax.syntaxARQ);
        Op algebra = Algebra.compile(query);

        // walk over query tree to construct an Automaton
        LinearARQOpVisitor<Automaton> visitor = new LinearARQOpVisitor<>(this);
        OpWalker.walk(algebra, visitor);

        // generate Automaton that is recognized by the RPQEngine
        Automaton nfa = visitor.getAutomaton();
        BricsAutomata automata = new BricsAutomata(nfa, this.getLabelMappings());

        // set query statistics
        automata.setAlternationCount(visitor.getAlternationCount());
        automata.setKleeneStarCount(visitor.getKleeneStarCount());
        automata.setPredicateCount(visitor.getPredicateCount());

        automata.finalize();
        return  automata;
    }

    @Override
    public Automaton transition(String label) {
        // obtain corresponding character mapping for the label
        Character charMapping = getLabelMapping(label);

        Automaton resultAutomaton = BasicAutomata.makeChar(charMapping);
        return resultAutomaton;
    }

    @Override
    public Automaton kleeneStar(Automaton nfa) {
        Automaton resultAutomaton = nfa.repeat();
        return resultAutomaton;
    }

    @Override
    public Automaton concenetation(Automaton first, Automaton second) {
        Automaton resultAutomaton = first.concatenate(second);
        return resultAutomaton;
    }

    @Override
    public Automaton alternation(Automaton first, Automaton second) {
        Automaton resultAutomaton = first.union(second);
        return resultAutomaton;
    }

    /**
     * Immutable String to character mapping as Brics Automata only supports character transitions
     * @return
     */
    public final Map<String, Character> getLabelMappings() {
        return labelMappings;
    }

    private Character getLabelMapping(String label) {
        if(labelMappings.containsKey(label)) {
            return  labelMappings.get(label);
        } else {
            Character character = nextChar++;
            labelMappings.put(label, character);
            return character;
        }
    }

}
