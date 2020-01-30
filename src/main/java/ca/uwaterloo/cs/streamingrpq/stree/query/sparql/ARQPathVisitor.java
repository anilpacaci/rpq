package ca.uwaterloo.cs.streamingrpq.stree.query.sparql;

import ca.uwaterloo.cs.streamingrpq.stree.query.AutomataBuilder;
import ca.uwaterloo.cs.streamingrpq.stree.query.NFA;
import ca.uwaterloo.cs.streamingrpq.stree.query.NFAAutomataBuilder;
import ca.uwaterloo.cs.streamingrpq.stree.util.Constants;
import org.apache.jena.sparql.path.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

public class ARQPathVisitor<A> implements PathVisitor {

    private final Logger logger = LoggerFactory.getLogger(ARQPathVisitor.class);


    private AutomataBuilder<A, String> automataBuilder;
    private Stack<A> nfaStack;

    private int kleeneStarCount;
    private int alternationCount;
    private int predicateCount;

    public ARQPathVisitor(AutomataBuilder<A, String> automataBuilder) {
        this.automataBuilder = automataBuilder;
        this.nfaStack = new Stack<>();

        // query statistics
        this.kleeneStarCount = 0;
        this.alternationCount = 0;
        this.predicateCount = 0;
    }

    public A getAutomaton() {
        return nfaStack.pop();
    }

    /**
     *
     * @return the total number of Kleene stars in the path syntax tree traversed by this Visitor
     */
    public int getKleeneStarCount() {
        return kleeneStarCount;
    }

    /**
     *
     * @return the total number of alternation symbols in the path syntax tree traversed by this Visitor
     */
    public int getAlternationCount() {
        return alternationCount;
    }

    /**
     * It uses bag semantics and therefore duplicates are counted
     * @return the total number of predicates in the path syntax tree traversed by this Visitor.
     */
    public int getPredicateCount() {
        return predicateCount;
    }

    @Override
    public void visit(P_Link pathNode){
        String localName = pathNode.getNode().getLocalName();
        A nfa = automataBuilder.transition(localName);
        nfaStack.push(nfa);

        //increase the predicate count
        predicateCount++;
    }
    @Override
    public void visit(P_ReverseLink pathNode){
        // not implemented
        return;
    }

    @Override
    public void visit(P_NegPropSet pathNotOneOf){
        // not implemented
        return;
    }

    @Override
    public void visit(P_Inverse inversePath){
        // assume it is an inverse of single label
        if(! (inversePath.getSubPath() instanceof P_Link)) {
            logger.error("Inverse can only be used over single labels: " + inversePath.toString());
            return;
        }
        P_Link subPath = (P_Link) inversePath.getSubPath();
        String localName = subPath.getNode().getLocalName();


        A nfa = automataBuilder.transition(Constants.REVERSE_LABEL_SYMBOL + localName);
        nfaStack.push(nfa);

        // increase the predicate count
        predicateCount++;
    }

    @Override
    public void visit(P_Mod pathMod){
        createIfNotExists(pathMod);

        A nfa = nfaStack.pop();
        A resultNFA = automataBuilder.kleeneStar(nfa);
        nfaStack.push(resultNFA);

        // increase the Kleene Star count
        kleeneStarCount++;
    }

    @Override
    public void visit(P_FixedLength pFixedLength){
        createIfNotExists(pFixedLength);

        A nfa = nfaStack.pop();
        A resultNFA = automataBuilder.kleeneStar(nfa);
        nfaStack.push(resultNFA);

        // increase the Kleene Star count
        kleeneStarCount++;
    }
    @Override
    public void visit(P_Distinct pathDistinct){
        // not implemented
        return;
    }
    @Override
    public void visit(P_Multi pathMulti){
        // not implemented
        return;
    }
    @Override
    public void visit(P_Shortest pathShortest){
        // not implemented
        return;
    }
    @Override
    public void visit(P_ZeroOrOne path){
        createIfNotExists(path);

        A nfa = nfaStack.pop();
        A resultNFA = automataBuilder.kleeneStar(nfa);
        nfaStack.push(resultNFA);

        // increase the Kleene Star count
        kleeneStarCount++;
    }

    @Override
    public void visit(P_ZeroOrMore1 path){
        createIfNotExists(path);

        A nfa = nfaStack.pop();
        A resultNFA = automataBuilder.kleeneStar(nfa);
        nfaStack.push(resultNFA);

        // increase the Kleene Star count
        kleeneStarCount++;
    }
    @Override
    public void visit(P_ZeroOrMoreN path){
        createIfNotExists(path);

        A nfa = nfaStack.pop();
        A resultNFA = automataBuilder.kleeneStar(nfa);
        nfaStack.push(resultNFA);

        // increase the Kleene Star count
        kleeneStarCount++;
    }

    @Override
    public void visit(P_OneOrMore1 path){
        createIfNotExists(path);

        A nfa = nfaStack.pop();
        A resultNFA = automataBuilder.kleeneStar(nfa);
        nfaStack.push(resultNFA);

        // increase the Kleene Star count
        kleeneStarCount++;
    }
    @Override
    public void visit(P_OneOrMoreN path){
        createIfNotExists(path);

        A nfa = nfaStack.pop();
        A resultNFA = automataBuilder.kleeneStar(nfa);
        nfaStack.push(resultNFA);

        // increase the Kleene Star count
        kleeneStarCount++;
    }

    @Override
    public void visit(P_Alt pathAlt){
        // generate the child paths if not exists
        createIfNotExists(pathAlt);

        A rightNFA = nfaStack.pop();
        A leftNFA = nfaStack.pop();

        A resultNFA = automataBuilder.alternation(leftNFA, rightNFA);
        nfaStack.push(resultNFA);

        // increase the alternation count
        alternationCount++;
    }
    @Override
    public void visit(P_Seq pathSeq){
        // generate the child paths if not exists
        createIfNotExists(pathSeq);

        A rightNFA = nfaStack.pop();
        A leftNFA = nfaStack.pop();

        A resultNFA = automataBuilder.concenetation(leftNFA, rightNFA);
        nfaStack.push(resultNFA);

        return;
    }

    protected void createIfNotExists(P_Path1 path) {
        Path subPath = path.getSubPath();
        subPath.visit(this);
    }

    protected void createIfNotExists(P_Path2 path) {
        Path left = path.getLeft();
        left.visit(this);

        Path right = path.getRight();
        right.visit(this);
    }
}
