package ca.uwaterloo.cs.streamingrpq.stree.query.sparql;

import ca.uwaterloo.cs.streamingrpq.stree.query.AutomataBuilder;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.path.Path;
import org.apache.jena.sparql.path.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.sampled.Line;
import java.util.Stack;

public class LinearARQOpVisitor<A> extends OpVisitorBase {

    private final Logger logger = LoggerFactory.getLogger(LinearARQOpVisitor.class);


    Stack<A> nfaStack;
    private AutomataBuilder<A, String> automataBuilder;

    private int conjunctCount;
    private int alternationCount;
    private int kleeneStarCount;
    private int predicateCount;

    public LinearARQOpVisitor(AutomataBuilder<A, String> automataBuilder) {
        nfaStack = new Stack<>();
        this.automataBuilder = automataBuilder;

        //initialize counter on query features
        conjunctCount = 0;
        alternationCount = 0;
        kleeneStarCount = 0;
        predicateCount = 0;
    }

    /**
     * The total number of conjuncts in this query
     * @return
     */
    public int getConjunctCount() {
        return conjunctCount;
    }

    /**
     * The total number of alternation symbols in the path expression
     * @return
     */
    public int getAlternationCount() {
        return alternationCount;
    }

    /**
     * The total number of Kleene star symbols in the path expression
     * @return
     */
    public int getKleeneStarCount() {
        return kleeneStarCount;
    }

    /**
     * The total number of predicates in the path expression
     * @return
     */
    public int getPredicateCount() {
        return predicateCount;
    }

    public A getAutomaton() {
        return nfaStack.pop();
    }

    @Override
    public void visit(OpGroup opGroup) {
        return;
    }

    @Override
    public void visit(OpPath opPath) {
        ARQPathVisitor<A> visitor = new ARQPathVisitor(this.automataBuilder);
        Path path = opPath.getTriplePath().getPath();
        path.visit(visitor);

        A nfa = visitor.getAutomaton();

        nfaStack.push(nfa);

        conjunctCount++;
        alternationCount += visitor.getAlternationCount();
        kleeneStarCount += visitor.getKleeneStarCount();
        predicateCount += visitor.getPredicateCount();
    }

    @Override
    public void visit(OpSequence opSequence)  {
        // this is a linear chain processor so assumption is that all conjuncts form a linear chain.
        // thus we can simply concataneta all conjuncts
        while(nfaStack.size() > 1) {
            A rightNFA = nfaStack.pop();
            A leftNFA = nfaStack.pop();

            A resultNFA = automataBuilder.concenetation(leftNFA, rightNFA);
            nfaStack.push(resultNFA);
        }
    }

    @Override
    public void visit(OpBGP opBGP) {
        if(opBGP.getPattern().size() > 1) {
            logger.error("This visitor can only parse single triple patterns as fixed size path");
        }

        Triple triple = opBGP.getPattern().get(0);
        Path triplePath = PathFactory.pathLink(triple.getPredicate());

        ARQPathVisitor<A> visitor = new ARQPathVisitor(this.automataBuilder);
        triplePath.visit(visitor);

        A nfa = visitor.getAutomaton();

        nfaStack.push(nfa);

        conjunctCount++;
        alternationCount += visitor.getAlternationCount();
        kleeneStarCount += visitor.getKleeneStarCount();
        predicateCount += visitor.getPredicateCount();;
    }
}
