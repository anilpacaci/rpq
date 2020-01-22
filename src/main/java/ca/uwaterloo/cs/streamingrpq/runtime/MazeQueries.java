package ca.uwaterloo.cs.streamingrpq.runtime;

import ca.uwaterloo.cs.streamingrpq.stree.data.ManualQueryAutomata;

/**
 * Created by anilpacaci on 2019-10-13.
 */
public class MazeQueries {

    public static  <L> ManualQueryAutomata<L> getMazeQuery(String queryName, L... predicateString) {
        ManualQueryAutomata<L> query;

        switch (queryName) {
            case "robotic1" :
                checkArguments(queryName, 1, predicateString);
                query = new ManualQueryAutomata<>(2);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(1, predicateString[0], 1);
                query.addFinalState(0);
                query.addFinalState(1);
                break;
            case "robotic2":
                checkArguments(queryName, 2, predicateString);
                query = new ManualQueryAutomata<>(2);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(1, predicateString[1], 1);
                query.addFinalState(1);
                break;
            case "robotic3":
                checkArguments(queryName, 4, predicateString);
                query = new ManualQueryAutomata<>(4);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(1, predicateString[1], 1);
                query.addTransition(1, predicateString[2], 2);
                query.addTransition(2, predicateString[3], 3);
                query.addTransition(3, predicateString[3], 3);
                query.addFinalState(1);
                query.addFinalState(2);
                query.addFinalState(3);
                break;
            case "robotic4":
                checkArguments(queryName, 3, predicateString);
                query = new ManualQueryAutomata<>(2);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(0, predicateString[1], 1);
                query.addTransition(0, predicateString[2], 1);
                query.addTransition(1, predicateString[0], 1);
                query.addTransition(1, predicateString[1], 1);
                query.addTransition(1, predicateString[2], 1);
                query.addFinalState(0);
                query.addFinalState(1);
                break;
            case "robotic5":
                checkArguments(queryName, 3, predicateString);
                query = new ManualQueryAutomata<>(3);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(1, predicateString[1], 1);
                query.addTransition(1, predicateString[2], 2);
                query.addFinalState(2);
                break;
            case "robotic6":
                checkArguments(queryName, 3, predicateString);
                query = new ManualQueryAutomata<>(4);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(1, predicateString[0], 1);
                query.addTransition(1, predicateString[1], 2);
                query.addTransition(2, predicateString[2], 3);
                query.addTransition(3, predicateString[2], 3);
                query.addFinalState(1);
                query.addFinalState(3);
                break;
            case "robotic7":
                checkArguments(queryName, 3, predicateString);
                query = new ManualQueryAutomata<>(3);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(1, predicateString[1], 2);
                query.addTransition(2, predicateString[2], 2);
                query.addFinalState(2);
                break;
            case "robotic8":
                checkArguments(queryName, 2, predicateString);
                query = new ManualQueryAutomata<>(2);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(0, predicateString[1], 1);
                query.addTransition(1, predicateString[1], 1);
                query.addFinalState(1);
                break;
            case "robotic9":
                checkArguments(queryName, 3, predicateString);
                query = new ManualQueryAutomata<>(2);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(0, predicateString[1], 1);
                query.addTransition(0, predicateString[2], 1);
                query.addTransition(1, predicateString[0], 1);
                query.addTransition(1, predicateString[1], 1);
                query.addTransition(1, predicateString[2], 1);
                query.addFinalState(1);
                break;
            case "robotic10":
                checkArguments(queryName, 3, predicateString);
                query = new ManualQueryAutomata<>(2);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(0, predicateString[1], 1);
                query.addTransition(1, predicateString[2], 1);
                query.addFinalState(1);
                break;
            case "robotic11":
                checkArguments(queryName, 3, predicateString);
                query = new ManualQueryAutomata<>(4);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(1, predicateString[1], 2);
                query.addTransition(2, predicateString[2], 3);
                query.addFinalState(3);
                break;
            case "robotic12":
                checkArguments(queryName, 3, predicateString);
                query = new ManualQueryAutomata<>(2);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(0, predicateString[1], 1);
                query.addTransition(0, predicateString[2], 1);
                query.addFinalState(1);
                break;
            case "robotic13":
                checkArguments(queryName, 3, predicateString);
                query = new ManualQueryAutomata<>(4);
                query.addTransition(0, predicateString[0], 1);
                query.addTransition(1, predicateString[1], 2);
                query.addTransition(1, predicateString[2], 3);
                query.addTransition(2, predicateString[2], 3);
                query.addFinalState(1);
                query.addFinalState(2);
                query.addFinalState(3);
                break;
            default:
                throw new IllegalArgumentException(queryName + " is not a valid query name");
        }

        return query;
    }

    private static void checkArguments(String queryName, int requiredArguments, Object... arguments) throws IllegalArgumentException{
        if(arguments.length < requiredArguments) {
            throw new IllegalArgumentException("Query " + queryName + " requires " + requiredArguments + " predicates");
        }
    }
}
