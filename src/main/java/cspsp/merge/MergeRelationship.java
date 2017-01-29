package cspsp.merge;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;


/**
 * Created by jbarrasa on 29/01/2017.
 */
public class MergeRelationship {

    public static final String[] EMPTY_ARRAY = new String[0];

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;


    @Procedure( name = "cspsp.merge.relationship", mode = Mode.WRITE)
    public Stream<RelationshipResult> relationship(@Name("from") Node from,
                                                   @Name("relType") String relType, @Name("props") String props,
                                                   @Name("to") Node to) {

        String cypher = " MATCH (from), (to)" +
                        " WHERE ID(from) = " + from.getId() + " AND ID(to)= " + to.getId() +
                        " MERGE (from)-[r:" + relType +  " " + props + " ]-(to) " +
                        " RETURN r ";
        Result result = db.execute(cypher);
        return Stream.of(new RelationshipResult((Relationship) result.next().get("r")));
    }

    public class RelationshipResult {
        public final Relationship rel;

        public RelationshipResult(Relationship rel) {
            this.rel = rel;
        }
    }

}
