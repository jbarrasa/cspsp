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
                                                   @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                   @Name("to") Node to) {

        //First we try ro find the rel
        Iterable<Relationship> relationships = from.getRelationships(RelationshipType.withName(relType), Direction.OUTGOING);
        for (Relationship r:relationships){
            if(matchesProperties(r.getAllProperties(), props)){
                return Stream.of(new RelationshipResult(r));
            }
        }
        // If not present, then we create it (APOC create.relationship code)
        return Stream.of(new RelationshipResult(setProperties(from.createRelationshipTo(to,RelationshipType.withName(relType)),props)));
    }

    private boolean matchesProperties(Map<String, Object> allProperties, Map<String, Object> mergeProps) {
        for (String key:mergeProps.keySet()){
            if(!allProperties.get(key).equals(mergeProps.get(key))){
                return false;
            }
        }
        return true;
    }

    private <T extends PropertyContainer> T setProperties(T pc, Map<String, Object> p) {
        if (p == null) return pc;
        for (Map.Entry<String, Object> entry : p.entrySet()) pc.setProperty(entry.getKey(), toPropertyValue(entry.getValue()));
        return pc;
    }

    private Object toPropertyValue(Object value) {
        if (value instanceof Iterable) {
            Iterable it = (Iterable) value;
            Object first = Iterables.firstOrNull(it);
            if (first==null) return EMPTY_ARRAY;
            return Iterables.asArray(first.getClass(), it);
        }
        return value;
    }

    public class RelationshipResult {
        public final Relationship rel;

        public RelationshipResult(Relationship rel) {
            this.rel = rel;
        }
    }

}
