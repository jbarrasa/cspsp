package cspsp.merge;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by jbarrasa on 29/01/2017.
 */
public class MergeRelationshipTest {

    // This rule starts a Neo4j instance
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            // This is the Procedure we want to test
            .withProcedure( MergeRelationship.class );

    @Test
    public void shouldAllowIndexingAndFindingANode() throws Throwable
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {

            // Given I've started Neo4j with the FullTextIndex procedure class
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // And given I have a node in the database
            long nodeId = session.run( " CREATE (p1:User {name:'Brookreson1'}) " +
                                       " CREATE (p2:User {name:'Brookreson2'}) " +
                                       " WITH p1, p2 CALL cspsp.merge.relationship(p1, 'MYREL', '{a:1,b:\"valforb\"}', p2) YIELD rel " +
                                       " RETURN id(rel) " )
                    .single()
                    .get( 0 ).asLong();

            // When I use the index procedure to index a node
            session.run( "CALL example.index({id}, ['name'])", parameters( "id", nodeId ) );

            // Then I can search for that node with lucene query syntax
            StatementResult result = session.run( "CALL example.search('User', 'name:Brook*')" );
            //assertThat( result.single().get( "nodeId" ).asLong(), equalTo( nodeId ) );
        }
    }
}


/*
CREATE (uno:Node)
CREATE (dos:Node)
WITH uno, dos
CALL cspsp.merge.relationship(uno, 'MYREL' ,"{a:123, b:'4565'}",  dos) YIELD rel AS r1
CALL cspsp.merge.relationship(uno, 'MYREL' ,"{a:123, b:'4565'}",  dos) YIELD rel AS r2
CALL cspsp.merge.relationship(uno, 'MYREL' ,"{a:123, b:'4565'}",  dos) YIELD rel AS r3
RETURN *
 */