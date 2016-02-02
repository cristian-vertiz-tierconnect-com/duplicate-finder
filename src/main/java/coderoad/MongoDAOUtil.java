package coderoad;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;


import java.net.UnknownHostException;

public class MongoDAOUtil
{
    private static MongoDAOUtil instance = new MongoDAOUtil();

    public static MongoClient mongoClient;
    public static DB db;
    public static DBCollection thingsCollection;
    public static DBCollection thingTypesCollection;
    public static DBCollection thingSnapshotsCollection;
    public static DBCollection thingSnapshotIdsCollection;
    public static DBCollection thingBucketCollection;
    public static DBCollection timeseriesCollection;
    public static DBCollection timeseriesControlCollection;

    private static boolean enabled = true;

    public MongoDAOUtil()
    {
    }

    public static MongoDAOUtil getInstance(){
        if(instance == null)
            instance = new MongoDAOUtil();
        return instance;
    }

    public static void setupMongodb( String mongoHost, int mongoPort, String mongoDatabase, Integer connectTimeOut,  Integer connectionsPerHost ) throws UnknownHostException
    {

        MongoClientOptions options = MongoClientOptions.builder()
                .connectTimeout( connectTimeOut==null?3000:connectTimeOut )
                .connectionsPerHost(connectionsPerHost==null?200:connectionsPerHost)  //sets the connection timeout to 3 seconds
                //.autoConnectRetry( true )
                .build();
        mongoClient = new MongoClient( mongoHost + ":" + mongoPort, options);
        //mongoClient = new MongoClient( mongoHost, mongoPort, options);
        db = mongoClient.getDB( mongoDatabase );
        // mongoClient.setWriteConcern(WriteConcern.JOURNALED);
        //mongoClient.setWriteConcern( WriteConcern.UNACKNOWLEDGED );

        MongoDAOUtil.setEnabled( true );


        thingsCollection = db.getCollection( "things" );

        thingTypesCollection = db.getCollection( "thingTypes" );

        thingSnapshotsCollection = db.getCollection( "thingSnapshots" );
        thingSnapshotIdsCollection = db.getCollection( "thingSnapshotIds" );
        thingBucketCollection = db.getCollection( "thingBucket" );
        timeseriesCollection = db.getCollection( "timeseries" );
        timeseriesControlCollection = db.getCollection( "timeseriesControl" );

    }

    public static boolean isEnabled()
    {
        return enabled;
    }

    public static void setEnabled( boolean b )
    {
        enabled = b;
    }
}
