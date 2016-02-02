package coderoad;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cvertiz on 2/2/16.
 */
public class MongoDAO {

    public static List<DBObject> getMongoThingList() {
//
//        db.getCollection('thingSnapshotIds').aggregate(
//                [
//                {
//                        $group: {
//            _id: "$_id",
//                    blinks_count:  {$first: {$size: "$blinks" }}
//        }
//        }
//        ]
//        )

        List<DBObject> groupPileline = new ArrayList<DBObject>();

        BasicDBObject aggregation = new BasicDBObject("_id", "$_id").append("blinks_count", new BasicDBObject("$first", new BasicDBObject("$size", "$blinks")));
        BasicDBObject group = new BasicDBObject("$group", aggregation);

        groupPileline.add(group);


        return Lists.newArrayList(MongoUtils.thingSnapshotIdsCollection.aggregate(
                groupPileline
        ).results());

    }
}
