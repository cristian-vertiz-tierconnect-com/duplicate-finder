    package coderoad;

import com.datastax.driver.core.*;
import com.datastax.driver.core.PreparedStatement;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.List;

public class DuplicateFinder {

    private static String[] car = {"|", "/", "-", "\\"};
    private static String database;
    private static String mongoHost;
    private static String cassandraHost;
    private static String dbHost;

    public static void verify2() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, IOException {


        Connection conn = initMysqlJDBCDrivers();
        CassandraUtils.init(cassandraHost, "riot_main");
        MongoDAOUtil.setupMongodb(mongoHost, 27017, "riot_main", null, null);

        System.out.print("\n ");
        System.out.print("\n ");
        System.out.print("\n ");

        Long countFVH = 0L, totalBLinks = 0L, totalBLinksMong = 0L, fvhOrphan = 0L, countFV = 0L, fvOrphan = 0L, missing = 0L;

        Map<Long, Map<String, Long>> thingFieldMap = getThingFieldMap();
        Map<Long, String> thingList = getThingList();
        List<DBObject> thingMongoList = getMongoThingList();

 
        Map<Long, List<Long>> thingTimeCount = new HashMap<Long, List<Long>>();

        PreparedStatement selectFVH2 = CassandraUtils.getSession().prepare("SELECT field_id, at FROM field_value_history limit 1000000000");
        for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFVH2))) {

            Long field_id = row.getLong("field_id");
            Long thing_id = thingFieldMap.get(field_id).get("thingId");
            Long time = row.getDate("at").getTime();

            List<Long> temp = new ArrayList<Long>();
            if (thingTimeCount.containsKey(thing_id)) {
                temp = thingTimeCount.get(thing_id);
            }
            if (!temp.contains(time))
                temp.add(time);

            thingTimeCount.put(thing_id, temp);
            countFVH++;

            if (!thingFieldMap.containsKey(field_id))
                fvhOrphan += 1;

            if (countFVH % 10000 == 0) {
                System.out.print("\rAnalysing Cassandra field_value_history " + car[(int) (countFVH / 10000) % 4]);
            }
        }
        System.out.println("\rAnalysing Cassandra field_value_history [OK]");

        //field_value
        PreparedStatement selectFV = CassandraUtils.getSession().prepare("SELECT field_id FROM field_value limit 1000000000");
        for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFV))) {

            Long field_id = row.getLong("field_id");

            if (!thingFieldMap.containsKey(field_id))
                fvOrphan += 1;

            countFV++;
            if (countFV % 10000 == 0) {
                System.out.print("\rAnalysing Cassandra field_value " + car[(int)(countFV / 10000) % 4]);
            }
        }

        System.out.println("\rAnalysing Cassandra field_value [OK]");


        StringBuilder sbBLinksPerThingCassandra = new StringBuilder();
        sbBLinksPerThingCassandra.append("Blink count per thing in cassandra\nthing_id | serial | total_blinks\n");
        for (Map.Entry<Long, List<Long>> entry : thingTimeCount.entrySet()) {
            totalBLinks += entry.getValue().size();
//            System.out.println(entry.getKey() + " => " + entry.getValue().size());
            sbBLinksPerThingCassandra.append(entry.getKey() + " | " + thingList.get(entry.getKey()) + " | " + entry.getValue().size() + "\n");
            if (totalBLinks % 10000 == 0) {
                System.out.print("\rAnalysing Cassandra against MySQL " + car[(int)(totalBLinks / 10000) % 4]);
            }
        }
        System.out.println("\rAnalysing Cassandra against MySQL [OK]");

        StringBuilder sbThingsMissing = new StringBuilder();
        sbThingsMissing.append("Missing things in mysql but existing in mongo\nthing_id | serial\n");
        for (DBObject doc : thingMongoList){
            if(!thingList.keySet().contains(Long.parseLong(doc.get("_id").toString()))){
                missing++;
                sbThingsMissing.append(doc.get("_id") + " | " + thingList.get(Long.parseLong(doc.get("_id").toString())) + "\n");
            }
            totalBLinksMong += Long.parseLong(doc.get("blinks_count").toString());
            if (missing % 10000 == 0) {
                System.out.print("\rAnalysing Mongo against MySQL " + car[(int)(missing / 10000) % 4]);
            }
        }
        System.out.println("\rAnalysing Mongo against MySQL [OK]");


        StringBuilder sb = new StringBuilder();
        sb.append("\n ");
        sb.append("\n================================ Summary ================================");
        sb.append("\n Total blinks (according to cassandra) -------------> " + totalBLinks);
        sb.append("\n Total blinks (according to mongo) -----------------> " + totalBLinksMong);
        sb.append("\n Things in mysql -----------------------------------> " + thingList.entrySet().size());
        sb.append("\n Things in cassandra -------------------------------> " + thingTimeCount.entrySet().size());
        sb.append("\n Things in mongo -----------------------------------> " + thingMongoList.size());
        sb.append("\n Missing things in mysql but existing in mongo -----> " + missing);
        sb.append("\n ");
        sb.append("\n Total cassandra rows in field_value_history -------> " + countFVH);
        sb.append("\n Total cassandra orphans in field_value_history ----> " + fvhOrphan);
        sb.append("\n ");
        sb.append("\n Total cassandra rows in field_value ---------------> " + countFV);
        sb.append("\n Total cassandra orphans in field_value ------------> " + fvOrphan);
        sb.append("\n=========================================================================");
        sb.append("\n ");

        System.out.print(sb);

        String fileName = "results_" + (new SimpleDateFormat("YYYYMMddhhmmss").format(new Date())) + ".txt";
        File file = new File(fileName);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        try {
            writer.write(sb.toString());
            writer.write("\n\n");
            writer.write(sbThingsMissing.toString());
            writer.write("\n\n");
            writer.write(sbBLinksPerThingCassandra.toString());
        } finally {
            if (writer != null) writer.close();
            System.out.println("***Results have been written to file  " + fileName);
        }

    }


    public static void verify() throws Exception {
        StringBuilder sb = new StringBuilder();

        Map<Long, List<Long>> thingTimeCount = new HashMap<Long, List<Long>>();
        int countFVH2 = 0, countFVH = 0, countFV = 0, countFT = 0, fvh2Orphan = 0, fvOrphan = 0;

        CassandraUtils.init("127.0.0.1", "riot_main");
        System.out.println("\n\n\n");

//        Map<Long, Long> thingFieldMap = getThingFieldMap();
        Map thingFieldMap = getThingFieldMap();

        //field_value_history2
        PreparedStatement selectFVH2 = CassandraUtils.getSession().prepare("SELECT thing_id, time FROM field_value_history2 limit 1000000000");
        for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFVH2))) {

            Long thing_id = row.getLong("thing_id");
            Long time = row.getDate("time").getTime();

            List<Long> temp = new ArrayList<Long>();
            if (thingTimeCount.containsKey(thing_id)) {
                temp = thingTimeCount.get(thing_id);
            }
            if (!temp.contains(time))
                temp.add(time);

            thingTimeCount.put(thing_id, temp);
            countFVH2++;

            if (countFVH2 % 10000 == 0) {
                System.out.print("\rAnalysing cassandra field_value_history2 " + car[(countFVH2 / 10000) % 4]);
            }

        }

        System.out.println("\rAnalysing Cassandra field_value_history2  [OK]");

        //field_value_history
        PreparedStatement selectFVH = CassandraUtils.getSession().prepare("SELECT field_id FROM field_value_history limit 1000000000");
        for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFVH))) {
            if (!thingFieldMap.containsKey(row.getLong("field_id")))
                fvh2Orphan += 1;
            countFVH++;
            if (countFVH % 10000 == 0) {
                System.out.print("\rAnalysing cassandra field_value_history " + car[(countFVH / 10000) % 4]);
            }
        }

        System.out.println("\rAnalysing cassandra field_value_history [OK]");


        //field_type
        PreparedStatement selectFT = CassandraUtils.getSession().prepare("SELECT field_type_id FROM field_type limit 1000000000");
//        Row rowFT = CassandraUtils.getSession().execute(new BoundStatement(selectFT)).all().get(0);

        for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFT))) {
            countFT++;
            if (countFT % 10000 == 0) {
                System.out.print("\rAnalysing cassandra field_type " + car[(countFT / 10000) % 4]);
            }
        }
//        countFT = Integer.parseInt(rowFT.getLong("count")+"");
        System.out.println("\rAnalysing cassandra field_type [OK]");


        //field_value
        PreparedStatement selectFV = CassandraUtils.getSession().prepare("SELECT field_id FROM field_value limit 1000000000");
        for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFV))) {
            if (!thingFieldMap.containsKey(row.getLong("field_id")))
                fvOrphan += 1;
            countFV++;
            if (countFV % 10000 == 0) {
                System.out.print("\rAnalysing cassandra field_value " + car[(countFV / 10000) % 4]);
            }
        }

        System.out.println("\rAnalysing cassandra field_value [OK]");


//        System.out.println("thing_id => blinks_count");
        sb.append("thing_id => blinks_count");

//        System.out.println("Cassandra field_value_history2 results :");
        sb.append("\nCassandra field_value_history2 results :");

        Long totalBLinks = 0L;
        for (Map.Entry<Long, List<Long>> entry : thingTimeCount.entrySet()) {
            totalBLinks += entry.getValue().size();
//            System.out.println(entry.getKey() + " => " + entry.getValue().size());
            sb.append("\n" + entry.getKey() + " => " + entry.getValue().size());
        }

        System.out.println("================================ Summary ================================");
        System.out.println("   Total blinks --------------------------------------> " + totalBLinks);
        System.out.println("   Things --------------------------------------------> " + thingTimeCount.entrySet().size());
        System.out.println("(1)Total cassandra rows in field_value_history2 ------> " + countFVH2);
        System.out.println("(2)Total cassandra orphans in field_value_history2 ---> " + fvh2Orphan);
        System.out.println("(3)Total cassandra rows in field_value_history -------> " + countFVH);
        System.out.print("\n");
        System.out.println("(4)Total cassandra rows in field_type ----------------> " + countFT);
        System.out.println("(5)Total cassandra orphans in field_value ------------> " + fvOrphan);
        System.out.println("(6)Total cassandra rows in field_value ---------------> " + countFV);
        System.out.println("\n***NOTE : Make sure (1)+(2)=(3) and (4)+(5)=(6)");

        sb.append("\n================================ Summary ================================");
        sb.append("\n   Total blinks --------------------------------------> " + totalBLinks);
        sb.append("\n   Things --------------------------------------------> " + thingTimeCount.entrySet().size());
        sb.append("\n(1)Total cassandra rows in field_value_history2 ------> " + countFVH2);
        sb.append("\n(2)Total cassandra orphans in field_value_history2 ---> " + fvh2Orphan);
        sb.append("\n(3)Total cassandra rows in field_value_history -------> " + countFVH);
        sb.append("\n");
        sb.append("\n(4)Total cassandra rows in field_type ----------------> " + countFT);
        sb.append("\n(5)Total cassandra orphans in field_value ------------> " + fvOrphan);
        sb.append("\n(6)Total cassandra rows in field_value ---------------> " + countFV);
        sb.append("\n\n***NOTE : Make sure (1)+(2)=(3) and (4)+(5)=(6)");

        String fileName = "results_" + (new SimpleDateFormat("YYYYMMddhhmmss").format(new Date())) + ".txt";
        File file = new File(fileName);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        try {
            writer.write(sb.toString());
        } finally {
            if (writer != null) writer.close();
            System.out.println("***Results have been written to file  " + fileName);
        }
    }

    public static Map<Long, Map<String, Long>> getThingFieldMap() throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        Connection conn = initMysqlJDBCDrivers();

        java.sql.ResultSet rs = null;
        if (conn != null && database.equals("mysql")) {
            rs = conn.createStatement().executeQuery("SELECT id, thing_id, thingTypeFieldId FROM thingfield");
        } else if (conn != null && database.equals("mssql")) {
            rs = conn.createStatement().executeQuery("SELECT id, thing_id, thingTypeFieldId FROM dbo.thingfield");
        }
        Map<Long, Map<String, Long>> thingFieldMap2 = new HashMap<Long, Map<String, Long>>();

        if (rs != null) {
            int counter = 0;
            while (rs.next()) {

                Long thingTypeFieldId = rs.getLong("thingTypeFieldId");
                Long thingId = rs.getLong("thing_id");
                Long thingFieldId = rs.getLong("id");

                Map<String, Long> data = new HashMap<String, Long>();

                data.put("thingId", thingId);
                data.put("thingTypeFieldId", thingTypeFieldId);

                thingFieldMap2.put(thingFieldId, data);
                counter++;
                if (counter % 10000 == 0) {
                    System.out.print("\rRetrieving data from thingFields " + car[(counter / 10000) % 4]);
                }
            }
            System.out.println("\rRetrieving data from thingFields [OK]");
            conn.close();
        } else {
            System.out.println("No connection available for " + System.getProperty("connection.url." + database));
        }


        return thingFieldMap2;
    }


    public static Map<Long, String> getThingList() throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        Connection conn = initMysqlJDBCDrivers();

        java.sql.ResultSet rs = null;
        if (conn != null && database.equals("mysql")) {
            rs = conn.createStatement().executeQuery("SELECT id, serial FROM apc_thing");
        } else if (conn != null && database.equals("mssql")) {
            rs = conn.createStatement().executeQuery("SELECT id, serial FROM dbo.apc_thing");
        }

        Map<Long, String> thingList = new HashMap<Long, String>();

        if (rs != null) {
            int counter = 0;
            while (rs.next()) {

                Long thingId = rs.getLong("id");
                String serial = rs.getString("serial");

                thingList.put(thingId, serial);

                counter++;
                if (counter % 10000 == 0) {
                    System.out.print("\rRetrieving data from apc_thing  " + car[(counter / 10000) % 4]);
                }
            }
            System.out.println("\rRetrieving data from apc_thing [OK]");
            conn.close();
        } else {
            System.out.println("No connection available for " + System.getProperty("connection.url." + database));
        }


        return thingList;
    }

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


        return Lists.newArrayList(MongoDAOUtil.thingSnapshotIdsCollection.aggregate(
                groupPileline
        ).results());

    }

//    public static Map<Long, Long> getThingFieldMap() throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {
//        Connection conn = initMysqlJDBCDrivers();
//
//        java.sql.ResultSet rs = null;
//        if(conn != null && database.equals("mysql")){
//            rs = conn.createStatement().executeQuery("SELECT id, thing_id, thingTypeFieldId FROM thingfield");
//        }else if (conn != null && database.equals("mssql")){
//            rs = conn.createStatement().executeQuery("SELECT id, thing_id, thingTypeFieldId FROM dbo.thingfield");
//        }
//        Map<Long, Long> thingFieldMap2 = new HashMap<Long, Long>();
//
//        if(rs != null) {
//            int counter  = 0 ;
//            while (rs.next()) {
//
//                Long thingId = rs.getLong("thing_id");
//                Long thingFieldId = rs.getLong("id");
//
//                thingFieldMap2.put(thingFieldId, thingId);
//                counter++;
//                if(counter % 10000 == 0){
//                    System.out.print("\rRetrieving data from thingFields " + car[(counter/10000)%4]);
//                }
//            }
//            System.out.println("\rRetrieving data from thingFields [OK]");
//            conn.close();
//        }else{
//            System.out.println("No connection available for " + System.getProperty("connection.url." + database));
//        }
//
//
//        return thingFieldMap2;
//    }

    public static Connection initMysqlJDBCDrivers() throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {

        String url = System.getProperty("connection.url." + database);
        String userName = System.getProperty("connection.username." + database);
        String password = System.getProperty("connection.password." + database);

        String driverMysql = "org.gjt.mm.mysql.Driver";
        String driverMssql = "net.sourceforge.jtds.jdbc.Driver";

        Class.forName(driverMysql).newInstance();
        Class.forName(driverMssql).newInstance();
        return DriverManager.getConnection(url, userName, password);
    }

    public static void setDBPrperties() {
        System.getProperties().put("connection.url.mysql", "jdbc:mysql://" + dbHost + ":3306/riot_main");
        System.getProperties().put("connection.username.mysql", "root");
        System.getProperties().put("connection.password.mysql", "control123!");
        System.getProperties().put("connection.url.mssql", "jdbc:jtds:sqlserver://" + dbHost + ":1433/riot_main");
        System.getProperties().put("connection.username.mssql", "sa");
        System.getProperties().put("connection.password.mssql", "control123!");

    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.print("Usage java -jar cassandraVerifier.jar <DB TYPE> <DB HOST> <MONGO HOST> <CASSANDRA HOST>");
            System.exit(0);
        }
        try {

            database = args[0];
            dbHost = args[1];
            mongoHost = args[2];
            cassandraHost = args[3];
            setDBPrperties();
            verify2();
            System.exit(0);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
