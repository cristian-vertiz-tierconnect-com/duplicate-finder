package com.mojix;

import com.mojix.dao.CsvDAO;
import com.mojix.dao.DbDAO;
import com.mojix.driver.CassandraUtils;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.List;

public class DuplicateFinder {

    private static String[] car = {"|", "/", "-", "\\"};
    private static String database;
    private static String csvFile;
    private static String cassandraHost;
    private static String dbHost;

    public static void findDuplicates() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, IOException {

        //Init drivers
        CassandraUtils.init(cassandraHost);
        DbDAO.getInstance().initMysqlJDBCDrivers(dbHost, database);

        //Get values from databases
        Map<Long, Map<String, Long>> thingFieldMap = DbDAO.getInstance().getThingFieldMap(database);
        Map<Long, Map<String, Object>> thingList = DbDAO.getInstance().getThingList(database);
        List<Map<String, Object>> csvFileList = CsvDAO.getInstance().readScv(csvFile);

        //Loop things from mysql/mssql
        for (Map.Entry<Long, Map<String, Object>> thingEntry : thingList.entrySet()) {
            if (!csvContains(thingEntry.getValue().get("serial").toString(), csvFileList) &&
                    thingEntry.getValue().get("parent_id") == null) {
                deleteThing(thingEntry.getValue(), thingFieldMap.get(thingEntry.getKey()));
            } else {
                mergeThing(thingEntry.getValue(), thingFieldMap.get(thingEntry.getKey()), csvFileList);
            }
        }

//        StringBuilder sb = new StringBuilder();
//        sb.append("\n ");
//        sb.append("\n================================ Summary ================================");
//        sb.append("\n Total blinks (according to cassandra) -------------> " + totalBLinks);
//        sb.append("\n Total blinks (according to mongo) -----------------> " + totalBLinksMong);
//        sb.append("\n Things in mysql -----------------------------------> " + thingList.entrySet().size());
//        sb.append("\n Things in cassandra -------------------------------> " + thingTimeCount.entrySet().size());
//        sb.append("\n Things in mongo -----------------------------------> " + thingMongoList.size());
//        sb.append("\n Missing things in mysql but existing in mongo -----> " + missing);
//        sb.append("\n ");
//        sb.append("\n Total cassandra rows in field_value_history -------> " + countFVH);
//        sb.append("\n Total cassandra orphans in field_value_history ----> " + fvhOrphan);
//        sb.append("\n ");
//        sb.append("\n Total cassandra rows in field_value ---------------> " + countFV);
//        sb.append("\n Total cassandra orphans in field_value ------------> " + fvOrphan);
//        sb.append("\n=========================================================================");
//        sb.append("\n ");
//
//        System.out.print(sb);
//
//        String fileName = "results_" + (new SimpleDateFormat("YYYYMMddhhmmss").format(new Date())) + ".txt";
//        File file = new File(fileName);
//        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
//        try {
//            writer.write(sb.toString());
//            writer.write("\n\n");
//            writer.write(sbThingsMissing.toString());
//            writer.write("\n\n");
//            writer.write(sbBLinksPerThingCassandra.toString());
//        } finally {
//            if (writer != null) writer.close();
//            System.out.println("***Results have been written to file  " + fileName);
//        }

    }

    private static void mergeThing(Map<String, Object> value, Map<String, Long> thingFieldMap, List<Map<String, Object>> csvFileList) {

    }

    private static void deleteThing(Map<String, Object> thingMap, Map<String, Long> thingFieldMap) {

    }

    private static boolean csvContains(String serial, List<Map<String, Object>> csvFileList) {
        boolean result = false;
        for (Map<String, Object> item : csvFileList) {
            result = result || item.get("serial").equals(serial);
        }
        return result;
    }

    public static void closeConnections() {

    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.print("Usage java -jar duplicate-finder.jar <DB TYPE> <DB HOST> <CASSANDRA HOST> <CSV FILE PATH>");
            System.exit(0);
        }
        try {

            database = args[0];
            dbHost = args[1];
            cassandraHost = args[2];
            csvFile = args[3];
            findDuplicates();
            System.exit(0);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
