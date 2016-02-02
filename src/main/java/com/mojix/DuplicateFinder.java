package com.mojix;

import com.mojix.cache.ArgsCache;
import com.mojix.dao.CsvDAO;
import com.mojix.dao.DbDAO;
import com.mojix.driver.CassandraUtils;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.List;

public class DuplicateFinder {

    private static String[] car = {"|", "/", "-", "\\"};

    public static void findDuplicates()
            throws ClassNotFoundException,
            SQLException,
            InstantiationException,
            IllegalAccessException,
            IOException {

        //Get values from databases
        Map<Long, Map<String, Long>> thingFieldMap = DbDAO.getInstance().getThingFieldMap(ArgsCache.database);
        Map<Long, Map<String, Object>> thingList = DbDAO.getInstance().getThingList(ArgsCache.database);
        List<Map<String, Object>> csvFileList = CsvDAO.getInstance().readScv(ArgsCache.csvFile);

        //Loop things from mysql/mssql
        //If thing is nor in csv file and thing has no child "delete" else "merge"
        for (Map.Entry<Long, Map<String, Object>> thingEntry : thingList.entrySet()) {
            boolean contains = csvContains(thingEntry.getValue().get("serial").toString(), csvFileList);
            boolean isParent = isParent(thingEntry.getValue(), thingList);

            if (!contains && !isParent) {
                deleteThing(thingEntry.getKey(), thingEntry.getValue(), thingFieldMap.get(thingEntry.getKey()));
            } else if (!contains) {
                mergeThing(thingEntry.getKey(), thingEntry.getValue(), thingFieldMap.get(thingEntry.getKey()), csvFileList);
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

    private static void mergeThing(Long thingId,
                                   Map<String, Object> value,
                                   Map<String, Long> thingFieldMap,
                                   List<Map<String, Object>> csvFileList) {

    }

    private static void deleteThing(Long thingId,
                                    Map<String, Object> thingMap,
                                    Map<String, Long> thingFieldMap) throws SQLException {

        //int dbDelete = DbDAO.getInstance().deleteThing(thingId, database);

    }

    private static boolean csvContains(String serial,
                                       List<Map<String, Object>> csvFileList) {
        boolean result = false;
        for (Map<String, Object> item : csvFileList) {
            result = result || item.get("serial").equals(serial);
        }
        return result;
    }

    private static boolean isParent(Map<String, Object> thingEntryValue,
                                    Map<Long, Map<String, Object>> thingList) {
        boolean result = false;

        for (Map.Entry<Long, Map<String, Object>> entry : thingList.entrySet()) {
            result = result || entry.getValue().get("serial").equals(thingEntryValue.get("serial"));
        }

        return result;
    }


    public static void openConnections() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        //Init drivers
        CassandraUtils.init();
        DbDAO.getInstance().initMysqlJDBCDrivers();
    }

    public static void closeConnections() {
        try {
            CassandraUtils.shutdown();
            DbDAO.getInstance().closeConnection();
        } catch (SQLException e) {
            System.out.println("Cannot close connections");
        }
    }


    public static void printMenu() {

        System.out.println("********MENU********");
        System.out.println("1) Find duplicates");
        //System.out.println("");
        System.out.println("x) Exit");
        System.out.println("********************");

    }

    public static void mainMenu(String[] args) {
        try {
            openConnections();
            String con;
            do {
                printMenu();
                con = Console.read();
                switch (con) {
                    case "1":
                        System.out.println("Finding duplicates...");
                        findDuplicates();
                        break;
                    case "x":
                        System.out.println("Bye!");
                        break;
                    default:
                        System.out.println("Option invalid!");
                        break;
                }

            } while (!con.equals("x"));

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            closeConnections();
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.print("Usage java -jar duplicate-finder.jar <DB TYPE> <DB HOST> <CASSANDRA HOST> <CSV FILE PATH>");
            System.exit(-1);
        } else {
            ArgsCache.setArgs(args[0], args[1], args[2], args[3]);
            mainMenu(args);
            System.exit(0);
        }


    }

}
