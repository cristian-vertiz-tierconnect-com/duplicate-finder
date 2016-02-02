package com.mojix;

import com.mojix.cache.ArgsCache;
import com.mojix.dao.CassandraDAO;
import com.mojix.dao.CsvDAO;
import com.mojix.dao.DbDAO;
import com.mojix.driver.Cassandra;
import com.mojix.utils.Console;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.List;

public class DuplicateFinder {

    private static String[] car = {"|", "/", "-", "\\"};

    private static String[] csvHeader = {"Action", "Date", "Serial", "Id", "IsInCsv", "IsParent", "DbRowsAffected", "CassandraRowsAffected"};

    public static void findDuplicates()
            throws ClassNotFoundException,
            SQLException,
            InstantiationException,
            IllegalAccessException,
            IOException {

        //Get values from databases
        Map<Long, Map<String, Long>> thingFieldMap = DbDAO.getInstance().getThingFieldMap(ArgsCache.database);
        Map<Long, Map<String, Object>> thingList = DbDAO.getInstance().getThingList(ArgsCache.database, ArgsCache.thingTypeCode);
        List<Map<String, Object>> csvFileList = CsvDAO.getInstance().readScv(ArgsCache.csvFile);

        List<String> results = new ArrayList<>();
        results.add(buildHeaderCsv());

        //Loop things from mysql/mssql
        //If thing is nor in csv file and thing has no child "delete" else "merge"
        for (Map.Entry<Long, Map<String, Object>> thingEntry : thingList.entrySet()) {
            boolean contains = csvContains(thingEntry.getValue().get("serial").toString(), csvFileList);
            boolean isParent = isParent(thingEntry.getValue().get("serial").toString(), thingList);

            if (!contains) {
                if (isParent) {
                    results.add(mergeThing(thingEntry.getKey(), thingEntry.getValue(), thingFieldMap.get(thingEntry.getKey()), csvFileList));
                } else {
                    results.add(deleteThing(thingEntry.getKey(), thingEntry.getValue(), thingFieldMap.get(thingEntry.getKey())));
                }
            } else if (!contains && isParent) {
                results.add(doNothing(thingEntry.getKey(), thingEntry.getValue(), thingFieldMap.get(thingEntry.getKey()), csvFileList));
            }
        }

        saveResultsTofile(results);

    }

    private static void saveResultsTofile(List<String> results) throws IOException {
        String fileName = "results_" + (new SimpleDateFormat("YYYYMMddhhmmss").format(new Date())) + ".csv";
        File file = new File(fileName);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        try {
            for (String row : results) {
                writer.write(row + "\n");
            }
        } finally {
            if (writer != null) writer.close();
            System.out.println("*** Results have been written to csv file  " + fileName);
        }
    }

    private static String mergeThing(Long thingId,
                                     Map<String, Object> thingMap,
                                     Map<String, Long> thingFieldMap,
                                     List<Map<String, Object>> csvFileList) {
        int dbDelete = 0;

        Map<String, Object> out = new HashMap<>();
        out.put("Action", "MERGED");
        out.put("Date", new Date());
        out.put("Serial", thingMap.get("serial"));
        out.put("Id", thingId);
        out.put("IsInCsv", false);
        out.put("IsParent", false);
        out.put("DbRowsAffected", dbDelete);
        out.put("CassandraRowsAffected", dbDelete);

        return buildCsvRow(out);
    }

    private static String doNothing(Long thingId,
                                    Map<String, Object> thingMap,
                                    Map<String, Long> thingFieldMap,
                                    List<Map<String, Object>> csvFileList) {
        int dbDelete = 0;

        Map<String, Object> out = new HashMap<>();
        out.put("Action", "NOTHING");
        out.put("Date", new Date());
        out.put("Serial", thingMap.get("serial"));
        out.put("Id", thingId);
        out.put("IsInCsv", false);
        out.put("IsParent", false);
        out.put("DbRowsAffected", dbDelete);
        out.put("CassandraRowsAffected", dbDelete);

        return buildCsvRow(out);
    }

    private static String deleteThing(Long thingId,
                                      Map<String, Object> thingMap,
                                      Map<String, Long> thingFieldMap) throws SQLException {

//        int dbDeleted = DbDAO.getInstance().deleteThing(thingId, ArgsCache.database);
        int cassandraDeleted = CassandraDAO.deleteThing(thingId, new ArrayList<Long>());

        Map<String, Object> out = new HashMap<>();
        out.put("Action", "DELETED");
        out.put("Date", new Date());
        out.put("Serial", thingMap.get("serial"));
        out.put("Id", thingId);
        out.put("IsInCsv", false);
        out.put("IsParent", false);
        out.put("DbRowsAffected", 0);
        out.put("CassandraRowsAffected", 0);

        return buildCsvRow(out);

    }

    private static String buildHeaderCsv() {
        Map<String, Object> out = new HashMap<>();
        for (int i = 0; i < csvHeader.length; i++) {
            out.put(csvHeader[i], csvHeader[i]);
        }
        return buildCsvRow(out);
    }

    private static String buildCsvRow(Map<String, Object> out) {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < csvHeader.length; i++) {
            sb.append(out.get(csvHeader[i]));
            if (i < csvHeader.length - 1) {
                sb.append(",");
            }
        }

        return sb.toString();
    }

    private static boolean csvContains(String serial,
                                       List<Map<String, Object>> csvFileList) {
        boolean result = false;
        for (Map<String, Object> item : csvFileList) {
            if (item.get("serial").toString() != null) {
                result = item.get("serial").toString().equals(serial);
                if (result) {
                    break;
                }
            }
        }
        return result;
    }

    private static boolean isParent(String serial,
                                    Map<Long, Map<String, Object>> thingList) {
        boolean result = false;

        for (Map.Entry<Long, Map<String, Object>> entry : thingList.entrySet()) {
            if (entry.getValue().get("parentSerial") != null) {
                result = entry.getValue().get("parentSerial").toString().equals(serial);
                if (result) {
                    break;
                }
            }
        }

        return result;
    }


    public static void openConnections() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        //Init drivers
        Cassandra.init();
        DbDAO.getInstance().initMysqlJDBCDrivers();
    }

    public static void closeConnections() {
        try {
            Cassandra.shutdown();
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
        System.out.print("Type your option ");

    }

    public static void mainMenu() {
        try {
            openConnections();
            String con;
            do {
                printMenu();
                con = Console.read();
                switch (con) {
                    case "1":
                        System.out.println("Finding duplicates...");
                        long ti = System.currentTimeMillis();
                        findDuplicates();
                        System.out.println("Done finding duplicates (elapsed time " + ((System.currentTimeMillis() - ti) / 1000) + " seconds)");
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
        if (args.length < 5) {
            System.out.print("Usage java -jar duplicate-finder.jar <THING TYPE CODE> <DB TYPE> <DB HOST> <CASSANDRA HOST> <CSV FILE PATH>");
            System.exit(-1);
        } else {
            ArgsCache.setArgs(args[0], args[1], args[2], args[3], args[4]);
            mainMenu();
            System.exit(0);
        }


    }

}
