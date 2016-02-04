package com.mojix;

import com.datastax.driver.core.Configuration;
import com.mojix.cache.ArgsCache;
import com.mojix.dao.CassandraDAO;
import com.mojix.dao.CsvDAO;
import com.mojix.dao.DbDAO;
import com.mojix.driver.Cassandra;
import com.mojix.utils.ArgsParser;
import com.mojix.utils.Console;
import com.mojix.utils.TextUtils;
import org.apache.commons.cli.CommandLine;

import java.io.*;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DuplicateFinder {


    private static final String CONFIGURATION_FILE_PATH = "conf.properties";
    private static ArgsParser argsParser;
    private static CommandLine line;

    private static String[] csvHeader = {"Action", "Date", "Serial", "Id", "IsInCsv", "IsParent", "DuplicateId", "DuplicateSerial", "DuplicateIsParent"};


    private static List<Long> donotProcessList = new ArrayList<>();

    public static void findDuplicates()
            throws ClassNotFoundException,
            SQLException,
            InstantiationException,
            IllegalAccessException,
            IOException {

        //Get values from databases
        Map<Long, List<Long>> thingFieldMap = DbDAO.getInstance().getThingFieldMap(ArgsCache.database);
        Map<Long, Map<Long, Long>> thingFieldToThingTypeFieldMap = DbDAO.getInstance().getThingFieldToThingTypeFieldMap(ArgsCache.database);
        Map<Long, Map<Long, Long>> thingTypeFieldToThingFieldMap = DbDAO.getInstance().getThingTypeFieldToThingFieldMap(ArgsCache.database);
        Map<Long, Long> thingFieldThingMap = DbDAO.getInstance().getThingFieldThingMap(ArgsCache.database);
        Map<String, List<Map<String, Object>>> thingMap = DbDAO.getInstance().getThingList(ArgsCache.database);
        List<Map<String, Object>> csvFileList = CsvDAO.getInstance().readCsv(ArgsCache.csvFile);
        Map<Long, List<Map<String, Object>>> fieldValue = CassandraDAO.getInstance().getLastValues(thingFieldThingMap);
        Map<Long, Map<Long, List<Map<String, Object>>>> fieldValueHistory = CassandraDAO.getInstance().getHistory(thingFieldThingMap);

        List<String> results = new ArrayList<>();
        results.add(buildHeaderCsv());

        //Loop things from mysql/mssql
        //If thing is nor in csv file and thing has no child "delete" else "merge"
        long countT = 0;
        for (Map<String, Object> thing : thingMap.get(ArgsCache.parentThingTypeCode)) {

            if (!donotProcessList.contains(Long.parseLong(thing.get("id").toString()))) {
                boolean contains = csvContains(thing.get("serial").toString(), csvFileList);
                boolean isParent = isParent(thing.get("serial").toString(), thingMap.get(ArgsCache.childrenThingTypeCode));
                Map<String, Object> duplicate = getDuplicate(thing.get("serial").toString(),
                        (long) thing.get("id"), thingMap.get(ArgsCache.parentThingTypeCode));

                if (!contains && ArgsCache.csvFile != null) {
                    if (isParent) {
                        if (duplicate != null) {
                            results.add(mergeThing(thing,
                                    thingFieldMap.get(Long.parseLong(thing.get("id").toString())),
                                    fieldValue.get(Long.parseLong(thing.get("id").toString())),
                                    fieldValueHistory.get(Long.parseLong(thing.get("id").toString())),
                                    duplicate,
                                    thingFieldMap.get(Long.parseLong(duplicate.get("id").toString())),
                                    fieldValue.get(Long.parseLong(duplicate.get("id").toString())),
                                    fieldValueHistory.get(Long.parseLong(duplicate.get("id").toString())),
                                    thingMap.get(ArgsCache.childrenThingTypeCode),
                                    isParent, contains,
                                    thingFieldToThingTypeFieldMap,
                                    thingTypeFieldToThingFieldMap
                            ));
                        } else {
                            results.add(doNothing(thing));
                        }
                    } else {
                        results.add(deleteThing(thing,
                                thingFieldMap.get(Long.parseLong(thing.get("id").toString()))));
                    }
                } else if (duplicate != null) {
                    results.add(mergeThing(thing,
                            thingFieldMap.get(Long.parseLong(thing.get("id").toString())),
                            fieldValue.get(Long.parseLong(thing.get("id").toString())),
                            fieldValueHistory.get(Long.parseLong(thing.get("id").toString())),
                            duplicate,
                            thingFieldMap.get(Long.parseLong(duplicate.get("id").toString())),
                            fieldValue.get(Long.parseLong(duplicate.get("id").toString())),
                            fieldValueHistory.get(Long.parseLong(duplicate.get("id").toString())),
                            thingMap.get(ArgsCache.childrenThingTypeCode),
                            isParent, contains, thingFieldToThingTypeFieldMap, thingTypeFieldToThingFieldMap));
                } else {
                    results.add(doNothing(thing));
                }
            }
            countT++;
            if ((countT * 1000) % TextUtils.MOD == 0) {
                System.out.print("\rAnalysing duplicated values " + TextUtils.CAR[(int) (countT * 1000 / TextUtils.MOD) % 4]);
            }
        }
        System.out.println("\rAnalysing duplicated values [OK]");
        saveResultsToFile(results);

    }

    private static String mergeThing(Map<String, Object> thing,
                                     List<Long> thingFieldList,
                                     List<Map<String, Object>> fieldValue,
                                     Map<Long, List<Map<String, Object>>> fieldValueHistory,
                                     Map<String, Object> duplicate,
                                     List<Long> duplicateThingFieldList,
                                     List<Map<String, Object>> duplicateFieldValue,
                                     Map<Long, List<Map<String, Object>>> duplicateFieldValueHistory,
                                     List<Map<String, Object>> thingMap,
                                     boolean isParent, boolean isInCsv,
                                     Map<Long, Map<Long, Long>> thingFieldToThingTypeFieldMap,
                                     Map<Long, Map<Long, Long>> thingTypeFieldToThingFieldMap) throws SQLException {


        boolean duplicateIsParent = isParent(duplicate.get("serial").toString(), thingMap);

        String action = "";
        String serial = "";
        String id = "";
        String duplicateSerial = "";
        String duplicateId = "";

        //if (ArgsCache.delete) {
        if (isParent) {
            if (ArgsCache.delete)
                mergeThingData(thing, fieldValue, fieldValueHistory, duplicate,
                        duplicateFieldValue, duplicateFieldValueHistory, thingFieldToThingTypeFieldMap, thingTypeFieldToThingFieldMap);

            id = thing.get("id").toString();
            serial = thing.get("serial").toString();
            duplicateId = duplicate.get("id").toString();
            duplicateSerial = duplicate.get("serial").toString();

            if (ArgsCache.delete) {
                deleteThing(duplicate, duplicateThingFieldList);
                action = "MERGED";
            } else {
                action = "FOR MERGING";
            }

        } else {
            if (duplicateIsParent) {
                mergeThingData(duplicate, duplicateFieldValue, duplicateFieldValueHistory,
                        thing, fieldValue, fieldValueHistory, thingFieldToThingTypeFieldMap, thingTypeFieldToThingFieldMap);

                isParent = true;
                duplicateIsParent = false;

                id = duplicate.get("id").toString();
                serial = duplicate.get("serial").toString();
                duplicateId = thing.get("id").toString();
                duplicateSerial = thing.get("serial").toString();

                if (ArgsCache.delete) {
                    deleteThing(thing, thingFieldList);
                    action = "MERGED";
                } else {
                    action = "FOR MERGING";
                }
            } else {

                id = thing.get("id").toString();
                serial = thing.get("serial").toString();
                duplicateId = duplicate.get("id").toString();
                duplicateSerial = duplicate.get("serial").toString();

                if (ArgsCache.delete) {
                    deleteThing(thing, thingFieldList);
                    deleteThing(duplicate, duplicateThingFieldList);
                    action = "DELETED";
                } else {
                    action = "FOR DELETING";
                }
            }
        }
        //}

        donotProcessList.add(Long.parseLong(thing.get("id").toString()));
        donotProcessList.add(Long.parseLong(duplicate.get("id").toString()));


        return buildCsvRow(getLineMap(action, serial, id, String.valueOf(isParent), String.valueOf(isInCsv), duplicateId, duplicateSerial, String.valueOf(duplicateIsParent)));
    }

    public static Map<String, Object> getLineMap(String action,
                                                 String serial,
                                                 String id,
                                                 String isParent,
                                                 String isInCsv,
                                                 String duplicateId,
                                                 String duplicateSerial,
                                                 String duplicateIsParent) {
        Map<String, Object> out = new HashMap<>();
        out.put("Action", action);
        out.put("Date", new Date());
        out.put("Serial", serial);
        out.put("Id", id);
        out.put("IsInCsv", isInCsv);
        out.put("IsParent", isParent);
        out.put("DuplicateId", duplicateId == null ? "" : duplicateId);
        out.put("DuplicateSerial", duplicateSerial == null ? "" : duplicateSerial);
        out.put("DuplicateIsParent", duplicateIsParent);
        return out;
    }

    private static void mergeThingData(Map<String, Object> thing,
                                       List<Map<String, Object>> fieldValue,
                                       Map<Long, List<Map<String, Object>>> fieldValueHistory,
                                       Map<String, Object> duplicate,
                                       List<Map<String, Object>> duplicateFieldValue,
                                       Map<Long, List<Map<String, Object>>> duplicateFieldValueHistory,
                                       Map<Long, Map<Long, Long>> thingFieldToThingTypeFieldMap,
                                       Map<Long, Map<Long, Long>> thingTypeFieldToThingFieldMap) {


        if (duplicateFieldValue != null && duplicateFieldValue.size() > 0) {

            for (Map<String, Object> value : duplicateFieldValue) {
                Map<String, Object> valueConverted = new HashMap<>();
                valueConverted.putAll(value);
                Long ttf = thingFieldToThingTypeFieldMap
                        .get(Long.parseLong(duplicate.get("id").toString()))
                        .get(Long.parseLong(value.get("field_id").toString()));

                Long tf = thingTypeFieldToThingFieldMap.get(ttf).get(Long.parseLong(thing.get("id").toString()));

                valueConverted.put("field_id", tf);
                fieldValue.add(valueConverted);

                CassandraDAO.getInstance().writeFieldValue(fieldValue);


            }

        }
        if (duplicateFieldValueHistory != null && duplicateFieldValueHistory.size() > 0) {
            for (Map.Entry<Long, List<Map<String, Object>>> fieldValueList : duplicateFieldValueHistory.entrySet()) {

                for (Map<String, Object> value : fieldValueList.getValue()) {

                    Map<String, Object> valueConverted = new HashMap<>();
                    valueConverted.putAll(value);
                    Long ttf = thingFieldToThingTypeFieldMap
                            .get(Long.parseLong(duplicate.get("id").toString()))
                            .get(Long.parseLong(value.get("field_id").toString()));

                    Long tf = thingTypeFieldToThingFieldMap.get(ttf).get(Long.parseLong(thing.get("id").toString()));

                    valueConverted.put("field_id", tf);

                    if(!fieldValueHistory.containsKey(tf)){
                        fieldValueHistory.get(tf).add(new HashMap<String, Object>());
                    }

                    fieldValueHistory.get(tf).add(valueConverted);

                }
            }
            CassandraDAO.getInstance().writeFieldValueHistory(fieldValueHistory);
        }
    }

    private static String deleteThing(Map<String, Object> thingMap,
                                      List<Long> thingFieldList) throws SQLException {

        String action = "FOR DELETING";

        if (ArgsCache.delete) {

            DbDAO.getInstance().deleteThing((long) thingMap.get("id"), ArgsCache.database);
            CassandraDAO.getInstance().deleteThing(thingFieldList);
            action = "DELETED";
        }

//        Map<String, Object> out = new HashMap<>();
//        out.put("Action", action);
//        out.put("Date", new Date());
//        out.put("Serial", thingMap.get("serial"));
//        out.put("Id", thingMap.get("id"));
//        out.put("IsInCsv", false);
//        out.put("IsParent", false);

        return buildCsvRow(getLineMap(action,
                thingMap.get("serial").toString(),
                thingMap.get("id").toString(), "false", "false", "", "", ""));

    }

    private static String doNothing(Map<String, Object> thingMap) {

        String action = "KEEP(NO ACTION)";

        Map<String, Object> out = new HashMap<>();
        out.put("Action", action);
        out.put("Date", new Date());
        out.put("Serial", thingMap.get("serial"));
        out.put("Id", thingMap.get("id"));
        out.put("IsInCsv", false);
        out.put("IsParent", false);


        return buildCsvRow(getLineMap(action, thingMap.get("serial").toString(),
                thingMap.get("id").toString(),"", "", "", "", ""));
    }

    private static void saveResultsToFile(List<String> results) throws IOException {
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
                                    List<Map<String, Object>> thingList) {
        boolean result = false;

        for (Map<String, Object> thing : thingList) {
            if (thing.get("parentSerial") != null) {
                result = thing.get("parentSerial").toString().equals(serial);
                if (result) {
                    break;
                }
            }
        }

        return result;
    }

    private static Map<String, Object> getDuplicate(String serial, Long thingId, List<Map<String, Object>> thingList) {

        for (Map<String, Object> thing : thingList) {
            if (thing.get("serial").toString().equalsIgnoreCase(serial) &&
                    thingId != Long.parseLong(thing.get("id").toString())) {
                //System.out.println(serial + "|" + thingId + " = " + thing.get("serial") + "|" + thing.get("id"));
                return thing;
            }

        }
        return null;
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

    public static boolean init(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        loadDefaultConfig();


        argsParser = new ArgsParser();
        argsParser.addOptions();
        line = argsParser.parseOptions(args);

        if (line.hasOption("f")) {
            ArgsCache.csvFile = line.getOptionValue("f");
        }

        if (line.hasOption("p")) {
            ArgsCache.parentThingTypeCode = line.getOptionValue("p");
        }else{
            System.out.println("Parent thing type code required (use -p <thingTypeCode>)");
            return false;
        }

        if (line.hasOption("c")) {
            ArgsCache.childrenThingTypeCode = line.getOptionValue("c");
        }else{
            System.out.println("Children thing type code required (use -c <thingTypeCode>)");
            return false;
        }

//        if (line.hasOption("g")) {
//            ArgsCache.groupCode = line.getOptionValue("g");
//        }else{
//            System.out.println("Group code required (use -g <groupCode>)");
//            return false;
//        }

        ArgsCache.delete = line.hasOption("d");

        ArgsCache.database = System.getProperty("db.engine");
        ArgsCache.dbHost = System.getProperty("db.host");
        ArgsCache.cassandraHost = System.getProperty("cassandra.host");

        openConnections();

        return true;
    }

    private static void loadDefaultConfig() {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(CONFIGURATION_FILE_PATH));
        } catch (Exception e) {
            try {
                prop.load(Configuration.class.getClassLoader().getResourceAsStream(CONFIGURATION_FILE_PATH));
            } catch (IOException e1) {
                throw new RuntimeException(e);
            }
        }
        for (Map.Entry<Object, Object> entry : prop.entrySet()) {
            if (entry.getValue() != null) {
                System.getProperties().put(entry.getKey().toString(), entry.getValue().toString());
            }
        }
    }

    public static void main(String[] args) {

        //mainMenu();
        try {
            if(init(args)){
                System.out.println("Analysing...");
                long ti = System.currentTimeMillis();
                findDuplicates();
                System.out.println("Done finding duplicates (elapsed time " + ((System.currentTimeMillis() - ti) / 1000) + " seconds)");
                System.exit(0);
            }else{
                System.exit(-1);
            }



        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
