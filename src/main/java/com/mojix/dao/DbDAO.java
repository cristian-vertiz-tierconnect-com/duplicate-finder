package com.mojix.dao;

import com.mojix.cache.ArgsCache;
import com.mojix.utils.TextUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cvertiz on 2/2/16.
 */
public class DbDAO {
    private Connection conn = null;

    private static DbDAO INSTANCE = new DbDAO();

    public static DbDAO getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DbDAO();
        }
        return INSTANCE;
    }


    public List<Map<String, Object>> getThingFields(String database)
            throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        java.sql.ResultSet rs = null;
        if (conn != null && database.equals("mysql")) {
            rs = conn.createStatement().executeQuery(
                    "SELECT id, thing_id, thingTypeFieldId, name " +
                            "FROM thingfield");
        } else if (conn != null && database.equals("mssql")) {
            rs = conn.createStatement().executeQuery(
                    "SELECT id, thing_id, thingTypeFieldId " +
                            "FROM dbo.thingfield");
        }
        List<Map<String, Object>> thingFieldMap = new ArrayList<>();
        long countTF = 0;
        if (rs != null) {
            while (rs.next()) {

                Long thingId = rs.getLong("thing_id");
                Long thingFieldId = rs.getLong("id");
                Long thingTypeFieldId = rs.getLong("thingTypeFieldId");
                String name = rs.getString("name");


                Map<String, Object> row = new HashMap<>();
                row.put("thing_id",thingId);
                row.put("id",thingFieldId);
                row.put("thingTypeFieldId",thingTypeFieldId);
                row.put("name",name);

                thingFieldMap.add(row);

                countTF++;
                if (countTF % TextUtils.MOD == 0) {
                    System.out.print("\rGetting " + ArgsCache.database + " thingField values " + TextUtils.CAR[(int) (countTF / TextUtils.MOD) % 4]);
                }

            }
        } else {
            System.out.println("No connection available for " + System.getProperty("connection.url." + database));
        }

        System.out.println("\rGetting " + ArgsCache.database + " thingField values [OK]");

        return thingFieldMap;
    }

    public Map<Long, List<Long>> getThingFieldMap(List<Map<String, Object>> thingFields)
            throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        long countTF = 0;
        Map<Long, List<Long>> thingFieldMap = new HashMap<>();
        if (thingFields != null && thingFields.size() > 0) {
            for(Map<String, Object> row : thingFields){
                Long thingId = Long.parseLong(row.get("thing_id").toString());
                Long thingFieldId = Long.parseLong(row.get("id").toString());

                if (!thingFieldMap.containsKey(thingId)) {
                    thingFieldMap.put(thingId, new ArrayList<Long>());
                }

                thingFieldMap.get(thingId).add(thingFieldId);

                countTF++;
                if (countTF % TextUtils.MOD == 0) {
                    System.out.print("\rBuilding " + ArgsCache.database + " thingField map " + TextUtils.CAR[(int) (countTF / TextUtils.MOD) % 4]);
                }
            }

        }

        System.out.println("\rBuilding " + ArgsCache.database + " thingField map [OK]");

        return thingFieldMap;
    }


    public Map<String, Map<Long,Long>> getThingFieldNameMap(List<Map<String, Object>> thingFields)
            throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        long countTF = 0;
        Map<String, Map<Long,Long>> thingFieldMap = new HashMap<>();
        if (thingFields != null && thingFields.size() > 0) {
            for(Map<String, Object> row : thingFields){
                Long thingId = Long.parseLong(row.get("thing_id").toString());
                Long thingFieldId = Long.parseLong(row.get("id").toString());
                String name = row.get("name").toString();

                if (!thingFieldMap.containsKey(name)) {
                    thingFieldMap.put(name, new HashMap<Long, Long>());
                }

                thingFieldMap.get(name).put(thingId, thingFieldId);

                countTF++;
                if (countTF % TextUtils.MOD == 0) {
                    System.out.print("\rBuilding " + ArgsCache.database + " thingField name map " + TextUtils.CAR[(int) (countTF / TextUtils.MOD) % 4]);
                }
            }

        }

        System.out.println("\rBuilding " + ArgsCache.database + " thingField name map [OK]");

        return thingFieldMap;
    }

    public Map<Long, Map<Long, Long>> getThingFieldToThingTypeFieldMap(List<Map<String, Object>> thingFields)
            throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        Map<Long, Map<Long, Long>> thingFieldMap = new HashMap<>();
        long countTF = 0;

        if (thingFields != null && thingFields.size() > 0) {
            for(Map<String, Object> row : thingFields){

                Long thingId = Long.parseLong(row.get("thing_id").toString());
                Long thingFieldId = Long.parseLong(row.get("id").toString());
                Long thingTypeFieldId = Long.parseLong(row.get("thingTypeFieldId").toString());

                if (!thingFieldMap.containsKey(thingId)) {
                    thingFieldMap.put(thingId, new HashMap<Long, Long>());
                }

                thingFieldMap.get(thingId).put(thingFieldId, thingTypeFieldId);

                countTF++;
                if (countTF % TextUtils.MOD == 0) {
                    System.out.print("\rBuilding " + ArgsCache.database + " thingField To thingTypeField map " + TextUtils.CAR[(int) (countTF / TextUtils.MOD) % 4]);
                }

            }
        }

        System.out.println("\rBuilding " + ArgsCache.database + " thingField To thingTypeField map [OK]");

        return thingFieldMap;
    }

    public Map<Long, Map<Long, Long>> getThingTypeFieldToThingFieldMap(List<Map<String, Object>> thingFields)
            throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        Map<Long, Map<Long, Long>> thingFieldMap = new HashMap<>();
        long countTF = 0;
        if (thingFields != null && thingFields.size() > 0) {
            for(Map<String, Object> row : thingFields){

                Long thingId = Long.parseLong(row.get("thing_id").toString());
                Long thingFieldId = Long.parseLong(row.get("id").toString());
                Long thingTypeFieldId = Long.parseLong(row.get("thingTypeFieldId").toString());

                if (!thingFieldMap.containsKey(thingTypeFieldId)) {
                    thingFieldMap.put(thingTypeFieldId, new HashMap<Long, Long>());
                }

                thingFieldMap.get(thingTypeFieldId).put(thingId, thingFieldId);

                countTF++;
                if (countTF % TextUtils.MOD == 0) {
                    System.out.print("\rBuilding " + ArgsCache.database + " thingTypeField to thingField map " + TextUtils.CAR[(int) (countTF / TextUtils.MOD) % 4]);
                }

            }
        }

        System.out.println("\rBuilding " + ArgsCache.database + " thingTypeField to thingField map [OK]");

        return thingFieldMap;
    }

    public Map<Long, Long> getThingFieldThingMap(List<Map<String, Object>> thingFields)
            throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        Map<Long, Long> thingFieldMap = new HashMap<>();
        long countTF = 0;
        if (thingFields != null && thingFields.size() > 0) {
            for(Map<String, Object> row : thingFields){

                Long thingId = Long.parseLong(row.get("thing_id").toString());
                Long thingFieldId = Long.parseLong(row.get("id").toString());

                thingFieldMap.put(thingFieldId, thingId);
                countTF++;
                if (countTF % TextUtils.MOD == 0) {
                    System.out.print("\rBuilding " + ArgsCache.database + " thingField to thing map " + TextUtils.CAR[(int) (countTF / TextUtils.MOD) % 4]);
                }
            }
        }

        System.out.println("\rBuilding " + ArgsCache.database + " thingField to thing map [OK]");
        return thingFieldMap;
    }


    public Map<String, List<Map<String, Object>>> getThingList(String database)
            throws SQLException,
            IllegalAccessException,
            InstantiationException,
            ClassNotFoundException {

        java.sql.ResultSet rs = null;

        String query = "SELECT t1.id AS id, " +
                "t1.serial AS serial, " +
                "t1.thingTypeCode AS thingTypeCode, " +
                "t2.id AS parent_id, " +
                "t2.serial AS parent_serial, " +
                "t2.thingTypeCode AS parentThingTypeCode " +
                "FROM " +
                "(SELECT t.*, tt.thingTypeCode FROM <SCHEMA>apc_thing AS t, thingtype as tt WHERE t.thingType_id = tt.id) AS t1 " +
                "LEFT JOIN " +
                "(SELECT t.*, tt.thingTypeCode FROM <SCHEMA>apc_thing AS t, thingtype as tt WHERE t.thingType_id = tt.id) AS t2 " +
                "ON t1.parent_id = t2.id";

        if (conn != null && database.equals("mysql")) {
            query = query.replaceAll("<SCHEMA>", "");
        } else if (conn != null && database.equals("mssql")) {
            query = query.replaceAll("<SCHEMA>", "dbo.");
        }
        rs = conn.createStatement().executeQuery(query);

        Map<String, List<Map<String, Object>>> thingMap = new HashMap<>();
        long countTF = 0;
        if (rs != null) {

            while (rs.next()) {

                Long thingId = rs.getLong("id");
                String serial = rs.getString("serial");
                String thingType = rs.getString("thingTypeCode");
                Long parentId = rs.getLong("parent_id") == 0L ? null : rs.getLong("parent_id");
                String parentSerial = rs.getString("parent_serial");
                String parentThingType = rs.getString("parentThingTypeCode");

                Map<String, Object> temp = new HashMap<String, Object>();

                temp.put("id", thingId);
                temp.put("serial", TextUtils.cleanString(serial));
                temp.put("parentId", parentId);
                temp.put("parentSerial", TextUtils.cleanString(parentSerial));

                if (!thingMap.containsKey(thingType)) {
                    thingMap.put(thingType, new ArrayList<Map<String, Object>>());
                }
                thingMap.get(thingType).add(temp);

                countTF++;
                if (countTF % TextUtils.MOD == 0) {
                    System.out.print("\rGetting " + ArgsCache.database + " thing values " + TextUtils.CAR[(int) (countTF / TextUtils.MOD) % 4]);
                }
            }
        } else {
            System.out.println("No connection available for " + System.getProperty("connection.url." + database));
        }

        System.out.println("\rGetting " + ArgsCache.database + " thing values [OK]");
        return thingMap;
    }

    public void setDBProperties(String dbHost) {
        System.getProperties().put("connection.url.mysql", "jdbc:mysql://" + dbHost + ":3306/riot_main");
        System.getProperties().put("connection.username.mysql", "root");
        System.getProperties().put("connection.password.mysql", "control123!");
        System.getProperties().put("connection.url.mssql", "jdbc:jtds:sqlserver://" + dbHost + ":1433/riot_main");
        System.getProperties().put("connection.username.mssql", "sa");
        System.getProperties().put("connection.password.mssql", "control123!");

    }

    public void initMysqlJDBCDrivers() throws ClassNotFoundException,
            SQLException,
            IllegalAccessException,
            InstantiationException {

        setDBProperties(ArgsCache.dbHost);

        String database = ArgsCache.database;
        String url = System.getProperty("connection.url." + database);
        String userName = System.getProperty("connection.username." + database);
        String password = System.getProperty("connection.password." + database);

        String driverMysql = "org.gjt.mm.mysql.Driver";
        String driverMssql = "net.sourceforge.jtds.jdbc.Driver";

        Class.forName(driverMysql).newInstance();
        Class.forName(driverMssql).newInstance();
        conn = DriverManager.getConnection(url, userName, password);
    }

    public void deleteThing(Long thingId,
                           String database) throws SQLException {
        String sqlT = "DELETE FROM dbo.apc_thing WHERE id=?";
        String sqlTf = "DELETE FROM thingField WHERE thing_id=?";
        if (conn != null && database.equals("mysql")) {
            sqlT = sqlT.replaceAll("dbo.","");
        } else if (conn != null && database.equals("mssql")) {

        }
        PreparedStatement statementTf = conn.prepareStatement(sqlTf);
        statementTf.setLong(1, thingId);
        statementTf.executeUpdate();

        PreparedStatement statementT = conn.prepareStatement(sqlT);
        statementT.setLong(1, thingId);
        statementT.executeUpdate();
    }

    public void closeConnection() throws SQLException {
        if (!conn.isClosed()) {
            conn.close();
        }
    }
}
