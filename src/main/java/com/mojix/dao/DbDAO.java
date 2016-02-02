package com.mojix.dao;

import com.mojix.cache.ArgsCache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
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


    public Map<Long, Map<String, Long>> getThingFieldMap(String database)
            throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {

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
            }
        } else {
            System.out.println("No connection available for " + System.getProperty("connection.url." + database));
        }


        return thingFieldMap2;
    }

    public Map<Long, Map<String, Object>> getThingList(String database)
            throws SQLException,
            IllegalAccessException,
            InstantiationException,
            ClassNotFoundException {

        java.sql.ResultSet rs = null;
        if (conn != null && database.equals("mysql")) {
            rs = conn.createStatement().executeQuery(
                    "SELECT t1.id AS id, t1.serial AS serial, t2.id AS parent_id, t2.serial AS parent_serial " +
                            "FROM apc_thing AS t1 LEFT JOIN apc_thing AS t2 ON t1.parent_id = t2.id");
//            rs = conn.createStatement().executeQuery("SELECT id, serial, parent_id FROM apc_thing");
        } else if (conn != null && database.equals("mssql")) {
            rs = conn.createStatement().executeQuery(
                    "SELECT t1.id id, t1.serial serial, t2.id parent_id, t2.serial parent_serial " +
                            "FROM dbo.apc_thing t1 LEFT JOIN dbo.apc_thing t2 ON t1.parent_id = t2.id");
//            rs = conn.createStatement().executeQuery("SELECT id, serial, parent_id FROM dbo.apc_thing");
        }

        Map<Long, Map<String, Object>> thingList = new HashMap<Long, Map<String, Object>>();

        if (rs != null) {
            int counter = 0;
            while (rs.next()) {

                Long thingId = rs.getLong("id");
                String serial = rs.getString("serial");
                Long parentId = rs.getLong("parent_id") == 0L ? null : rs.getLong("parent_id");
                String parentSerial = rs.getString("parent_serial");

                Map<String, Object> temp = new HashMap<String, Object>();

                temp.put("serial", serial);
                temp.put("parentId", parentId);
                temp.put("parentSerial", parentSerial);

                thingList.put(thingId, temp);

                counter++;

            }
        } else {
            System.out.println("No connection available for " + System.getProperty("connection.url." + database));
        }

        return thingList;
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

    public int deleteThing(Long thingId,
                           String database) throws SQLException {
        String sql = "";
        if (conn != null && database.equals("mysql")) {
            sql = "DELETE FROM apc_things WHERE id=?";
        } else if (conn != null && database.equals("mssql")) {
            sql = "DELETE FROM dbo.apc_things WHERE id=?";
        }
        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setLong(1, thingId);
        return statement.executeUpdate();
    }

    public void closeConnection() throws SQLException {
        if(!conn.isClosed()){
            conn.close();
        }
    }
}
