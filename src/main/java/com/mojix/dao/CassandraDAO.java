package com.mojix.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.mojix.driver.Cassandra;
import com.mojix.utils.TextUtils;

import java.util.*;

/**
 * Created by cvertiz on 2/2/16.
 */
public class CassandraDAO {

    public static CassandraDAO INSTANCE = new CassandraDAO();

    public static CassandraDAO getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new CassandraDAO();
        }
        return INSTANCE;
    }


    public void deleteThing(List<Long> ids) {

        PreparedStatement deletPSFV = Cassandra.getSession().prepare(
                "DELETE FROM field_value WHERE field_id IN :idx");

        BoundStatement bsFV = new BoundStatement(deletPSFV);
        bsFV.setList("idx", ids);

        PreparedStatement deletPSFVH = Cassandra.getSession().prepare(
                "DELETE FROM field_value_history WHERE field_id IN :idx");

        BoundStatement bsFVH = new BoundStatement(deletPSFVH);
        bsFVH.setList("idx", ids);

        Cassandra.getSession().executeAsync(bsFV);
        Cassandra.getSession().executeAsync(bsFVH);
    }

    public Map<Long, Map<Long, List<Map<String, Object>>>> getHistory(Map<Long, Long> thingFieldThingMap) {
        Map<Long, Map<Long, List<Map<String, Object>>>> result = new HashMap<>();
        long countFVH = 0;
        PreparedStatement selectFVH2 = Cassandra.getSession().prepare("SELECT field_id, at, value FROM field_value_history limit 1000000000");
        for (Row row : Cassandra.getSession().execute(new BoundStatement(selectFVH2))) {

            Long field_id = row.getLong("field_id");
            Long thing_id = thingFieldThingMap.get(field_id);
            Long at = row.getDate("at").getTime();
            String value = row.getString("value");

            Map<String, Object> rowValues = new HashMap<>();

            rowValues.put("field_id", field_id);
            rowValues.put("thing_id", thing_id);
            rowValues.put("at", at);
            rowValues.put("value", value);

            if (!result.containsKey(thing_id)) {
                result.put(thing_id, new HashMap<Long, List<Map<String, Object>>>());
            }
            if (!result.get(thing_id).containsKey(field_id)) {
                result.get(thing_id).put(field_id, new ArrayList<Map<String, Object>>());
            }

            result.get(thing_id).get(field_id).add(rowValues);

            countFVH++;

            if (countFVH % TextUtils.MOD == 0) {
                System.out.print("\rGetting Cassandra field_value_history " + TextUtils.CAR[(int) (countFVH / TextUtils.MOD) % 4]);
            }
        }
        System.out.println("\rGetting Cassandra field_value_history [OK]");
        return result;
    }

    public Map<Long, List<Map<String, Object>>> getLastValues(Map<Long, Long> thingFieldThingMap) {
        Map<Long, List<Map<String, Object>>> result = new HashMap<>();
        long countFV = 0;
        PreparedStatement selectFVH2 = Cassandra.getSession().prepare("SELECT field_id, time, value FROM field_value limit 1000000000");
        for (Row row : Cassandra.getSession().execute(new BoundStatement(selectFVH2))) {

            Long field_id = row.getLong("field_id");
            Long thing_id = thingFieldThingMap.get(field_id);
            Long time = row.getDate("time").getTime();
            String value = row.getString("value");

            Map<String, Object> rowValues = new HashMap<>();

            rowValues.put("field_id", field_id);
            rowValues.put("thing_id", thing_id);
            rowValues.put("time", time);
            rowValues.put("value", value);

            if (!result.containsKey(thing_id)) {
                result.put(thing_id, new ArrayList<Map<String, Object>>());
            }

            result.get(thing_id).add(rowValues);

            countFV++;

            if (countFV % TextUtils.MOD == 0) {
                System.out.print("\rGetting Cassandra field_value " + TextUtils.CAR[(int) (countFV / TextUtils.MOD) % 4]);
            }
        }
        System.out.println("\rGetting Cassandra field_value [OK]");
        return result;
    }

    public void writeFieldValue(List<Map<String, Object>> fieldValue) {
        PreparedStatement fieldValuePS = Cassandra.getSession().prepare(
                "INSERT INTO field_value (field_id, time, value) VALUES (:fId, :t, :v)");
        for (Map<String, Object> value : fieldValue) {
            BoundStatement bsFV = new BoundStatement(fieldValuePS);

            bsFV.setLong("fId", value.get("field_id") != null ? (long) value.get("field_id") : null);
            bsFV.setDate("t", value.get("time") != null ? new Date((long) value.get("time")) : null);
            bsFV.setString("v", value.get("value") != null ? value.get("value").toString() : null);

            Cassandra.getSession().executeAsync(bsFV);

        }
    }

    public void writeFieldValueHistory(Map<Long, List<Map<String, Object>>> fieldValueHistory) {
        PreparedStatement fieldValuePS = Cassandra.getSession().prepare(
                "INSERT INTO field_value_history (field_id, at, value) VALUES (:fId, :t, :v)");

        for (Map.Entry<Long, List<Map<String, Object>>> fieldValueList : fieldValueHistory.entrySet()) {

            for (Map<String, Object> value : fieldValueList.getValue()) {
                BoundStatement bsFV = new BoundStatement(fieldValuePS);
                bsFV.setLong("fId", value.get("field_id") != null ? (long) value.get("field_id") : null);
                bsFV.setDate("t", value.get("at") != null ? new Date((long) value.get("at")) : null);
                bsFV.setString("v", value.get("value") != null ? value.get("value").toString() : null);
                Cassandra.getSession().executeAsync(bsFV);
            }

        }


    }
}
