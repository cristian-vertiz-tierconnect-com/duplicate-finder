package com.mojix.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.mojix.driver.Cassandra;
import com.mojix.utils.TextUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cvertiz on 2/2/16.
 */
public class CassandraDAO {

    public static CassandraDAO INSTANCE = new CassandraDAO();

    public static CassandraDAO getInstance(){
        if(INSTANCE == null){
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

    public Map<Long, List<Map<String, Object>>> getHistory(Map<Long, Long> thingFieldThingMap) {
        Map<Long, List<Map<String, Object>>> result = new HashMap<>();
        long countFVH = 0;
        PreparedStatement selectFVH2 = Cassandra.getSession().prepare("SELECT field_id, at, value FROM field_value_history limit 1000000000");
        for (Row row : Cassandra.getSession().execute(new BoundStatement(selectFVH2))) {

            Long field_id = row.getLong("field_id");
            Long thing_id = thingFieldThingMap.get(field_id);
            Long time = row.getDate("at").getTime();
            String value = row.getString("value");

            Map<String, Object> rowValues = new HashMap<>();

            rowValues.put("field_id",field_id);
            rowValues.put("thing_id",thing_id);
            rowValues.put("at",time);
            rowValues.put("value",value);

            if (!result.containsKey(thing_id)) {
                result.put(thing_id, new ArrayList<Map<String, Object>>());
            }

            result.get(thing_id).add(rowValues);

            countFVH++;

            if (countFVH % TextUtils.MOD == 0) {
                System.out.print("\rGetting Cassandra field_value_history " + TextUtils.CAR[(int) (countFVH / TextUtils.MOD) % 4]);
            }
        }
        System.out.println("\rGetting Cassandra field_value_history [OK]");
        return  result;
    }

    public Map<Long, Map<String, Object>> getLastValues(Map<Long, Long> thingFieldThingMap) {
        Map<Long, Map<String, Object>> result = new HashMap<>();
        long countFV = 0;
        PreparedStatement selectFVH2 = Cassandra.getSession().prepare("SELECT field_id, time, value FROM field_value limit 1000000000");
        for (Row row : Cassandra.getSession().execute(new BoundStatement(selectFVH2))) {

            Long field_id = row.getLong("field_id");
            Long thing_id = thingFieldThingMap.get(field_id);
            Long time = row.getDate("time").getTime();
            String value = row.getString("value");

            Map<String, Object> rowValues = new HashMap<>();

            rowValues.put("field_id",field_id);
            rowValues.put("thing_id",thing_id);
            rowValues.put("at",time);
            rowValues.put("value",value);


            result.put(thing_id, rowValues);

            countFV++;

            if (countFV % TextUtils.MOD == 0) {
                System.out.print("\rGetting Cassandra field_value " + TextUtils.CAR[(int) (countFV / TextUtils.MOD) % 4]);
            }
        }
        System.out.println("\rGetting Cassandra field_value [OK]");
        return  result;
    }

}
