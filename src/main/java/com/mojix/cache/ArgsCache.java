package com.mojix.cache;

/**
 * Created by cvertiz on 2/2/16.
 */
public class ArgsCache {

    ArgsCache INSTANCE = new ArgsCache();

    public static String database;
    public static String dbHost;
    public static String cassandraHost;
    public static String csvFile;

    public ArgsCache() {
    }

    public static void setArgs(String db, String host, String chost, String csv) {
        database = db;
        dbHost = host;
        cassandraHost = chost;
        csvFile = csv;
    }
}
