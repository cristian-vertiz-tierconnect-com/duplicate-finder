package com.mojix.dao;

import com.mojix.utils.TextUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cvertiz on 2/2/16.
 */
public class CsvDAO {

    private static CsvDAO INSTANCE = new CsvDAO();

    public static CsvDAO getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new CsvDAO();
        }
        return INSTANCE;
    }

    public List<Map<String, Object>> readCsv(String filePath) {
        List<Map<String, Object>> result = new ArrayList<>();

        try {
            Reader in = new FileReader(filePath);
            CSVParser records = CSVFormat.EXCEL.withHeader().withSkipHeaderRecord().parse(in);

            for(CSVRecord row : records.getRecords()){
                Map<String, Object> rowMap = new HashMap<>();

                String serial = TextUtils.cleanString(row.get("Equipment"));
                String child = TextUtils.cleanString(row.get("Serial Number"));

                rowMap.put("serial", serial.isEmpty()?null:serial);
                rowMap.put("parent", child);
                result.add(rowMap);
            }


        } catch (IOException e) {
            System.out.println("File " + filePath + " not found!");
        }
        return  result;
    }

}
