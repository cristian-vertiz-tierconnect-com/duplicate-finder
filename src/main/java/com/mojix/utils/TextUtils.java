package com.mojix.utils;

/**
 * Created by cvertiz on 2/2/16.
 */
public class TextUtils {

    public final static String[] CAR = {"|", "/", "-", "\\"};
    public final static long MOD = 10000;

    private static String[] replace = {"\n", " "};

    public static String cleanString(String text){
        for(String item : replace){
            if(text!=null){
                text = text.replaceAll(item, "");
            }
        }
        return text;
    }
}
