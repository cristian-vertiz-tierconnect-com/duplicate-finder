package com.mojix.utils;

/**
 * Created by cvertiz on 2/2/16.
 */
public class TextUtils {

    private static String[] replace = {"\n", " "};

    public static String cleanString(String text){
        for(String item : replace){
            if(text!=null){
                text = text.replace(item, "");
            }
        }
        return text;
    }
}
