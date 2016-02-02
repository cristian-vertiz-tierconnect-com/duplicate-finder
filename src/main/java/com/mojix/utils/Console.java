package com.mojix.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by cvertiz on 2/2/16.
 */
public class Console {

    public static String read () throws IOException {
       InputStreamReader isr = new InputStreamReader (System.in);
            BufferedReader in = new BufferedReader (isr);
       return in.readLine();
    }
}
