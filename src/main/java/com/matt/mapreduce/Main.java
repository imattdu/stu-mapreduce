package com.matt.mapreduce;

import com.google.common.collect.UnmodifiableIterator;

import java.util.zip.CheckedOutputStream;

/**
 * @author matt
 * @create 2021-12-15 1:26
 */
public class Main {


    public static void main(String[] args) {
        String s1 = "aa bb cc";
        String[] split = s1.split("\t");
        for (String s : split) {
            System.out.println(s);
        }
    }

}
