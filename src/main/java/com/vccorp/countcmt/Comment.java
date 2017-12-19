package com.vccorp.countcmt;

import scala.Tuple1;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Comment {

    private static ArrayList<Tuple1<String>> readFileName(String path) {
        ArrayList<Tuple1<String>> fileNames = new ArrayList<>();
        File folder = new File(path);
        File[] listFiles = folder.listFiles();
        for (File f : listFiles) {
            fileNames.add(new Tuple1(f.getName()));
        }
        return fileNames;
    }
    public static void main(String[] args) {
        List<Tuple1<String>> fileNames = readFileName("/home/ahcogn/Documents");
        File folder = new File("/home/ahcogn/Documents");
        File[] listFiles = folder.listFiles();
        fileNames.forEach(s -> System.out.println(s._1));
        String s = "hello from the other side";
        String[] arr = s.split(" ");
        System.out.println(arr.length);
    }
}
