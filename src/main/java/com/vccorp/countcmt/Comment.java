package com.vccorp.countcmt;

import java.io.File;

public class Comment {
    public static void main(String[] args) {
        File folder = new File("/home/ahcogn/Documents");
        File[] listFiles = folder.listFiles();
        for(File f : listFiles){
            System.out.println(f.getName());
        }
    }
}
