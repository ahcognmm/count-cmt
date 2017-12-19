package com.vccorp.countcmt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParseJson {



    public static void main(String[] args) {

        ArrayList<Integer> list = new ArrayList<>();
        for(int i =0; i<100 ; i++){
            list.add(i);
        }

        for(int i =0; i<100;i++){
            if(i >50) list.remove(50);
        }

        System.out.println(list.size());

    }

}
