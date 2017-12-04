//package com.vccorp.countcmt;
//
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.codehaus.jackson.map.ObjectMapper;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Iterator;
//
//public class ParseJson implements FlatMapFunction<Iterator<String>,Comment>{
//
//    JavaRDD<String> lines, input;
//
//    public ParseJson (JavaRDD<String> lines){
//        this.lines = lines;
//        input =lines.flatMap(s-> Arrays.asList(s.split(" ")).iterator());
//    }
//
//    @Override
//    public Iterator<Comment> call(Iterator<String> stringIterator) throws Exception {
//        ArrayList<Comment> comments = new ArrayList<>();
//        ObjectMapper mapper = new ObjectMapper();
//        while (input.collect().iterator().hasNext()){
//
//        }
//        return null;
//    }
//}
