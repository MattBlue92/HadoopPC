package org.MarulliGemignani;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.log4j.Logger;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class ClusterAssignment extends Mapper<Object, Text, LongWritable , Text > {

    private Logger logger = Logger.getLogger(ClusterAssignment.class); //sto dicendo al logger di monitorare la classe ClusterAssignment
    public static List<double[]> centroids = new ArrayList<>();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        List<Double> coordinates = new ArrayList<Double>();
        StringTokenizer tokenizer = new StringTokenizer(line, ",");
        while(tokenizer.hasMoreTokens())
            coordinates.add(Double.parseDouble(tokenizer.nextToken()));

        double distance = Double.MAX_VALUE;
        double distanceTmp;
        long index = -1;
        for(int i  = 0; i<centroids.size();i++){
            distanceTmp = getDistance(coordinates, centroids.get(i));
            if(distanceTmp<distance){
                distance = distanceTmp;
                index= i;
            }
        }
       if(index!=-1)
           context.write(new LongWritable(index), value);
       else
           logger.fatal("\n\nNessun cluster vicino trovato? min = "+distance+" coordinate = "+coordinates+"\n\n\n");
    }

   public double getDistance(List<Double> point, double[] centroid){
        double distance = 0;
        for(int i = 0; i<point.size(); i++)
            distance+= Math.pow((point.get(i)-centroid[i]), 2);
        return Math.sqrt(distance);
   }

}
