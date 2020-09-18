package org.MarulliGemignani;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;



import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class UpdateCentroids extends Reducer<LongWritable, Text, LongWritable, Text> {


    public static int nb_dimension;
    public static List<double[]> centroids = new ArrayList<>();
    public static DecimalFormat dFormater = new DecimalFormat("#.###");

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double[] center = new double[nb_dimension];
        int numPoint = 0;

        for(int i = 0; i<nb_dimension;i++)
            center[i] = 0;

        for(Text record: values){
            String line = record.toString();
            List<Double> coordinates = new ArrayList<Double>();
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
            for(int i = 0; i<nb_dimension;i++)
                center[i] += Double.parseDouble(tokenizer.nextToken());
            numPoint += Integer.parseInt(tokenizer.nextToken());
        }

        for(int i = 0; i<nb_dimension;i++)
            center[i] /= numPoint;

        StringBuilder center_sb = new StringBuilder();
        for(int i = 0; i< nb_dimension;i++){
            center_sb.append(dFormater.format(center[i]));
            if(i< nb_dimension-1)
                center_sb.append(" ");
        }
        centroids.set((int)key.get(), center);
        context.write(key, new Text(center_sb.toString()));
    }
}
