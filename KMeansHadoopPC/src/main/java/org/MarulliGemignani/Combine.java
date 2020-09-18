package org.MarulliGemignani;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Combine extends Reducer<LongWritable, Text, LongWritable, Text> {


    public static int nb_dimension;

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double[] sum = new double[nb_dimension];
        int numPoint = 0;

        for(int i = 0; i<nb_dimension;i++)
            sum[i] = 0;

        for(Text record: values){
            String line = record.toString();
            List<Double> coordinates = new ArrayList<Double>();
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
            while(tokenizer.hasMoreTokens())
                coordinates.add(Double.parseDouble(tokenizer.nextToken()));

            for(int i = 0; i<nb_dimension;i++)
                sum[i] += coordinates.get(i);
            numPoint++;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 0; i<nb_dimension;i++) {
            stringBuilder.append(sum[i]);
            stringBuilder.append(",");
        }
        stringBuilder.append(numPoint);
        context.write(key, new Text(stringBuilder.toString()));

    }
}
