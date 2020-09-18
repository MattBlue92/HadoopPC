package org.MarulliGemignani;

import org.apache.giraph.writable.tuple.PairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;


public class Reduce extends Reducer<IntWritable, PairWritable<Point, Point>,IntWritable, HashMap<Integer,Point>>{
    private Logger logger = Logger.getLogger(Reduce.class);

    public enum FLAG{
        CONVERGED
    }

    @Override
    public void reduce(IntWritable key, Iterable<PairWritable<Point, Point>> values, Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        Path centroidPath = new Path(conf.get("casaDeiCentroidi"));
        SequenceFile.Writer centroidWriter = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(centroidPath),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Point.class));
        double centroidsDistance=0;
        int c=0;
        HashMap<Integer,Point> outPut=new HashMap<Integer,Point>();
        for(PairWritable<Point, Point> couple: values){

                centroidsDistance += Math.pow(couple.getLeft().getDistance(couple.getRight()),2);
                centroidWriter.append(new IntWritable(0),couple.getRight());
                outPut.put(c,couple.getRight());
                c++;
        }
        centroidWriter.close();

        double threshold= conf.getDouble("soglia",0.0001);
        if(threshold>centroidsDistance){
            context.getCounter(FLAG.CONVERGED).increment(1);
        }

        context.write(key,outPut);
        logger.fatal("Converged centroids: " + FLAG.CONVERGED);
    }
}
