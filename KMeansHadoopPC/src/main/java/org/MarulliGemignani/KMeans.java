package org.MarulliGemignani;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.python.modules._threading._threading;

import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

public class KMeans {

    private Configuration conf;
    private Job job;

    private ArrayList<double[]> centroids;
    private double epsilon;
    private String input,output;

    private int k, nb_dimensions,n_iter;

    private Random random;
    private Logger logger =Logger.getLogger(KMeans.class);

    public static DecimalFormat dFormater= new DecimalFormat("0.000");

    private FileSystem fs;


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        KMeans pkMeans = new KMeans(
                args[0],
                args[1],
                Integer.parseInt(args[2]),
                Integer.parseInt(args[3]),
                Double.parseDouble(args[4]));
        pkMeans.run();
    }

    private void run() throws IOException, ClassNotFoundException, InterruptedException {

        do{
            centroids.clear();
            centroids.addAll(UpdateCentroids.centroids);
            ClusterAssignment.centroids.clear();
            ClusterAssignment.centroids.addAll(centroids);
            UpdateCentroids.centroids.clear();
            UpdateCentroids.centroids.addAll(centroids);

            initJob();
            deleteOutputDirIfExists();

            if(!job.waitForCompletion(true))
                logger.info("job failed");
            n_iter++;

            logger.info("\n#iter = "+n_iter+"\n");
            logger.info("\tOld_centers: "+ centroidsToString(centroids)+"\n");
            logger.info("\tNew_centers: "+ centroidsToString(UpdateCentroids.centroids)+"\n");

        }while(compare(centroids,UpdateCentroids.centroids));
        logger.info("Finished in "+ n_iter + " iterations");
    }

    private boolean compare(List<double[]> old_c, List<double[]> new_c) {
        double sum=0;
        for(int i=0;i<old_c.size();i++)
            sum+=this.getDistance(old_c.get(i),new_c.get(i));

        logger.info("SSE: "+sum);
        return sum>this.epsilon;
    }

    public double getDistance(double[] a, double[] b){
        double distance = 0;
        for(int i = 0; i<a.length; i++)
            distance+= Math.pow((a[i]-b[i]), 2);
        return distance;
    }


    private String centroidsToString(List<double[]> centroids) {
        StringBuilder sb= new StringBuilder();
        for(double[] c : centroids){
            sb.append(centroidToString(c)+"\t");
        }
        return sb.toString();
    }

    private String centroidToString(double[] c) {
        StringBuilder sb= new StringBuilder();
        for(int i=0;i<c.length;i++){
            sb.append(c[i]+" ");
        }
        return sb.toString();
    }

    private void deleteOutputDirIfExists() throws IOException {
        fs.delete(new Path(this.output),true);
    }

    private void initJob() throws IOException {
        conf = new Configuration();
        fs= FileSystem.get(conf);
        job= Job.getInstance(conf,"KMeans");
        job.setJarByClass(KMeans.class);

        job.setMapperClass(ClusterAssignment.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(UpdateCentroids.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(this.input));
        FileOutputFormat.setOutputPath(job, new Path(this.output));

    }

    public KMeans (String input, String output,int k, int nb_dimensions, double epsilon){
        this.k=k;
        this.nb_dimensions=nb_dimensions;
        this.epsilon=epsilon;
        this.input=input;
        this.output=output;

        this.random=new Random(42);
        this.centroids=new ArrayList<double[]>();

        this.n_iter=1;
        this.initCentroids();

        UpdateCentroids.centroids.addAll(centroids);
        ClusterAssignment.centroids.addAll(centroids);
        Combine.nb_dimension=this.nb_dimensions;
        UpdateCentroids.nb_dimension=this.nb_dimensions;


    }

    private void initCentroids() {
        for(int i=0;i<this.k;i++){
            double[] center= new double[this.nb_dimensions];
            for(int j=0;j<this.nb_dimensions;j++)
                center[j]=Math.floor( -1000+2000*random.nextDouble())/100 ;
            this.centroids.add(center);
        }
    }

}