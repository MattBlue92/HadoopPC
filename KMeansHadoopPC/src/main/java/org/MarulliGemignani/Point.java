package org.MarulliGemignani;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Point implements WritableComparable<Point> {
    private List<DoubleWritable> coordinates;

    private IntWritable index;


    public Point(List<DoubleWritable> coordinates) {
        this.coordinates = new ArrayList<DoubleWritable>();
        for(DoubleWritable x: coordinates)
            this.coordinates.add(x);

        this.index=new IntWritable(99);
    }

    public Point() {

    }

    public void setIndex(IntWritable index) {
        this.index = new IntWritable(index.get());
    }
    public IntWritable getIndex(){
        return index;
    }

    @Override
    public int compareTo(Point point) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.coordinates.size());
        for (DoubleWritable p: coordinates)
            dataOutput.writeDouble(p.get());
        dataOutput.writeInt(index.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int par = dataInput.readInt();
        coordinates = new ArrayList<DoubleWritable>();
        for(int i = 0 ; i<par; i++)
            coordinates.add(new DoubleWritable(dataInput.readDouble()));
        index = new IntWritable(dataInput.readInt());

    }

    public List<DoubleWritable> getCoordinates() {
        return this.coordinates;
    }

    public Double getDistance(Point point) {
        double distance = 0;
        for(int i = 0; i<coordinates.size();i++)
            distance += Math.pow(coordinates.get(i).get() - point.getCoordinates().get(i).get(), 2);
        return Math.sqrt(distance);
    }

    @Override
    public String toString() {
        return " Centroid "+index.get()+" {" +
                "coordinates=" + coordinates +
                '}';
    }
}
