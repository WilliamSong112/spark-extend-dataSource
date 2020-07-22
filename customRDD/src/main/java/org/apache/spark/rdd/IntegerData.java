package org.apache.spark.rdd;

/**
 * @author yufei.liu
 */
public class IntegerData {

    private int partitionId;

    private int start;

    private int end;

    private int currentIndex;

    public IntegerData(int partitionId, int start, int end) {
        this.partitionId = partitionId;
        this.start = start;
        this.end = end;
        currentIndex = start;
    }

    public boolean hasNext() {
        return currentIndex <= end;
    }

    public int next() throws Exception {
        if (currentIndex <= end) {
            return currentIndex++;
        }
        throw new Exception();
    }

    @Override
    public String toString() {
        return "IntegerData{" +
                "partitionId=" + partitionId +
                ", start=" + start +
                ", end=" + end +
                ", currentIndex=" + currentIndex +
                '}';
    }

}