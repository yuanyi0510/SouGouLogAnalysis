package com.hadoop.mapreduce.counturl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;

public class SortUrlComparator implements RawComparator {
    private static final LongWritable.Comparator LONGWRITABLE_COMPARATOR = new LongWritable.Comparator();

    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        return -LONGWRITABLE_COMPARATOR.compare(bytes, i, i1, bytes1, i2, i3);
    }

    public int compare(Object o1, Object o2) {
        return -LONGWRITABLE_COMPARATOR.compare(o1, o2);
    }
}
