package com.google.hadoop.recordreader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CustomJsonInputFormat extends FileInputFormat<LongWritable, CustomMapWritable> {

  @Override
  public RecordReader<LongWritable, CustomMapWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new CustomJsonRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }
}
