package com.google.hadoop.recordreader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class CustomJsonRecordReader extends RecordReader<LongWritable, CustomMapWritable> {

  private LineRecordReader reader = new LineRecordReader();

  private final Text currentLine_ = new Text();
  private final CustomMapWritable value = new CustomMapWritable();
  private final JSONParser jsonParser = new JSONParser();

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    reader.initialize(split, context);
  }

  @Override
  public synchronized void close() throws IOException {
    reader.close();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return reader.getCurrentKey();
  }

  @Override
  public CustomMapWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return reader.getProgress();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    while (reader.nextKeyValue()) {
      value.clear();
      if (parseStringToJson(jsonParser, reader.getCurrentValue(), value)) {
        return true;
      }
    }
    return false;
  }

  private static boolean parseStringToJson(JSONParser parser, Text line, MapWritable value) {
    try {
      JSONObject jsonObj = (JSONObject) parser.parse(line.toString());
      for (Object key : jsonObj.keySet()) {
        Text mapKey = new Text(key.toString());
        Text mapValue = new Text();
        if (jsonObj.get(key) != null) {
          mapValue.set(jsonObj.get(key).toString());
        }
        value.put(mapKey, mapValue);
      }
      return true;
    } catch (ParseException e) {
      System.out.println("Could not parse json!: " + line + e);
      return false;
    }
  }
}
