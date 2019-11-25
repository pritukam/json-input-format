package com.google.hadoop.recordreader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.mockito.Mockito.mock;

public class CustomJsonRecordReaderTest {

  private TaskAttemptContext TASK_ATTEMPT_CONTEXT;
  private JobContext JOB_CONTEXT;
  Configuration CONF;
  private final String JSON_INPUT = "/home/pritishkamath/Desktop/input-reader-testing/MOCK_DATA.json";
  private final String CSV_INPUT = "/home/pritishkamath/Desktop/input-reader-testing/data.csv";
  private final String MIXED_INPUT = "/home/pritishkamath/Desktop/input-reader-testing/data-mixed.csv";
  private final String ID64_EXPECTED = "{\"last_name\":Grey,\"id\":64,\"first_name\":Rica,\"email\":rgrey1r@paypal.com}";

  @Before
  public void setup() {
    CONF = new Configuration();
    CONF.set("mapred.JSON_INPUT.dir", "file:///home/pritishkamath/Downloads/MOCK_DATA.json");

    TASK_ATTEMPT_CONTEXT = new TaskAttemptContextImpl(CONF, mock(TaskAttemptID.class));
    JOB_CONTEXT = new JobContextImpl(CONF, TASK_ATTEMPT_CONTEXT.getJobID());
  }



  @Test
  public void testReaderSuccess() throws IOException, InterruptedException {
    File testFile = new File(JSON_INPUT);
    Path inputPath = new Path(testFile.getAbsoluteFile().toURI());

    FileSplit split = new FileSplit(inputPath, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(CustomJsonInputFormat.class, CONF);
    TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(CONF, new TaskAttemptID());

    RecordReader reader = inputFormat.createRecordReader(split, taskAttemptContext);

    reader.initialize(split, taskAttemptContext);
    int count = 0;
    LongWritable currentKeyValue = new LongWritable();
    String id64Actual = "";
    while (reader.nextKeyValue()) {
      count++;
      if (reader.getCurrentValue().toString().replaceAll(" ", "").contains("\"id\":64,")) {
        id64Actual = reader.getCurrentValue().toString();
        currentKeyValue = new LongWritable(Long.parseLong(reader.getCurrentKey().toString()));
      }
    }

    Assert.assertEquals(0, Double.compare(reader.getProgress(), 1.0));
    Assert.assertEquals(1000, count);
    Assert.assertEquals(ID64_EXPECTED, id64Actual);
    Assert.assertNotNull(currentKeyValue);

    reader.close();
  }

  @Test
  public void testReaderFailing() throws IOException, InterruptedException{
    File testFile = new File(CSV_INPUT);
    Path inputPath = new Path(testFile.getAbsoluteFile().toURI());

    FileSplit split = new FileSplit(inputPath, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(CustomJsonInputFormat.class, CONF);
    TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(CONF, new TaskAttemptID());

    RecordReader reader = inputFormat.createRecordReader(split, taskAttemptContext);

    reader.initialize(split, taskAttemptContext);
    int count = 0;
    String id64Actual = "";
    while (reader.nextKeyValue()) {
      count++;
      if (reader.getCurrentValue().toString().replaceAll(" ", "").contains("\"id\":64,")) {
        id64Actual = reader.getCurrentValue().toString();
      }
    }
    Assert.assertEquals(0, count);
    Assert.assertEquals("", id64Actual);
    Assert.assertEquals(0, Double.compare(reader.getProgress(), 1.0));

    reader.close();
  }

  @Test
  public void testReaderPartialParsing() throws IOException, InterruptedException{
    File testFile = new File(MIXED_INPUT);
    Path inputPath = new Path(testFile.getAbsoluteFile().toURI());

    FileSplit split = new FileSplit(inputPath, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(CustomJsonInputFormat.class, CONF);
    TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(CONF, new TaskAttemptID());

    RecordReader reader = inputFormat.createRecordReader(split, taskAttemptContext);

    reader.initialize(split, taskAttemptContext);
    int count = 0;
    String id64Actual = "";
    while (reader.nextKeyValue()) {
      count++;
      if (reader.getCurrentValue().toString().replaceAll(" ", "").contains("\"id\":64,")) {
        id64Actual = reader.getCurrentValue().toString();
      }
    }
    Assert.assertEquals(16, count);
    Assert.assertEquals(ID64_EXPECTED, id64Actual);
    Assert.assertEquals(0, Double.compare(reader.getProgress(), 1.0));

    reader.close();
  }
}
