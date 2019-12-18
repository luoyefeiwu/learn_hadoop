package com.jerry.hello.inputformat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 自定义 RecordReader，处理一个文件，将文件读成 一个 key value
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notRead = true;

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;
    FileSplit fs;

    /**
     * 初始化,只调用了一次
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //转换切片类型到文件切片
        fs = (FileSplit) split;
        //通过切片获取路径
        Path path = fs.getPath();
        //通过路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        // 开流
        inputStream = fileSystem.open(path);
    }

    /**
     * 读取下一个key value值
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (notRead) {
            notRead = false;
            //设置key
            key.set(fs.getPath().toString());
            byte[] buf = new byte[(int) fs.getLength()];
            inputStream.read(buf);
            //设置value
            value.set(buf, 0, buf.length);
            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取当前的key
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取当前的value
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 读取读取的进度
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }

    /**
     * 关闭资源
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            IOUtils.closeStream(inputStream);
        }
    }
}
