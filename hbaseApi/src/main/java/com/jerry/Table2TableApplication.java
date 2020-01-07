package com.jerry;

import com.jerry.tool.HbaseMapperReduceTool;
import org.apache.hadoop.util.ToolRunner;

public class Table2TableApplication {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new  HbaseMapperReduceTool(),args);
    }
}
