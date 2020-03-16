package com.flink.learning.datasource.sources;

import com.flink.learning.datasource.CustomIterator;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @description: 内置Flink Datasources
 * @author: fanyeuxiang
 * @createDate: 2020-03-16
 */
public class InstalledDataSources {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 基于集合
         * 1、fromCollection(Collection) - 从 Java 的 Java.util.Collection 创建数据流。集合中的所有元素类型必须相同。
         *
         * 2、fromCollection(Iterator, Class) - 从一个迭代器中创建数据流。Class 指定了该迭代器返回元素的类型。
         *
         * 3、fromElements(T …) - 从给定的对象序列中创建数据流。所有对象类型必须相同。
         *
         * 4、fromParallelCollection(SplittableIterator, Class) - 从一个迭代器中创建并行数据流。Class 指定了该迭代器返回元素的类型。
         *
         * 5、generateSequence(from, to) - 创建一个生成指定区间范围内的数字序列的并行数据流。
         */
        env.fromCollection(Arrays.asList(1,2,3,4,5)).print();

        env.fromCollection(new CustomIterator(), BasicTypeInfo.INT_TYPE_INFO).print();

        env.fromElements(1,2,3,4,5).print();

        DataSource<Long> integerInput = env.generateSequence(1,100);
        integerInput.print();
        /**
         * 基于文件构建DataSource:
         *  1、readTextFile(path) - 读取文本文件，即符合 TextInputFormat 规范的文件，并将其作为字符串返回。
         *
         *  2.readFile(fileInputFormat, path) - 根据指定的文件输入格式读取文件。
         *
         *  3. readFile(fileInputFormat, path, watchType, interval, pathFilter, typeInfo)
         *  inputFormat：数据流的输入格式。
         *  filePath：文件路径，可以是本地文件系统上的路径，也可以是 HDFS 上的文件路径。
         *  watchType：读取方式，它有两个可选值，分别是 FileProcessingMode.PROCESS_ONCE 和 FileProcessingMode.PROCESS_CONTINUOUSLY：
         *            前者表示对指定路径上的数据只读取一次，然后退出；后者表示对路径进行定期地扫描和读取。
         *            需要注意的是如果 watchType 被设置为 PROCESS_CONTINUOUSLY，那么当文件被修改时，其所有的内容 (包含原有的内容和新增的内容) 都将被重新处理，
         *            因此这会打破 Flink 的 exactly-once 语义。
         *  interval：定期扫描的时间间隔。
         *  typeInformation：输入流中元素的类型。
         */
        DataSet<String> textDataSource = env.readTextFile("D:\\experiment\\flink_learning\\flink-learning-introduction\\src\\main\\java\\com\\flink\\learning\\introduction\\simple_example\\wordcount.txt");

    }
}
