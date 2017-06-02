# hadoopLearn
hadoop learning code
these codes can be run in windows,before run it,you should    
1 configure HADOOP_HOME    
2 add corresponding version hadoop.dll and winutils.exe to HADOOP_HOME\bin   
3 configurage input and output arguments

when run the code, the following problems arise,   
Exception in thread "main" java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Ljava/lang/String;I)Z
you should add hadoop.dll to C：\windows\system32
