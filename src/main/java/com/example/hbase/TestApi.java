package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Desription:
 *
 * @ClassName TestApi
 * @Author Zhanyuwei
 * @Date 2020/9/20 5:23 下午
 * @Version 1.0
 **/
public class TestApi {

    private static Connection connection = null;
    private static Admin admin = null;

    static {

        try {
            // 获取配置文件信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","slave01.sfygroup.com,slave02.sfygroup.com,master.sfygroup.com");
            configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

            // 获取管理员对象
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void close(){
        if (admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    public static boolean isTableExist(String tableName) throws IOException {


        // 判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        //// 关闭资源
        //admin.close();

        return exists;
    }

    /**
     * 创建表
     * @param tableName
     * @param cfs
     * @throws IOException
     */
    public static void createTable(String tableName,String... cfs) throws IOException {

        // 判断是否存在列族信息
        if (cfs.length <= 0){
            System.out.println("请设置列族信息");
            return;
        }

        // 判断表是否存在
        if (isTableExist(tableName)){
            System.out.println(tableName + "表已存在");
            return;
        }

        // 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 循环添加列族信息
        for (String cf : cfs) {
            // 创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            // 添加具体的列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        // 创建表
        admin.createTable(hTableDescriptor);
    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {

        // 判断表是否存在
        if (!isTableExist(tableName)){
            System.out.println(tableName+"表不存在！");
            return;
        }

        // 使表下线
        admin.disableTable(TableName.valueOf(tableName));

        // 删除表
        admin.deleteTable(TableName.valueOf(tableName));

    }

    /**
     * 创建命名空间
     * @param nameSpace
     */
    public static void createNameSpace(String nameSpace){
        // 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        try {
            admin.createNamespace(namespaceDescriptor);
        }catch (NamespaceExistException e){
            System.out.println("命名空间已存在！");
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("创建命名空间完成");
    }

    /**
     * 向表插入数据
     * @param tableName
     * @param rowKey
     * @param cf 列族
     * @param cn 列名
     * @param value
     */
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {

        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        // 给put对象赋值
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

        table.put(put);

        table.close();
    }

    public static void main(String[] args) throws IOException {

        //// 判断表是否存在
        //System.out.println(isTableExist("stu5"));
        //
        //// 创建表测试
        //createTable("stu5","info1","info2");
        //
        //// 删除表测试
        //dropTable("stu5");
        //
        //// 判断表是否存在
        //System.out.println(isTableExist("stu5"));

        //createNameSpace("0920");

        putData("student","1004","info","name","zhangsan4");
        // 关闭资源
        close();
    }
}
