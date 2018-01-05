package com.from.data.utils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * Created by Administrator on 2018/1/5.
 */
public class Utils {
    /**
     * 生成表schema
     * @param columns
     * @return
     */
    public static StructType initSchema(String...columns){
        Map<String,String> map = new LinkedHashMap<String, String>();
        for(String str:columns){
            map.put(str,null);
        }
        return initSchema(map);
    }
    /**
     * 生成表schema
     * @param tableMap
     * @return
     */
    public static StructType initSchema(Map<String, String> tableMap){
        Set<String> tables = tableMap.keySet();
        List<StructField> fields = new ArrayList<StructField>();
        for(String table :tables){
            StructField field = DataTypes.createStructField(table, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }

    public static String getUUID(int length) {
        int hashCodeV = UUID.randomUUID().toString().hashCode();
        if(hashCodeV < 0) {//有可能是负数
            hashCodeV = - hashCodeV;
        }
        // 0 代表前面补充0
        // 4 代表长度为4
        // d 代表参数为正数型
        return  String.format("%0"+length+"d", hashCodeV);
    }

    public static void main(String[] args) {
        System.out.println(getUUID(8));
    }

}
