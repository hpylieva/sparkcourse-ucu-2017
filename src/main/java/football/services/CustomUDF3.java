package football.services;

import org.apache.spark.sql.api.java.UDF3;

public interface CustomUDF3 extends UDF3<String,String,String,String> {
    String UdfName();
}
