package football.services.enrichers;

import org.apache.spark.sql.api.java.UDF1;

public interface CustomUDF extends UDF1<String,String> {
    String UDFname();
}
