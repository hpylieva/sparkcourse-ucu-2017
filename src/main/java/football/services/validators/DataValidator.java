package football.services.validators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;

public interface DataValidator {
    Dataset validate(Dataset dataset);
}
