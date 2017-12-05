package football.services.enrichers;

import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
public class GamePartEnricher implements DataEnricher, CustomUDF {
    @Override
    public Dataset addColumn(Dataset dataset) {
        return dataset.withColumn("gamePart", callUDF(UDFname(),col("eventTime")));
    }

    public String UDFname() { return "defineGamePart"; }

    @SneakyThrows
    @Override
    public String call(String time) {
       if( Integer.parseInt(time.split(":")[0]) <45) {
           return "first";
       } else return "second";
    }
}
