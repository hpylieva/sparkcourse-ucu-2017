package football.services.enrichers;

import football.services.CustomUDF;
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

    // Let's suppose that everything that was after 45th minute of the game happened in the second part
    // as we don't have any information about additional time given in each part of the game.
    @SneakyThrows
    @Override
    public String call(String time) {
       return (Integer.parseInt(time.split(":")[0]) <45) ? "first" :  "second";
    }
}
