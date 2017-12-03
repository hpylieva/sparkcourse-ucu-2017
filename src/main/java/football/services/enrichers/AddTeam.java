package football.services.enrichers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;

@Service
public class AddTeam implements DataEnricher {
    @Override
    public Dataset addColumns(Dataset dataset) {
        return dataset.withColumn("testColumn",
                functions.lit("testMessage"));
    }
}
