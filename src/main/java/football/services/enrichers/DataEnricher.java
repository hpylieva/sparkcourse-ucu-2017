package football.services.enrichers;

import org.apache.spark.sql.Dataset;

public interface DataEnricher {
    Dataset addColumn(Dataset dataset);
}
