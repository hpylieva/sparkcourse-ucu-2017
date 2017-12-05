package football.services.validators;

import org.apache.spark.sql.Dataset;
import org.springframework.stereotype.Service;

@Service
public class PlayerExistsValidator implements DataValidator {
    @Override
    public Dataset validate(Dataset dataset) {
        return null;
    }
}
