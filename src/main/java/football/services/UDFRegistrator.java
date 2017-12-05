package football.services;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

@Service
public class UDFRegistrator {

    @Autowired
    private List<CustomUDF> customUDFs;

    @Autowired
    private SQLContext sqlContext;

    @PostConstruct
    public void registerCustomUDFs() {
        for (CustomUDF udf : customUDFs) {
            sqlContext.udf().register(udf.UDFname(), udf, DataTypes.StringType);
        }
    }
}
