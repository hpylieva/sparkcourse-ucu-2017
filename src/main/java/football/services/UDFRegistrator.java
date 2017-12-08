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
    private List<CustomUDF1> customUDF1s;

    @Autowired
    private List<CustomUDF3> customUDF3s;

    @Autowired
    private SQLContext sqlContext;

    @PostConstruct
    public void registerCustomUDFs() {
        for (CustomUDF1 udf : customUDF1s) {
            sqlContext.udf().register(udf.UdfName(), udf, DataTypes.StringType);
        }
        for (CustomUDF3 udf : customUDF3s) {
            sqlContext.udf().register(udf.UdfName(), udf, DataTypes.StringType);
        }
    }
}
