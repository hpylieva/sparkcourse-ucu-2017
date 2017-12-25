package football.services;


import football.configs.UserConfig;
import football.services.enrichers.DataEnricher;
import football.services.validators.DataValidator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.List;

import static football.constants.ConstDataPath.DATA_PATH;

@Service
public class DatasetCreator implements Serializable {
    @Autowired
    private transient SQLContext sqlContext;

    @Autowired
    private transient JavaSparkContext sc;

    @Autowired
    private RowsCreator rowsCreator;

    @Autowired
    private UserConfig userConfig;

    @Autowired
    private List<DataValidator> validators;

    @Autowired
    private List<DataEnricher> enrichers;

    /* Here I decided not to use @ShowDataSetInTheEnd because I need to review dataset in both modes - DEV and PROD
        so I just added dataset.show() */

    public Dataset createDataset() {
        JavaRDD<String> rdd = sc.textFile(DATA_PATH);
        rdd = rdd.filter(line->!line.isEmpty()); // filtering empty rows

        JavaRDD<Row> rowRdd = rdd.map(rowsCreator::createRowFromLine);
        List<String> columnNames = userConfig.getColumnNames();

        StructField[] fields = new StructField[columnNames.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = DataTypes.createStructField(columnNames.get(i), DataTypes.StringType, true);
        }
        Dataset dataset = sqlContext.
                createDataFrame(rowRdd,
                        DataTypes.createStructType(fields));

        dataset.show();

        return dataset;

    }

 /*   @SneakyThrows
    public Dataset createDataset(String data) {
        FileUtils.writeStringToFile(new File(TEST_PATH), data, String.valueOf(forName("UTF-8")),true);

        JavaRDD<String> rdd = sc.textFile(TEST_PATH);
        rdd = rdd.filter(line->!line.isEmpty()); // filtering empty rows

        JavaRDD<Row> rowRdd = rdd.map(rowsCreator::createRowFromLine);
        List<String> columnNames = userConfig.getColumnNames();

        StructField[] fields = new StructField[columnNames.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = DataTypes.createStructField(columnNames.get(i), DataTypes.StringType, true);
        }
        Dataset dataset = sqlContext.
                createDataFrame(rowRdd,
                        DataTypes.createStructType(fields));

        dataset.show();

        return dataset;

    }
*/
}
