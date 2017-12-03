package football.services;


import football.configs.UserConfig;
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


    public Dataset createDataset() {
        JavaRDD<String> rdd = sc.textFile("data/football/rawData.txt");
        rdd = rdd.filter(line->!line.isEmpty());
        JavaRDD<Row> rowRdd = rdd.map(rowsCreator::createRowFromLine);
        List<String> columnNames = userConfig.getColumnNames();
        StructField[] fields = new StructField[columnNames.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = DataTypes.createStructField(columnNames.get(i), DataTypes.StringType, true);
        }
        Dataset dataset = sqlContext.
                createDataFrame(rowRdd,
                        DataTypes.createStructType(fields));
        return dataset;

    }

}
