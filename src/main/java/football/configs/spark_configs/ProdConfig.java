package football.configs.spark_configs;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import static football.configs.spark_configs.ConstProfiles.PROD;

@Profile(PROD)
@Configuration

public class ProdConfig {
    @Bean
    public SparkConf sparkConf(){
        return new SparkConf().setMaster("local[*]").
                setAppName("football");
    }
}
