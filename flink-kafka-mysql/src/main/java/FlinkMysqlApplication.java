import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.coco.flinkmysql"})
@EnableAutoConfiguration
public class FlinkMysqlApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(FlinkMysqlApplication.class, args);
    }

}
