package tech.fulink.canal.config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import tech.fulink.canal.model.MyPropsModel;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;


@Slf4j
@Component
public class SimpleCanalClient extends AbstractCanalClient {

    @Value("${canal.server.hostIp}")
    private String hostIp;
    @Value("${canal.server.port}")
    private int port;
    @Value("${canal.server.destination}")
    private String destination;
    @Value("${canal.server.username}")
    private String username;
    @Value("${canal.server.password}")
    private String password;

    @Value("${canal.server.batchSize}")
    private int batchSize;

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected MyPropsModel myPropsModel;

    public SimpleCanalClient(JdbcTemplate jdbcTemplate,MyPropsModel myPropsModel) {
        super(jdbcTemplate,myPropsModel);
    }

    @PostConstruct
    public void beanContextAware() {
         CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostIp,
                 port), destination, username, password);
         final SimpleCanalClient client = new SimpleCanalClient(jdbcTemplate,myPropsModel);
         client.setConnector(connector);
         client.start();
         Runtime.getRuntime().addShutdownHook(new Thread() {
             public void run() {
                 try {
                     log.info("## stop the canal client");
                     client.stop();
                 } catch (Throwable e) {
                     log.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                 } finally {
                     log.info("## canal client is down.");
                 }
             }
         });
     }


}
