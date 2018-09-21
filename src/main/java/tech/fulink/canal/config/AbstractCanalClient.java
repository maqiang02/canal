package tech.fulink.canal.config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import tech.fulink.canal.model.MyPropsModel;
import tech.fulink.canal.model.MyPropsModelDatabase;
import tech.fulink.canal.model.MyPropsModelTable;

import java.util.List;

@Slf4j
@Component
public class AbstractCanalClient {
    protected Thread thread = null;

    protected CanalConnector connector;

    protected volatile boolean running = false;

    protected JdbcTemplate jdbcTemplate;

    protected MyPropsModel myPropsModel;

    protected Thread.UncaughtExceptionHandler handler            = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            log.error("parse events has an error", e);
        }
    };
    public AbstractCanalClient(){

    }
    public AbstractCanalClient(JdbcTemplate jdbcTemplate,MyPropsModel myPropsModel){
        this.jdbcTemplate = jdbcTemplate;
        this.myPropsModel = myPropsModel;
    }
    public AbstractCanalClient(CanalConnector connector){
        this.connector = connector;
    }
    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {
            public void run() {
                process();
            }
        });
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }
    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
    protected void process() {
        int batchSize = 5 * 1024;
        while (running) {
            try {
                connector.connect();
                connector.subscribe(myPropsModel.toString());
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId != -1 && size > 0) {
                        System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                        printEntry(message.getEntries());
                    }
                    //log.info(message.toString());
                    connector.ack(batchId); // 提交确认
                    // connector.rollback(batchId); // 处理失败, 回滚数据
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                connector.disconnect();
            }
        }
    }
    private  void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            String dbTable = entry.getHeader().getSchemaName()+"."+entry.getHeader().getTableName();
            String slaveDbTable =getSlaveTable(entry.getHeader().getSchemaName(),entry.getHeader().getTableName());
            if (StringUtils.isEmpty(slaveDbTable)) {
                log.info("slaveDbTable id empty");
            }
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            log.info(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList(),eventType,slaveDbTable);
                    deleteSlave(rowData.getBeforeColumnsList(),eventType,slaveDbTable);
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList(),eventType,slaveDbTable);
                    insertSlave(rowData.getAfterColumnsList(),eventType,slaveDbTable);
                } else {
                    log.info("-------> before");
                    printColumn(rowData.getBeforeColumnsList(),eventType,slaveDbTable);
                    log.info("-------> after");
                    printColumn(rowData.getAfterColumnsList(),eventType,slaveDbTable);
                    updateSlave(rowData.getAfterColumnsList(),eventType,slaveDbTable);

                }
            }
        }
    }

    /**
     * 插入数据到从库表
     * @param columns
     * @param eventType
     * @param dbTable
     */
    public void insertSlave(List<CanalEntry.Column> columns,CanalEntry.EventType eventType,String dbTable){
        StringBuffer sql = new StringBuffer();
        StringBuffer sqlColumn = new StringBuffer();
        StringBuffer sqlValue = new StringBuffer();
        sql.append("INSERT INTO ");
        sql.append(dbTable);
        int i=0;
        sqlColumn.append("(");
        sqlValue.append("(");
        for (CanalEntry.Column column : columns) {
            if(i!=0){
                sqlColumn.append(",") ;
                sqlValue.append(",");
            }
            sqlColumn.append(column.getName());
            sqlValue.append("'");
            sqlValue.append(column.getValue());
            sqlValue.append("'");
            i++;

        }
        sqlColumn.append(")");
        sqlValue.append(")");
        sql.append(sqlColumn);
        sql.append("VALUES");
        sql.append(sqlValue);
        try {
            jdbcTemplate.execute(sql.toString());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新从库表
     * @param columns
     * @param eventType
     * @param dbTable
     */
    public void updateSlave(List<CanalEntry.Column> columns,CanalEntry.EventType eventType,String dbTable){
        StringBuffer sql = new StringBuffer();
        StringBuffer sqlColumn = new StringBuffer();
        StringBuffer sqlCondition = new StringBuffer();
        sql.append("UPDATE ");
        sql.append(dbTable);
        sql.append(" SET ");
        int i=0;
        int j=0;
        for (CanalEntry.Column column : columns) {

            if(column.getUpdated()) {
                if(i!=0){
                    sqlColumn.append(",") ;
                    sqlCondition.append(",");
                }
                sqlColumn.append(column.getName());
                sqlColumn.append("=");
                sqlColumn.append("'");
                sqlColumn.append(column.getValue());
                sqlColumn.append("'");
                i++;
            } else {
                if(j!=0){
                    sqlCondition.append(",");
                }
                sqlCondition.append(column.getName());
                sqlCondition.append("=");
                sqlCondition.append("'");
                sqlCondition.append(column.getValue());
                sqlCondition.append("'");
                j++;
            }
        }
        sql.append(sqlColumn);
        sql.append(" where ");
        sql.append(sqlCondition);
        try {
            jdbcTemplate.execute(sql.toString());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除存库表
     * @param columns
     * @param eventType
     * @param dbTable
     */
    public void deleteSlave(List<CanalEntry.Column> columns,CanalEntry.EventType eventType,String dbTable){
        StringBuffer sql = new StringBuffer();
        sql.append("delete from ");
        sql.append(dbTable);
        for (CanalEntry.Column column : columns) {
            if(column.getIsKey()){
                sql.append(" where ");
                sql.append(column.getName());
                sql.append("=");
                sql.append("'");
                sql.append(column.getValue());
                sql.append("'");
            }
        }
        try {
            jdbcTemplate.execute(sql.toString());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    private  void printColumn(List<CanalEntry.Column> columns,CanalEntry.EventType eventType,String dbTable) {
        for (CanalEntry.Column column : columns) {
            log.info(column.getName() + " : " + column.getValue() + "    "+eventType + column.getUpdated());
        }
    }
    private  String getSlaveTable(String database,String tableName){
        String dbTable="";
        List<MyPropsModelDatabase> databaseList =  myPropsModel.getDatabase();
        for (MyPropsModelDatabase myPropsModelDatabase:databaseList) {
            if(database.equals(myPropsModelDatabase.getDatabaseName())) {
                List<MyPropsModelTable> tableNameList = myPropsModelDatabase.getTableName();
                for (MyPropsModelTable myPropsModelTable:tableNameList) {
                    if (tableName.equals(myPropsModelTable.getName())) {
                        dbTable = myPropsModelTable.getSlaveDatabase()+"."+myPropsModelTable.getSlaveTableName();
                    }
                }

            }
        }

        return dbTable;
    }
    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }
}
