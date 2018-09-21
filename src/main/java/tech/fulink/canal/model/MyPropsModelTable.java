package tech.fulink.canal.model;

import org.springframework.stereotype.Component;

@Component
public class MyPropsModelTable {
    private String name;

    private String slaveDatabase;

    private String slaveTableName;

    public String getSlaveTableName() {
        return slaveTableName;
    }

    public void setSlaveTableName(String slaveTableName) {
        this.slaveTableName = slaveTableName;
    }

    public String getSlaveDatabase() {
        return slaveDatabase;
    }

    public void setSlaveDatabase(String slaveDatabase) {
        this.slaveDatabase = slaveDatabase;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}