package tech.fulink.canal.model;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MyPropsModelDatabase {
    private String databaseName;

    private List<MyPropsModelTable> tableName;

    public List<MyPropsModelTable> getTableName() {
        return tableName;
    }

    public void setTableName(List<MyPropsModelTable> tableName) {
        this.tableName = tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}