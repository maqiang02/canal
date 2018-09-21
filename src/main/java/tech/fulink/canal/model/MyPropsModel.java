package tech.fulink.canal.model;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix="canal.inputdata")
public class MyPropsModel {
    private List<MyPropsModelDatabase> database;

    public List<MyPropsModelDatabase> getDatabase() {
        return database;
    }

    public void setDatabase(List<MyPropsModelDatabase> database) {
        this.database = database;
    }

    @Override
    public String toString(){
        StringBuffer  myPropsModelToString= new StringBuffer();
        if(null!=database&&database.size()>0){
            int i=0;
            for (MyPropsModelDatabase myPropsModelDatabase:database) {
                String databaseName = myPropsModelDatabase.getDatabaseName();

                for (MyPropsModelTable myPropsModelTable:myPropsModelDatabase.getTableName()) {
                    if(i!=0){
                        myPropsModelToString.append(",");
                    }
                    myPropsModelToString.append(databaseName);
                    myPropsModelToString.append(".");
                    myPropsModelToString.append(myPropsModelTable.getName());
                    i++;
                }

            }
        }
        System.out.print(myPropsModelToString.toString());
        return myPropsModelToString.toString();
    }
}