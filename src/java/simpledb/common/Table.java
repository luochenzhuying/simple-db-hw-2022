package simpledb.common;

import lombok.Data;
import simpledb.storage.DbFile;

@Data
public class Table {
    private DbFile file;
    private String name;
    private String pkeyField;


    public Table(DbFile file, String name, String pkeyField) {
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }

    public Table(DbFile file, String name) {
        this(file, name, "");
    }

}
