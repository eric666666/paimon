package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.Map;

/**
 * Table selector prefer from record.
 *
 * @author <a href="mailto:zengbao@shopline.com">zengbao</a>
 * @since 2024/7/4 20:49
 */
public class TableSelector {
    public static FileStoreTable getTable(Map<Identifier, FileStoreTable> tables, Identifier tableId, Table externalTable, Catalog.Loader catalogLoader) throws Exception {
        if (tables.containsKey(tableId)) {
            return tables.get(tableId);
        } else {
            if (externalTable != null) {
                return tables.computeIfAbsent(tableId, id -> (FileStoreTable) externalTable);
            } else {
                try (Catalog catalog = catalogLoader.load()) {
                    FileStoreTable table = (FileStoreTable) catalog.getTable(tableId);
                    tables.put(tableId, table);
                    return table;
                }
            }
        }
    }

    public static FileStoreTable getTable(Map<Identifier, FileStoreTable> tables, Identifier tableId, CdcMultiplexRecord record, Catalog.Loader catalogLoader) throws Exception {
        return getTable(tables, tableId, record.record().getTable(), catalogLoader);
    }

    public static FileStoreTable getTable(Map<Identifier, FileStoreTable> tables, Identifier tableId, StpCdcRecord raw, Catalog.Loader catalogLoader) throws Exception {
        return getTable(tables, tableId, raw.getTable(), catalogLoader);
    }

    public static FileStoreTable getTable(Map<Identifier, FileStoreTable> tables, Identifier tableId, Table externalTable, Catalog catalog) throws Exception {
        if (tables.containsKey(tableId)) {
            return tables.get(tableId);
        } else {
            if (externalTable != null) {
                return tables.computeIfAbsent(tableId, id -> (FileStoreTable) externalTable);
            } else {
                FileStoreTable table = (FileStoreTable) catalog.getTable(tableId);
                tables.put(tableId, table);
                return table;
            }
        }
    }

    public static FileStoreTable getTable(Map<Identifier, FileStoreTable> tables, Identifier tableId, CdcMultiplexRecord record, Catalog catalog) throws Exception {
        return getTable(tables, tableId, record.record().getTable(), catalog);
    }

    public static FileStoreTable getTable(Map<Identifier, FileStoreTable> tables, Identifier tableId, StpCdcRecord raw, Catalog catalog) throws Exception {
        return getTable(tables, tableId, raw.getTable(), catalog);
    }
}
