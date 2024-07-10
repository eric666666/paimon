package org.apache.paimon.flink.sink.cdc;

/**
 * Table selector prefer from record.
 *
 * @author <a href="mailto:zengbao@shopline.com">zengbao</a>
 * @since 2024/7/4 20:49
 */
@Deprecated
public class TableSelector {
    //public static FileStoreTable getTable(Map<Identifier, FileStoreTable> tables, Identifier tableId, Table externalTable, Catalog.Loader catalogLoader) throws Exception {
    //    if (tables.containsKey(tableId)) {
    //        return tables.get(tableId);
    //    } else {
    //        if (externalTable != null) {
    //            return tables.computeIfAbsent(tableId, id -> (FileStoreTable) externalTable);
    //        } else {
    //            try (Catalog catalog = catalogLoader.load()) {
    //                FileStoreTable table = (FileStoreTable) catalog.getTable(tableId);
    //                tables.put(tableId, table);
    //                return table;
    //            }
    //        }
    //    }
    //}
    //
    //public static FileStoreTable getTable(Map<Identifier, FileStoreTable> tables, Identifier tableId, CdcMultiplexRecord record, Catalog.Loader catalogLoader) throws Exception {
    //    return getTable(tables, tableId, record.record().getTable(), catalogLoader);
    //}
    //
    //public static FileStoreTable getTable(Map<Identifier, FileStoreTable> tables, Identifier tableId, StpCdcRecord raw, Catalog.Loader catalogLoader) throws Exception {
    //    return getTable(tables, tableId, raw.getTable(), catalogLoader);
    //}
}
