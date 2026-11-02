-- One Hive-registered Hudi table per statement.
RUN COMPACTION ON sales.orders_mor;
RUN COMPACTION ON sales.customers_mor;
