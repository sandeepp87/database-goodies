/*
 * Copyright 2018 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.susom.dbgoodies.etl;

import com.github.susom.database.ConfigFrom;
import com.github.susom.database.Config;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DatabaseProvider.Builder;
import com.github.susom.database.Metric;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple example of how to copy all the tables from source to destination. The source
 * and the target are different databases.
 */
public class EtlCopyTables {
  private static final Logger log = LoggerFactory.getLogger(EtlCopyTables.class);

  // List of tables to be copied
  private static List<String> tables = null;
  // Table index in the table list
  private static int i = 0;
  private static final String SCHEMA_TABLES = "schema.tables";
  private static final String DELIMITER = ",";

  private static List<String> createTableList(String tableNamesFromConfig) {
    String[] tempTableNamesArray = tableNamesFromConfig.split(DELIMITER);
    tables = new ArrayList<String>(Arrays.asList(tempTableNamesArray));
    return tables;
  }

  public static void main(String[] args) {
    Metric metric = new Metric(true);
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Date date = new Date();
    System.out.println("Migration Started at: "+ dateFormat.format(date));
    String propertiesFile = System.getProperty("local.properties", "local.properties");
    Config config = Config.from().systemProperties().propertyFile(propertiesFile).get();
    // We use postgres here because hsqldb doesn't seem to handle the
    // volume of data well. If you want to use hsqldb, reduce j below.
    String oracleDbUrl = config.getString("oracle.database.url");
    String oracleDbUser = config.getString("oracle.database.user");
    String oracleDbPassword = config.getString("oracle.database.password");
    Builder dbb = DatabaseProvider.fromDriverManager(oracleDbUrl, oracleDbUser, oracleDbPassword)
        // The ETL utilities do incremental commits in order to handle large tables
        .withTransactionControl();

    String tableNamesFromConfig = config.getString(SCHEMA_TABLES);
    if(tableNamesFromConfig == null) {
      String message = "Please mention the schema tables property.";
      System.out.println(message);
      log.info(message);
      return;
    }
    // Get all tables from the properties (first parent tables, then child tables)
    tables = createTableList(tableNamesFromConfig);

    log.info("Number of tables to copy:" + tables.size());
    System.out.println("Number of tables to copy:" + tables.size());
    String postgresDbUrl = config.getString("postgres.database.url");
    String postgresDbUser = config.getString("postgres.database.user");
    String postgresDbPassword = config.getString("postgres.database.password");
    Builder dbb2 = DatabaseProvider.fromDriverManager(postgresDbUrl, postgresDbUser, postgresDbPassword)
        // The ETL utilities do incremental commits in order to handle large tables
        .withTransactionControl();

    // iterate over all the "tables" and copy to the target database
    for (i = 0; i < tables.size(); i++) {
	Date tableStartDate = new Date();
	    System.out.println("Table "+ tables.get(i) + " started at: "+ dateFormat.format(tableStartDate));
        dbb2.transact(dbs2 ->
            dbb.transact(dbs ->
                Etl.saveQuery(dbs.get().toSelect(String.format("select * from %s", tables.get(i))))
                    .asTable(dbs2, tables.get(i)).batchSize(2)
                    .start()
            )
        );
       Date tableEndDate = new Date();
       System.out.println("Table "+ tables.get(i) + " ended at: "+ dateFormat.format(tableEndDate));
       System.out.println("-----------------------------------------------");
    }
    Date endDate= new Date();
    System.out.println("Migration Ended at: "+ dateFormat.format(endDate));
    metric.checkpoint("copy");

    // Check the results
    dbb2.transact(dbs ->
        log.info("Tables copied to destination: " + dbs.get()
            .toSelect("select count(*) from information_schema.tables where table_schema='public'")
            .queryIntegerOrZero() + " in " + metric.getMessage())
    );
  }
}
