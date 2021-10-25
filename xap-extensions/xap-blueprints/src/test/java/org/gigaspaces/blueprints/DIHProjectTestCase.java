package org.gigaspaces.blueprints;

import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.start.SystemLocations;
import org.gigaspaces.blueprints.java.DocumentInfo;
import org.gigaspaces.blueprints.java.dih.DIHProjectGenerator;
import org.gigaspaces.blueprints.java.dih.DIHProjectPropertiesOverrides;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class DIHProjectTestCase {

    @Test
    void generateProject() {
        String projectPipelineName = "Foo";
        String projectVersion = "1.0";
        String kafkaSpaceName = "mySpace";
        String kafkaTopic = "myTopic";
        String resourcesTypeMetadataJson = getTypeMetadataJson();
        String resourcesDefaultTypeConversionMap = getDefaultTypeConversionMap();
        String configStreamJson = getStreamJson();
        Path target = SystemLocations.singleton().work("data-integration");
        List<DocumentInfo> documents = getDocumentsInfo();

        DIHProjectPropertiesOverrides dihProjectProperties = new DIHProjectPropertiesOverrides(
                projectPipelineName, projectVersion, null,
                null, null, null,
                kafkaSpaceName, null, kafkaTopic,
                null, null, resourcesTypeMetadataJson,
                resourcesDefaultTypeConversionMap, configStreamJson, target, documents);

        DIHProjectGenerator.generate(dihProjectProperties);
    }

    @NotNull
    private List<DocumentInfo> getDocumentsInfo() {
        DocumentInfo employeeDocumentInfo = new DocumentInfo("Employee", "com.gigaspaces.dih.model.types",
                "companyDb_companySchema_Employee", false, true);
        employeeDocumentInfo.addIdProperty("employeeId", Long.class, SpaceIndexType.EQUAL, false);
        employeeDocumentInfo.addIndexProperty("name", String.class, SpaceIndexType.EQUAL, false);
        employeeDocumentInfo.addRoutingProperty("age", Integer.class, SpaceIndexType.EQUAL_AND_ORDERED);

        DocumentInfo studentDocumentInfo = new DocumentInfo("Student", "com.gigaspaces.dih.model.types",
                "Student", false, true);
        studentDocumentInfo.addIdProperty("studentId", Long.class, SpaceIndexType.EQUAL, false);
        studentDocumentInfo.addIndexProperty("name", String.class, SpaceIndexType.EQUAL, false);
        studentDocumentInfo.addRoutingProperty("age", Integer.class, SpaceIndexType.EQUAL_AND_ORDERED);

        return Arrays.asList(employeeDocumentInfo, studentDocumentInfo);
    }

    private static String getTypeMetadataJson() {
        return "[\n" +
                "  {\n" +
                "    \"typeName\": \"companyDb_companySchema_Employee\",\n" +
                "    \"fields\": [\n" +
                "      {\n" +
                "        \"fieldName\": \"employeeId\",\n" +
                "        \"originalType\": \"INTEGER\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"fieldName\": \"name\",\n" +
                "        \"originalType\": \"NVARCHAR\"\n" +
                "      },{\n" +
                "        \"fieldName\": \"age\",\n" +
                "        \"originalType\": \"TINYINT\"\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"typeName\": \"companyDb_companySchemaOverride_Employee\",\n" +
                "    \"fields\": [\n" +
                "      {\n" +
                "        \"fieldName\": \"employeeId\",\n" +
                "        \"originalType\": \"INTEGER\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"fieldName\": \"name\",\n" +
                "        \"originalType\": \"NVARCHAR\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"fieldName\": \"age\",\n" +
                "        \"originalType\": \"SMALLINT\"\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"typeName\": \"app_db_dbo_COMPANY\",\n" +
                "    \"fields\": [\n" +
                "      {\n" +
                "        \"fieldName\": \"ID\",\n" +
                "        \"originalType\": \"INTEGER\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"fieldName\": \"AGE\",\n" +
                "        \"originalType\": \"INTEGER\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"fieldName\": \"NAME\",\n" +
                "        \"originalType\": \"VARCHAR2\"\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "]";
    }

    private static String getDefaultTypeConversionMap() {
        return "VARCHAR2:com.gigaspaces.dih.type_converter.VARCHAR2ToString\n" +
                "NVARCHAR2:com.gigaspaces.dih.type_converter.NVARCHAR2ToString\n" +
                "NCHAR_VARYING:com.gigaspaces.dih.type_converter.NCHAR_VARYINGToString\n" +
                "VARCHAR:com.gigaspaces.dih.type_converter.VarcharToString\n" +
                "CHAR:com.gigaspaces.dih.type_converter.CharToString\n" +
                "NCHAR:com.gigaspaces.dih.type_converter.NCHARToString\n" +
                "NVARCHAR:com.gigaspaces.dih.type_converter.NVARCHARToString\n" +
                "SYSNAME:com.gigaspaces.dih.type_converter.SYSNAMEToString\n" +
                "CLOB:com.gigaspaces.dih.type_converter.CLOBToString\n" +
                "RAW:com.gigaspaces.dih.type_converter.RAWToString\n" +
                "MONEY:com.gigaspaces.dih.type_converter.MONEYToString\n" +
                "SMALLMONEY:com.gigaspaces.dih.type_converter.SMALLMONEYToString\n" +
                "TEXT:com.gigaspaces.dih.type_converter.TEXTToString\n" +
                "NTEXT:com.gigaspaces.dih.type_converter.NTEXTToString\n" +
                "GRAPHIC:com.gigaspaces.dih.type_converter.GRAPHICToString\n" +
                "VARGRAPHIC:com.gigaspaces.dih.type_converter.VARGRAPHICToString\n" +
                "VARG:com.gigaspaces.dih.type_converter.VARGToString\n" +
                "VARBINARY:com.gigaspaces.dih.type_converter.VARBINARYToString\n" +
                "VARBIN:com.gigaspaces.dih.type_converter.VARBINToString\n" +
                "CHARACTER:com.gigaspaces.dih.type_converter.CHARACTERToString\n" +
                "UNIQUEIDENTIFIER:com.gigaspaces.dih.type_converter.UNIQUEIDENTIFIERToString\n" +
                "DECFLOAT:com.gigaspaces.dih.type_converter.DECFLOATToString\n" +
                "LONG:com.gigaspaces.dih.type_converter.LONGToString\n" +
                "TINYINT:com.gigaspaces.dih.type_converter.TINYINTtoShort\n" +
                "SMALLINT:com.gigaspaces.dih.type_converter.SMALLINTToShort\n" +
                "INT:com.gigaspaces.dih.type_converter.INTToInt\n" +
                "INTEGER:com.gigaspaces.dih.type_converter.INTEGERToInt\n" +
                "BIGINT:com.gigaspaces.dih.type_converter.BIGINTToBigInteger\n" +
                "NUMBER:com.gigaspaces.dih.type_converter.NUMBERToDouble\n" +
                "DOUBLE:com.gigaspaces.dih.type_converter.DOUBLEToDouble\n" +
                "DOUBLE_PRECISION:com.gigaspaces.dih.type_converter.DOUBLE_PRECISIONToDouble\n" +
                "NUMERIC:com.gigaspaces.dih.type_converter.NUMERICToBigDecimal\n" +
                "DECIMAL:com.gigaspaces.dih.type_converter.DECIMALToBigDecimal\n" +
                "BOOLEAN:com.gigaspaces.dih.type_converter.BOOLEANToBoolean\n" +
                "REAL:com.gigaspaces.dih.type_converter.REALToFloat\n" +
                "FLOAT:com.gigaspaces.dih.type_converter.FLOATToFloat\n" +
                "TIMESTAMP:com.gigaspaces.dih.type_converter.TIMESTAMPToTimestamp\n" +
                "TIMESTAMP_WITH_TIME_ZONE:com.gigaspaces.dih.type_converter.TIMESTAMP_WITH_TIME_ZONEToTimestamp\n" +
                "TIMESTAMP_WITH_LOCAL_TIME_ZONE:com.gigaspaces.dih.type_converter.TIMESTAMP_WITH_LOCAL_TIME_ZONEToTimestamp\n" +
                "TIMESTMP:com.gigaspaces.dih.type_converter.TIMESTMPToTimestamp\n" +
                "TIMESTZ:com.gigaspaces.dih.type_converter.TIMESTZToTimestamp\n" +
                "TIME:com.gigaspaces.dih.type_converter.TIMEToTime\n" +
                "TIME_WITH_TIME_ZONE:com.gigaspaces.dih.type_converter.TIME_WITH_TIME_ZONEToTime\n" +
                "DATE:com.gigaspaces.dih.type_converter.DATEToDate\n" +
                "DATETIME:com.gigaspaces.dih.type_converter.DATETIMEToDate";
    }

    private static String getStreamJson() {
        return "{\n" +
                "  \"name\": \"{{pipelineName}}\",\n" +
                "  \"userDefinedName\": \"MSSQL to Kafka test stream 1\",\n" +
                "  \"lastModified\": \"\",\n" +
                "  \"allowNoSync\": \"false\",\n" +
                "  \"environment\": \"\",\n" +
                "  \"automaticStart\": \"true\",\n" +
                "  \"streamType\": \"normal\",\n" +
                "  \"startFromLastApplied\": \"true\",\n" +
                "  \"basicProperties\": {\n" +
                "    \"source\": {\n" +
                "      \"dBType\": \"{{source.dBType}}\",\n" +
                "      \"host\": \"{{source.host}}\",\n" +
                "      \"port\": \"{{source.port}}\",\n" +
                "      \"serviceName\": \"{{source.serviceName}}\",\n" +
                "      \"oracleDefaultPDB\": \"\",\n" +
                "      \"archiveDirectory\": \"\",\n" +
                "      \"username\": \"{{username}}\",\n" +
                "      \"password\": \"{{password}}\",\n" +
                "      \"adminSchemaName\": \"{{source.adminSchemaName}}\",\n" +
                "      \"adminSchemaPassword\": \"{{source.adminSchemaPassword}}\",\n" +
                "      \"ii_system\": \"\",\n" +
                "      \"ii_DBCommunicationProtocol\": \"SSH\",\n" +
                "      \"dbCharset\": \"{{dbCharset}}\",\n" +
                "      \"db2ZosDsnLoadLibrary\": \"{{db2ZosDsnLoadLibrary}}\",\n" +
                "      \"db2ZosBsds\": \"{{db2ZosBsds}}\",\n" +
                "      \"db2AdminDBName\": \"{{db2AdminDBName}}\"\n" +
                "    },\n" +
                "    \"target\": {\n" +
                "      \"dBType\": \"{{target.dBType}}\",\n" +
                "      \"host\": \"{{target.host}}\",\n" +
                "      \"port\": \"{{target.port}}\",\n" +
                "      \"zooKeeperHost\": \"localhost\",\n" +
                "      \"clientPort\": \"2181\",\n" +
                "      \"hiveHost\": \"\",\n" +
                "      \"hivePort\": \"10000\",\n" +
                "      \"hiveNamespace\": \"\",\n" +
                "      \"jmsType\": \"TIBCO\",\n" +
                "      \"jmsFactoryName\": \"QueueConnectionFactory\",\n" +
                "      \"jmsQueueName\": \"\",\n" +
                "      \"serviceName\": \"\",\n" +
                "      \"adminSchemaName\": \"\",\n" +
                "      \"adminSchemaPassword\": \"\",\n" +
                "      \"gcsBucketName\": \"\",\n" +
                "      \"gcsServiceAccountId\": \"\",\n" +
                "      \"gcsP12FileName\": \"\",\n" +
                "      \"kafkaTopic\": \"{{pipelineName}}\",\n" +
                "      \"kafkaTopicPrefix\": \"\",\n" +
                "      \"kafkaKey\": \"objectId\",\n" +
                "      \"kafkaAsync\": \"false\",\n" +
                "      \"kafkaAsyncRecordsCheck\": \"10000\",\n" +
                "      \"kafkaGCRecords\": \"0\",\n" +
                "      \"kafkaSecurityProtocol\": \"PLAINTEXT\",\n" +
                "      \"kafkaSecurityJaasConfig\": \"\",\n" +
                "      \"kafkaSSLCertificateAlias\": \"\",\n" +
                "      \"kafkaSSLCertificateFile\": \"\",\n" +
                "      \"kafkaSSLtruststoreFile\": \"\",\n" +
                "      \"kafkaSSLtruststorePassword\": \"\",\n" +
                "      \"kafkaSchemaRegistryUrl\": \"http://localhost:8081\",\n" +
                "      \"azureEventHubsConnectString\": \"\",\n" +
                "      \"hadoopSecurityAuthentication\": \"simple\",\n" +
                "      \"hbasePrincipal\": \"hbase/_HOST@EXAMPLE.COM\",\n" +
                "      \"cr8KerberosPrincipal\": \"dbsh@EXAMPLE.COM\",\n" +
                "      \"cr8KerberosKeytabFile\": \"\",\n" +
                "      \"kerberosConfigFile\": \"\",\n" +
                "      \"s3BucketName\": \"\",\n" +
                "      \"s3ProfileName\": \"\",\n" +
                "      \"s3ProfilesConfigFileName\": \"\",\n" +
                "      \"hdfsUser\": \"\",\n" +
                "      \"hdfsDirPathToWrite\": \"\",\n" +
                "      \"gbqProjectId\": \"\",\n" +
                "      \"gbqDatasetId\": \"\",\n" +
                "      \"gbqServiceAccountId\": \"\",\n" +
                "      \"gbqP12FileName\": \"\",\n" +
                "      \"neo4JEncryptionLevel\": \"NONE\",\n" +
                "      \"sqlSchema\": \"\",\n" +
                "      \"gigaspacesGroup\": \"xap-15.0.0\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"advancedProperties\": {\n" +
                "    \"logReader\": {\n" +
                "      \"start\": \"true\",\n" +
                "      \"skipAllow\": \"false\",\n" +
                "      \"generateDataDictionaryOnDDL\": \"false\",\n" +
                "      \"generateDataDictionaryMethod\": \"DBSH\",\n" +
                "      \"startScn\": \"\",\n" +
                "      \"endScn\": \"\",\n" +
                "      \"sleepTime\": \"1500\",\n" +
                "      \"oracleRealTime\": \"true\",\n" +
                "      \"sybaseFetchMode\": \"database\",\n" +
                "      \"oracleMaxScnDelta\": \"400\",\n" +
                "      \"oracleParallelThreads\": \"1\",\n" +
                "      \"timeLagThreshold\": \"300\",\n" +
                "      \"oracleSupplementalLogging\": \"ALL\",\n" +
                "      \"createConditionList\": \"true\",\n" +
                "      \"publishUncommitted\": \"true\",\n" +
                "      \"outputFileFormat\": \"JSON\",\n" +
                "      \"onErrorRetries\": \"10\",\n" +
                "      \"mssqlMaxTrxDelta\": \"10000\",\n" +
                "      \"mssqlTempUsageTresholdInKB\": \"1000000\",\n" +
                "      \"secondaryFetch\": \"false\",\n" +
                "      \"standAloneRecovery\": \"false\",\n" +
                "      \"postgresDBMaxTrxDelta\": \"10000\",\n" +
                "      \"mssqlCharsetConversion\": \"false\"\n" +
                "    },\n" +
                "    \"fetcher\": {\n" +
                "      \"start\": \"true\",\n" +
                "      \"mode\": \"{{mode}}\",\n" +
                "      \"startDate\": \"01/01/197000:00:00\",\n" +
                "      \"sleepTime\": \"1500\",\n" +
                "      \"freeSpacePercent\": \"10\",\n" +
                "      \"oracleContinuousMine\": \"false\",\n" +
                "      \"oracleContinuousMineTimeout\": \"1500\",\n" +
                "      \"oracleQueryTimeoutInSeconds\": \"3600\",\n" +
                "      \"collectionsThreshold\": \"10000\",\n" +
                "      \"maxWaitingCycles\": \"10\",\n" +
                "      \"reconnectTimeoutInMinutes\": \"60\",\n" +
                "      \"db2MaxLrsnDelta\": \"0\",\n" +
                "      \"skipAllow\": \"false\",\n" +
                "      \"db2JobCard\": \"\",\n" +
                "      \"onErrorRetries\": \"10\",\n" +
                "      \"db2KeepLogLocalCopy\": \"false\",\n" +
                "      \"splitBigLobs\": \"false\",\n" +
                "      \"collectStatistics\": \"false\",\n" +
                "      \"sshHost\": \"{{source.host}}\",\n" +
                "      \"sshPort\": \"\",\n" +
                "      \"sshPrivateKeyFile\": \"\",\n" +
                "      \"db2OutputFileLocation\": \"/gs/u/gscdcu\",\n" +
                "      \"onErrorSleepTimeInMinutes\": \"5\",\n" +
                "      \"vsamFetchTimeoutMin\": \"5\"\n" +
                "    },\n" +
                "    \"applier\": {\n" +
                "      \"start\": \"true\",\n" +
                "      \"mode\": \"JOURNAL\",\n" +
                "      \"journalType\": \"SINGLE\",\n" +
                "      \"sleepTime\": \"1500\",\n" +
                "      \"skipAllow\": \"false\",\n" +
                "      \"conflictResolution\": \"true\",\n" +
                "      \"elasticSearchAsync\": \"false\",\n" +
                "      \"elasticSearchAsyncBatchSize\": \"1000\",\n" +
                "      \"applyFormat\": \"reducedJSON\",\n" +
                "      \"markLastJournalVersion\": \"false\",\n" +
                "      \"ignoreDDLCommands\": \"ALL\",\n" +
                "      \"hbaseKeyDelimiter\": \"|\",\n" +
                "      \"hbaseDurability\": \"ASYNC_WAL\",\n" +
                "      \"asyncWrite\": \"true\",\n" +
                "      \"publishUnchanged\": \"true\",\n" +
                "      \"kafkaDDLTopic\": \"\",\n" +
                "      \"checkpointBatchSize\": \"1\",\n" +
                "      \"skipMissingRange\": \"false\",\n" +
                "      \"gigaspacesConvertDateToLong\": \"false\"\n" +
                "    },\n" +
                "    \"generic\": {\n" +
                "      \"includeSchemasAndTables\": \"{{tablesList}}\",\n" +
                "      \"excludeSchemasAndTables\": \"NONE\",\n" +
                "      \"includeCommands\": \"INSERT;UPDATE;DELETE\",\n" +
                "      \"excludeCommands\": \"NONE\",\n" +
                "      \"heartbeatStart\": \"false\",\n" +
                "      \"heartbeatSchema\": \"DBSH\",\n" +
                "      \"heartbeatSleepTime\": \"60000\",\n" +
                "      \"convertDataToString\": \"true\",\n" +
                "      \"standAloneMode\": \"false\",\n" +
                "      \"tzOffset\": \"+00:00\",\n" +
                "      \"includeTZ\": \"true\",\n" +
                "      \"dateTimeFormat\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\n" +
                "      \"archiveLag\": \"7\",\n" +
                "      \"fullSyncParallelThreads\": \"1\",\n" +
                "      \"kafkaConnectionTimeOut\": \"30000\",\n" +
                "      \"kafkaProducerConfig\": \"CR8KafkaProducer.cfg\",\n" +
                "      \"fullSyncOnly\": \"false\",\n" +
                "      \"hbaseToHiveTablesMap\": \"true\",\n" +
                "      \"hbaseZookeeperZnodeParent\": \"/hbase\",\n" +
                "      \"hbaseHColumnDescriptorConfig\": \"CR8HbaseHColumnDescriptor.cfg\",\n" +
                "      \"fullSyncTargetObjectExistsAction\": \"drop\",\n" +
                "      \"excludeColumns\": \"NONE\",\n" +
                "      \"fullSyncRetries\": \"10\",\n" +
                "      \"fullSyncSleepTime\": \"1500\",\n" +
                "      \"fullSyncAsyncWrite\": \"false\",\n" +
                "      \"collectCR8Statistics\": \"false\",\n" +
                "      \"includeDDLCommands\": \"NONE\",\n" +
                "      \"gatherSourceTableKeys\": \"false\",\n" +
                "      \"dataCompareParallelThreads\": \"1\",\n" +
                "      \"deltaSyncParallelThreads\": \"4\",\n" +
                "      \"fullSyncTablesInOrder\": \"false\",\n" +
                "      \"unsupportedSymbolsInTableNames\": \"\",\n" +
                "      \"unsupportedSymbolsInColumnNames\": \"\",\n" +
                "      \"allowAddTables\": \"false\",\n" +
                "      \"encryptPassword\": \"true\",\n" +
                "      \"fullSyncPartitionsList\": \"\",\n" +
                "      \"lagLoggingTrxRange\": \"10000\",\n" +
                "      \"userDefinedTargetStructure\": \"false\",\n" +
                "      \"vsamListFile\": \"ConfigurationName_vsamListFile.txt\"\n" +
                "    },\n" +
                "    \"sender\": {\n" +
                "      \"start\": \"false\",\n" +
                "      \"senderIP\": \"\",\n" +
                "      \"senderPort\": \"2060\",\n" +
                "      \"receiverIP\": \"\",\n" +
                "      \"receiverPort\": \"2070\"\n" +
                "    },\n" +
                "    \"receiver\": {\n" +
                "      \"start\": \"false\",\n" +
                "      \"senderIP\": \"\",\n" +
                "      \"senderPort\": \"2060\",\n" +
                "      \"receiverIP\": \"\",\n" +
                "      \"receiverPort\": \"2070\"\n" +
                "    },\n" +
                "    \"cr8viewer\": {\n" +
                "      \"start\": \"false\",\n" +
                "      \"host\": \"\",\n" +
                "      \"port\": \"\",\n" +
                "      \"asyncBatch\": \"true\",\n" +
                "      \"asyncBatchSize\": \"1000\",\n" +
                "      \"sleepTime\": \"1500\",\n" +
                "      \"journalType\": \"MULTIPLE\",\n" +
                "      \"skipAllow\": \"true\",\n" +
                "      \"convertDataToString\": \"false\",\n" +
                "      \"applyFormat\": \"flatJSON\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"hiddenProperties\": {\n" +
                "    \"logReader\": {\n" +
                "      \"host\": \"cr8\",\n" +
                "      \"port\": \"1571\",\n" +
                "      \"serviceName\": \"DBSH\",\n" +
                "      \"schemaName\": \"dbsh\",\n" +
                "      \"schemaPassword\": \"dbsh\",\n" +
                "      \"dictionaryFile\": \"dbshLogMinerDataDictionary\",\n" +
                "      \"genericGenerateDDOnStartup\": \"true\",\n" +
                "      \"cmdbCursorTimeoutMillis\": \"90000\",\n" +
                "      \"uncommitedCommandsThreshold\": \"1000000\",\n" +
                "      \"cmdbTransactionListBatchSize\": \"1000\",\n" +
                "      \"oracleFetchSize\": \"1000\",\n" +
                "      \"skipCMDB\": \"true\",\n" +
                "      \"scnSuddenIncreaseFactor\": \"1000\",\n" +
                "      \"zeroParsedCheckRetry\": \"10\",\n" +
                "      \"manualCheckpoint\": \"false\",\n" +
                "      \"mssqlMarkPublished\": \"false\",\n" +
                "      \"oracleReparseMode\": \"NATIVE\",\n" +
                "      \"cacheTableColumns\": \"true\",\n" +
                "      \"dropThreadCollections\": \"true\",\n" +
                "      \"minimumThreadScnRange\": \"100\",\n" +
                "      \"loggingBatchSize\": \"10000\",\n" +
                "      \"setParserMinimalScnAutomatically\": \"false\"\n" +
                "    },\n" +
                "    \"fetcher\": {\n" +
                "      \"oraContinuousLoggingBatch\": \"5000\",\n" +
                "      \"oraContinuousCmdbUploadBatch\": \"1000\",\n" +
                "      \"oraContinuousFetchUncommitted\": \"false\",\n" +
                "      \"oraContinuousControlTable\": \"\",\n" +
                "      \"oraContinuousControlTableTimeout\": \"10000\",\n" +
                "      \"internalQueueSize\": \"100000\"\n" +
                "    },\n" +
                "    \"applier\": {\n" +
                "      \"batchSize\": \"1\",\n" +
                "      \"ignoreErrors\": \"1918:DROP USER;1531:ALTER DATABASE OPEN;942:DROP TABLE\",\n" +
                "      \"purgeTime\": \"30\",\n" +
                "      \"MongoDBWriteConcern\": \"ACKNOWLEDGED\",\n" +
                "      \"neo4jFailOnUnsupportedLabels\": \"false\",\n" +
                "      \"loggingBatchSize\": \"10000\",\n" +
                "      \"elasticSearchConnectTimeout\": \"60000\",\n" +
                "      \"elasticSearchSocketTimeout\": \"60000\",\n" +
                "      \"elasticMaxRetryTimeout\": \"60000\",\n" +
                "      \"hbaseValidateTable\": \"false\"\n" +
                "    },\n" +
                "    \"generic\": {\n" +
                "      \"privateKeyFile\": \"dbsh_key\",\n" +
                "      \"archiveDirectory\": \"archiveLog\",\n" +
                "      \"parsedDirectory\": \"parsed\",\n" +
                "      \"parsingOutDirectory\": \"parsingOutFile\",\n" +
                "      \"appliedDirectory\": \"applied\",\n" +
                "      \"excludeSchemas\": \"{ORACLE:SYS;SYSTEM;SYSMAN;OUTLN;PERFSTAT;CTXSYS;DMSYS;DVSYS;EXFSYS;LBACSYS;MDDATA;MDSYS;OLAPSYS;ORDDATA;ORDPLUGINS;ORDSYS;OUTLN;SI_INFORMTN_SCHEMA;WMSYS;XDB;TSMSYS;DBSNMP,MSSQL:master;tempdb;model;msdb,DB2ZOS:SYSCAT;SYSIBM;SYSIBMTS;SYSTOOLS}\",\n" +
                "      \"supportedDataTypes\": \"{ORACLE:VARCHAR2;CHAR;NVARCHAR2;NCHAR;DATE;NUMBER;TIMESTAMP;TIMESTAMP(6);TIMESTAMP(3);TIMESTAMP(6) WITH TIME ZONE;TIMESTAMP(6) WITH LOCAL TIME ZONE;CLOB;RAW;LONG;FLOAT,MSSQL:datetime2;geometry;geography;nvarchar;sysname;nchar;varchar;char;tinyint;smallint;int;bigint;real;decimal;numeric;date;float;datetime;money;smallmoney;text;ntext;time,DB2ZOS:VARBIN;VARBINARY;TIMESTZ;INT;INTEGER;BIGINT;SMALLINT;FLOAT;DECFLOAT;REAL;DOUBLE;DECIMAL;GRAPHIC;VARGRAPHIC;VARG;VARCHAR;CHAR;CHARACTER;NUMERIC;DATE;TIME;TIMESTAMP;TIMESTMP,SYBASE:INT;BIGINT;SMALLINT;TINYINT;FLOAT;REAL;DOUBLE;DECIMAL;VARCHAR;CHAR;NUMERIC;DATE;TIME;DATETIME;NCHAR;NVARCHAR;UNICHAR;UNIVARCHAR;MONEY;SMALLMONEY,POSTGRESQL:varchar;bytea;text;char;money;numeric;double;smallint;integer;bigint;real;timestamp;timestamp with time zone;time;time with time zone;geometry;geography;int2;int4;int8;date;bpchar;timestamptz;timetz;bool;float4;float8;json,DB2LUW:VARBIN;VARBINARY;TIMESTZ;INT;INTEGER;BIGINT;SMALLINT;FLOAT;DECFLOAT;REAL;DOUBLE;DECIMAL;GRAPHIC;VARGRAPHIC;VARG;VARCHAR;CHAR;CHARACTER;NUMERIC;DATE;TIME;TIMESTAMP;TIMESTMP;CLOB}\",\n" +
                "      \"syncDataTypes\": \"{ORACLE:VARCHAR2;CHAR;NVARCHAR2;NCHAR;DATE;NUMBER;TIMESTAMP;TIMESTAMP(6);TIMESTAMP(3);TIMESTAMP(6) WITH TIME ZONE;TIMESTAMP(6) WITH LOCAL TIME ZONE;CLOB;RAW;LONG;FLOAT,MSSQL:datetime2;geometry;geography;nvarchar;sysname;nchar;varchar;char;tinyint;smallint;int;bigint;real;decimal;numeric;date;float;datetime;money;smallmoney;text;ntext;time,DB2ZOS:VARBIN;VARBINARY;TIMESTZ;INT;INTEGER;BIGINT;SMALLINT;FLOAT;DECFLOAT;REAL;DOUBLE;DECIMAL;GRAPHIC;VARGRAPHIC;VARG;VARCHAR;CHAR;CHARACTER;NUMERIC;DATE;TIME;TIMESTAMP;TIMESTMP,SYBASE:INT;BIGINT;SMALLINT;TINYINT;FLOAT;REAL;DOUBLE;DECIMAL;VARCHAR;CHAR;NUMERIC;DATE;TIME;DATETIME;NCHAR;NVARCHAR;UNICHAR;UNIVARCHAR;MONEY;SMALLMONEY,POSTGRESQL:varchar;bytea;text;char;money;numeric;double;smallint;integer;bigint;real;timestamp;timestamp with time zone;time;time with time zone;geometry;geography;int2;int4;int8;date;bpchar;timestamptz;timetz;bool;float4;float8;json,DB2LUW:VARBIN;VARBINARY;TIMESTZ;INT;INTEGER;BIGINT;SMALLINT;FLOAT;DECFLOAT;REAL;DOUBLE;DECIMAL;GRAPHIC;VARGRAPHIC;VARG;VARCHAR;CHAR;CHARACTER;NUMERIC;DATE;TIME;TIMESTAMP;TIMESTMP;CLOB}\",\n" +
                "      \"identityFile\": \"dbsh.identity\",\n" +
                "      \"sslConnection\": \"false\",\n" +
                "      \"sslKeyStoreDir\": \"sslKeyStoreDir\",\n" +
                "      \"fullSyncBatchSize\": \"1000\",\n" +
                "      \"fullSyncLoggingBatch\": \"10000\",\n" +
                "      \"CMDBMongoDBWriteConcern\": \"ACKNOWLEDGED\",\n" +
                "      \"fullSyncCreateIndexes\": \"true\",\n" +
                "      \"fullSyncTransactionIsolation\": \"TRANSACTION_SERIALIZABLE\",\n" +
                "      \"fullSyncHbaseDurability\": \"SKIP_WAL\",\n" +
                "      \"fullSyncFetchSize\": \"100\",\n" +
                "      \"storeTransactionMetaDataOnTarget\": \"false\",\n" +
                "      \"kafkaSecurityProtocol\": \"PLAINTEXT\",\n" +
                "      \"kafkaSecurityJaasConfig\": \"\",\n" +
                "      \"kafkaSSLCertificateAlias\": \"\",\n" +
                "      \"kafkaSSLCertificateFile\": \"\",\n" +
                "      \"kafkaSSLtruststoreFile\": \"\",\n" +
                "      \"kafkaSSLtruststorePassword\": \"\",\n" +
                "      \"kafkaBrokerConfig\": \"CR8KafkaBroker.cfg\",\n" +
                "      \"kafkaCommitBatchSize\": \"1\",\n" +
                "      \"kafkaAsync\": \"false\",\n" +
                "      \"kafkaAsyncRecordsCheck\": \"1000\",\n" +
                "      \"kafkaTopicReplicationFactor\": \"3\",\n" +
                "      \"kafkaPollDuration\": \"1000\"\n" +
                "    },\n" +
                "    \"sender\": {\n" +
                "      \"readDir\": \"parsingOutFile\",\n" +
                "      \"readPattern\": \"readyToApply\",\n" +
                "      \"deliverPattern\": \"toSend\",\n" +
                "      \"processingDir\": \"senderProcessingDir\",\n" +
                "      \"sentDir\": \"senderSentDir\",\n" +
                "      \"sleepTime\": \"1500\",\n" +
                "      \"timeout\": \"1500\",\n" +
                "      \"serverMaxContentSize\": \"32768000\",\n" +
                "      \"senderMaxHeapSize\": \"3G\",\n" +
                "      \"convertJsonToXml\": \"false\",\n" +
                "      \"purgeTimeInDays\": \"7\"\n" +
                "    },\n" +
                "    \"receiver\": {\n" +
                "      \"writeDir\": \"receiverWriteDir\",\n" +
                "      \"processingDir\": \"receiverProcessingDir\",\n" +
                "      \"receivedDir\": \"parsingOutFile\",\n" +
                "      \"writePattern\": \"toAccept\",\n" +
                "      \"deliverPattern\": \"readyToApply\",\n" +
                "      \"sleepTime\": \"1500\",\n" +
                "      \"timeout\": \"1500\",\n" +
                "      \"serverMaxContentSize\": \"32768000\",\n" +
                "      \"receiverMaxHeapSize\": \"3G\",\n" +
                "      \"convertXmlToJson\": \"false\",\n" +
                "      \"purgeTimeInDays\": \"7\"\n" +
                "    },\n" +
                "    \"cr8viewer\": {\n" +
                "      \"loggingBatchSize\": \"10000\",\n" +
                "      \"checkpointBatchSize\": \"1\",\n" +
                "      \"purgeTimeInDays\": \"\",\n" +
                "      \"connectTimeout\": \"60000\",\n" +
                "      \"socketTimeout\": \"60000\",\n" +
                "      \"maxRetryTimeout\": \"60000\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
    }
}
