package org.chdb.jdbc;

import org.jspecify.annotations.*;

import java.sql.*;

/**
 * chdb database metadata
 *
 * @author linux_china
 */
public class ChdbDatabaseMetaData implements DatabaseMetaData {
    private static final String[] TABLE_TYPES = new String[]{"DICTIONARY", "LOG TABLE", "MEMORY TABLE", "REMOTE TABLE", "TABLE", "VIEW", "SYSTEM TABLE", "TEMPORARY TABLE"};
    private final ChdbConnection conn;

    public ChdbDatabaseMetaData(ChdbConnection connection) throws SQLException {
        this.conn = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return this.conn.getUrl();
    }

    @Override
    public String getUserName() throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.conn.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "chdb";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "24.5.1.1";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "chdb JDBC Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "0.1.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 1;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "APPLY,ASOF,ATTACH,CLUSTER,DATABASE,DATABASES,DETACH,DICTIONARY,DICTIONARIES,ILIKE,INF,LIMIT,LIVE,KILL,MATERIALIZED,NAN,OFFSET,OPTIMIZE,OUTFILE,POLICY,PREWHERE,PROFILE,QUARTER,QUOTA,RENAME,REPLACE,SAMPLE,SETTINGS,SHOW,TABLES,TIES,TOP,TOTALS,TRUNCATE,USE,WATCH,WEEK";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "abs,acos,acosh,asin,asinh,atan,atan2,atanh,cbrt,cos,cosh,divide,e,erf,erfc,exp,exp10,exp2,gcd,hypot,intDiv,intDivOrZero,intExp10,intExp2,lcm,lgamma,ln,log,log10,log1p,log2,minus,modulo,moduloOrZero,multiply,negate,pi,plus,pow,power,sign,sin,sinh,sqrt,tan,tgamma";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "appendTrailingCharIfAbsent,base64Decode,base64Encode,char_length,CHAR_LENGTH,character_length,CHARACTER_LENGTH,concat,concatAssumeInjective,convertCharset,countMatches,countSubstrings,countSubstringsCaseInsensitive,countSubstringsCaseInsensitiveUTF8,CRC32,CRC32IEEE,CRC64,decodeXMLComponent,empty,encodeXMLComponent,endsWith,extract,extractAll,extractAllGroupsHorizontal,extractAllGroupsVertical,extractTextFromHTML ,format,ilike,isValidUTF8,lcase,leftPad,leftPadUTF8,length,lengthUTF8,like,locate,lower,lowerUTF8,match,mid,multiFuzzyMatchAllIndices,multiFuzzyMatchAny,multiFuzzyMatchAnyIndex,multiMatchAllIndices,multiMatchAny,multiMatchAnyIndex,multiSearchAllPositions,multiSearchAllPositionsUTF8,multiSearchAny,multiSearchFirstIndex,multiSearchFirstPosition,ngramDistance,ngramSearch,normalizedQueryHash,normalizeQuery,notEmpty,notLike,position,positionCaseInsensitive,positionCaseInsensitiveUTF8,positionUTF8,regexpQuoteMeta,repeat,replace,replaceAll,replaceOne,replaceRegexpAll,replaceRegexpOne,reverse,reverseUTF8,rightPad,rightPadUTF8,startsWith,substr,substring,substringUTF8,tokens,toValidUTF8,trim,trimBoth,trimLeft,trimRight,tryBase64Decode,ucase,upper,upperUTF8";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "bar,basename,blockNumber,blockSerializedSize,blockSize,buildId,byteSize,countDigits,currentDatabase,currentProfiles,currentRoles,currentUser,defaultProfiles,defaultRoles,defaultValueOfArgumentType,defaultValueOfTypeName,dumpColumnStructure,enabledProfiles,enabledRoles,errorCodeToName,filesystemAvailable,filesystemCapacity,filesystemFree,finalizeAggregation,formatReadableQuantity,formatReadableSize,formatReadableTimeDelta,FQDN,getMacro,getServerPort,getSetting,getSizeOfEnumType,greatest,hasColumnInTable,hostName,identity,ifNotFinite,ignore,indexHint,initializeAggregation,initialQueryID,isConstant,isDecimalOverflow,isFinite,isInfinite,isNaN,joinGet,least,MACNumToString,MACStringToNum,MACStringToOUI,materialize,modelEvaluate,neighbor,queryID,randomFixedString,randomPrintableASCII,randomString,randomStringUTF8,replicate,rowNumberInAllBlocks,rowNumberInBlock,runningAccumulate,runningConcurrency,runningDifference,runningDifferenceStartingWithFirstValue,shardCount ,shardNum,sleep,sleepEachRow,tcpPort,throwIf,toColumnTypeName,toTypeName,transform,uptime,version,visibleWidth";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "addDays,addHours,addMinutes,addMonths,addQuarters,addSeconds,addWeeks,addYears,date_add,date_diff,date_sub,date_trunc,dateName,formatDateTime,FROM_UNIXTIME,fromModifiedJulianDay,fromModifiedJulianDayOrNull,now,subtractDays,subtractHours,subtractMinutes,subtractMonths,subtractQuarters,subtractSeconds,subtractWeeks,subtractYears,timeSlot,timeSlots,timestamp_add,timestamp_sub,timeZone,timeZoneOf,timeZoneOffset,today,toDayOfMonth,toDayOfWeek,toDayOfYear,toHour,toISOWeek,toISOYear,toMinute,toModifiedJulianDay,toModifiedJulianDayOrNull,toMonday,toMonth,toQuarter,toRelativeDayNum,toRelativeHourNum,toRelativeMinuteNum,toRelativeMonthNum,toRelativeQuarterNum,toRelativeSecondNum,toRelativeWeekNum,toRelativeYearNum,toSecond,toStartOfDay,toStartOfFifteenMinutes,toStartOfFiveMinute,toStartOfHour,toStartOfInterval,toStartOfISOYear,toStartOfMinute,toStartOfMonth,toStartOfQuarter,toStartOfSecond,toStartOfTenMinutes,toStartOfWeek,toStartOfYear,toTime,toTimeZone,toUnixTimestamp,toWeek,toYear,toYearWeek,toYYYYMM,toYYYYMMDD,toYYYYMMDDhhmmss,yesterday";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return this.empty("PROCEDURE_CAT Nullable(String), PROCEDURE_SCHEM Nullable(String), RESERVED1 Nullable(String), RESERVED2 Nullable(String), RESERVED3 Nullable(String), PROCEDURE_NAME String, REMARKS String, PROCEDURE_TYPE Int16, SPECIFIC_NAME String");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return this.empty("PROCEDURE_CAT Nullable(String), PROCEDURE_SCHEM Nullable(String), PROCEDURE_NAME String, COLUMN_NAME String, COLUMN_TYPE Int16, DATA_TYPE Int32, TYPE_NAME String, PRECISION Int32, LENGTH Int32, SCALE Int16, RADIX Int16, NULLABLE Int16, REMARKS String, COLUMN_DEF Nullable(String), SQL_DATA_TYPE Int32, SQL_DATETIME_SUB Int32, CHAR_OCTET_LENGTH Int32, ORDINAL_POSITION Int32, IS_NULLABLE String, SPECIFIC_NAME String");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return this.empty("TABLE_CAT String");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        int len = TABLE_TYPES.length;
        Object[][] rows = new Object[len][];
        for(int i = 0; i < len; ++i) {
            rows[i] = new Object[]{TABLE_TYPES[i]};
        }
        return this.fixed("TABLE_TYPE String", rows);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return this.empty("TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, COLUMN_NAME String, GRANTOR Nullable(String), GRANTEE String, PRIVILEGE String, IS_GRANTABLE Nullable(String)");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return this.empty("TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, GRANTOR Nullable(String), GRANTEE String, PRIVILEGE String, IS_GRANTABLE Nullable(String)");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return this.empty("SCOPE Int16, COLUMN_NAME String, DATA_TYPE Int32, TYPE_NAME String, COLUMN_SIZE Int32, BUFFER_LENGTH Int32, DECIMAL_DIGITS Int16, PSEUDO_COLUMN Int16");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return this.empty("TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, COLUMN_NAME String, KEY_SEQ Int16, PK_NAME String");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return this.empty("PKTABLE_CAT Nullable(String), PKTABLE_SCHEM Nullable(String), PKTABLE_NAME String, PKCOLUMN_NAME String, FKTABLE_CAT Nullable(String), FKTABLE_SCHEM Nullable(String), FKTABLE_NAME String, FKCOLUMN_NAME String, KEY_SEQ Int16, UPDATE_RULE Int16, DELETE_RULE Int16, FK_NAME Nullable(String), PK_NAME Nullable(String), DEFERRABILITY Int16");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return this.getImportedKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return this.empty("PKTABLE_CAT Nullable(String), PKTABLE_SCHEM Nullable(String), PKTABLE_NAME String, PKCOLUMN_NAME String, FKTABLE_CAT Nullable(String), FKTABLE_SCHEM Nullable(String), FKTABLE_NAME String, FKCOLUMN_NAME String, KEY_SEQ Int16, UPDATE_RULE Int16, DELETE_RULE Int16, FK_NAME Nullable(String), PK_NAME Nullable(String), DEFERRABILITY Int16");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return 1003 == type;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return this.empty("TYPE_CAT Nullable(String), TYPE_SCHEM Nullable(String), TYPE_NAME String, CLASS_NAME String, DATA_TYPE Int32, REMARKS String, BASE_TYPE Int16");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return this.empty("TYPE_CAT Nullable(String), TYPE_SCHEM Nullable(String), TYPE_NAME String, SUPERTYPE_CAT Nullable(String), SUPERTYPE_SCHEM Nullable(String), SUPERTYPE_NAME String");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return this.empty("TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, SUPERTABLE_NAME String");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return this.empty("TYPE_CAT Nullable(String), TYPE_SCHEM Nullable(String), TYPE_NAME String, ATTR_NAME String, DATA_TYPE Int32, ATTR_TYPE_NAME String, ATTR_SIZE Int32, DECIMAL_DIGITS Int32, NUM_PREC_RADIX Int32, NULLABLE Int32, REMARKS Nullable(String), ATTR_DEF Nullable(String), SQL_DATA_TYPE Int32, SQL_DATETIME_SUB Int32, CHAR_OCTET_LENGTH Int32, ORDINAL_POSITION Int32, IS_NULLABLE String, SCOPE_CATALOG String, SCOPE_SCHEMA String, SCOPE_TABLE String, SOURCE_DATA_TYPE Int16");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 1;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 24;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 2;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return this.empty("FUNCTION_CAT Nullable(String), FUNCTION_SCHEM Nullable(String), FUNCTION_NAME String,COLUMN_NAME String, COLUMN_TYPE Int16, DATA_TYPE Int32, TYPE_NAME String, PRECISION Int32,LENGTH Int32, SCALE Int16, RADIX Int16, NULLABLE Int16, REMARKS String, CHAR_OCTET_LENGTH Int32,ORDINAL_POSITION Int32, IS_NULLABLE String, SPECIFIC_NAME String");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return this.empty("TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, COLUMN_NAME String, DATA_TYPE Int32, COLUMN_SIZE Int32, DECIMAL_DIGITS Int32, NUM_PREC_RADIX Int32, COLUMN_USAGE String, REMARKS Nullable(String), CHAR_OCTET_LENGTH Int32, IS_NULLABLE String");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws ClassCastException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    protected ResultSet empty(String columns) throws SQLException {
        return this.fixed(columns, null);
    }

    protected ResultSet fixed(String columns, @Nullable Object[][] values) throws SQLException {
        return new ChdbResultSet(this.conn.createStatement(), "", columns, values);
    }

    protected ResultSet query(String sql) throws SQLException {
        return this.conn.createStatement().executeQuery(sql);
    }
}
