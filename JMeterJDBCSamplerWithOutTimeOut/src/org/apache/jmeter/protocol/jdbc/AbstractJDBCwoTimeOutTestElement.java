/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jmeter.protocol.jdbc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.map.LRUMap;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.save.CSVSaveService;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
 * A base class for all JDBC test elements handling the basics of a SQL request.
 * 
 * Updated to not take into account TimeOut for Hive or Phoenix drivers
 * 
 */
public abstract class AbstractJDBCwoTimeOutTestElement extends AbstractJDBCTestElement implements TestStateListener{
    private static final long serialVersionUID = 235L;

    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final String COMMA = ","; // $NON-NLS-1$
    private static final char COMMA_CHAR = ',';

    private static final String UNDERSCORE = "_"; // $NON-NLS-1$

    // String used to indicate a null value
    private static final String NULL_MARKER =
        JMeterUtils.getPropDefault("jdbcsampler.nullmarker","]NULL["); // $NON-NLS-1$
    
    private static final int MAX_OPEN_PREPARED_STATEMENTS =
        JMeterUtils.getPropDefault("jdbcsampler.maxopenpreparedstatements", 100); 

    private static final String INOUT = "INOUT"; // $NON-NLS-1$

    private static final String OUT = "OUT"; // $NON-NLS-1$

    // TODO - should the encoding be configurable?
    protected static final String ENCODING = "UTF-8"; // $NON-NLS-1$

    // key: name (lowercase) from java.sql.Types; entry: corresponding int value
    private static final Map<String, Integer> mapJdbcNameToInt;
    // read-only after class init

    static {
        // based on e291. Getting the Name of a JDBC Type from javaalmanac.com
        // http://javaalmanac.com/egs/java.sql/JdbcInt2Str.html
        mapJdbcNameToInt = new HashMap<String, Integer>();

        //Get all fields in java.sql.Types and store the corresponding int values
        final Field[] fields = java.sql.Types.class.getFields();
        for (int i=0; i<fields.length; i++) {
            try {
                final String name = fields[i].getName();
                final Integer value = (Integer)fields[i].get(null);
                mapJdbcNameToInt.put(name.toLowerCase(java.util.Locale.ENGLISH),value);
            } catch (final IllegalAccessException e) {
                throw new RuntimeException(e); // should not happen
            }
        }
    }

    // Query types (used to communicate with GUI)
    // N.B. These must not be changed, as they are used in the JMX files
    static final String SELECT   = "Select Statement"; // $NON-NLS-1$
    static final String UPDATE   = "Update Statement"; // $NON-NLS-1$
    static final String CALLABLE = "Callable Statement"; // $NON-NLS-1$
    static final String PREPARED_SELECT = "Prepared Select Statement"; // $NON-NLS-1$
    static final String PREPARED_UPDATE = "Prepared Update Statement"; // $NON-NLS-1$
    static final String COMMIT   = "Commit"; // $NON-NLS-1$
    static final String ROLLBACK = "Rollback"; // $NON-NLS-1$
    static final String AUTOCOMMIT_FALSE = "AutoCommit(false)"; // $NON-NLS-1$
    static final String AUTOCOMMIT_TRUE  = "AutoCommit(true)"; // $NON-NLS-1$

    private final String query = ""; // $NON-NLS-1$

    private final String dataSource = ""; // $NON-NLS-1$

    private final String queryType = SELECT;
    private final String queryArguments = ""; // $NON-NLS-1$
    private final String queryArgumentsTypes = ""; // $NON-NLS-1$
    private final String variableNames = ""; // $NON-NLS-1$
    private final String resultVariable = ""; // $NON-NLS-1$
    private final String queryTimeout = ""; // $NON-NLS-1$

    /**
     *  Cache of PreparedStatements stored in a per-connection basis. Each entry of this
     *  cache is another Map mapping the statement string to the actual PreparedStatement.
     *  At one time a Connection is only held by one thread
     */
    private static final Map<Connection, Map<String, PreparedStatement>> perConnCache =
            new ConcurrentHashMap<Connection, Map<String, PreparedStatement>>();

    /**
     * Creates a JDBCSampler.
     */
    protected AbstractJDBCwoTimeOutTestElement() {
    }
    
    /**
     * Execute the test element.
     * 
     * @param conn a {@link SampleResult} in case the test should sample; <code>null</code> if only execution is requested
     * @throws UnsupportedOperationException if the user provided incorrect query type 
     */
    @Override
	protected byte[] execute(final Connection conn) throws SQLException, UnsupportedEncodingException, IOException, UnsupportedOperationException {
        log.debug("executing jdbc");
        Statement stmt = null;
        
        try {
            // Based on query return value, get results
            final String _queryType = getQueryType();
            if (SELECT.equals(_queryType)) {
                stmt = conn.createStatement();
                //stmt.setQueryTimeout(getIntegerQueryTimeout());
                ResultSet rs = null;
                try {
                	final String query = getQuery();
                	System.out.println(query);
                    rs = stmt.executeQuery(query);
                    return getStringFromResultSet(rs).getBytes(ENCODING);
                } finally {
                    close(rs);
                }
            } else if (CALLABLE.equals(_queryType)) {
                final CallableStatement cstmt = getCallableStatement(conn);
                final int out[]=setArguments(cstmt);
                // A CallableStatement can return more than 1 ResultSets
                // plus a number of update counts.
                final boolean hasResultSet = cstmt.execute();
                final String sb = resultSetsToString(cstmt,hasResultSet, out);
                return sb.getBytes(ENCODING);
            } else if (UPDATE.equals(_queryType)) {
                stmt = conn.createStatement();
                stmt.setQueryTimeout(getIntegerQueryTimeout());
                stmt.executeUpdate(getQuery());
                final int updateCount = stmt.getUpdateCount();
                final String results = updateCount + " updates";
                return results.getBytes(ENCODING);
            } else if (PREPARED_SELECT.equals(_queryType)) {
                final PreparedStatement pstmt = getPreparedStatement(conn);
                setArguments(pstmt);
            	System.out.println(pstmt);
                ResultSet rs = null;
                try {
                    rs = pstmt.executeQuery();
                    return getStringFromResultSet(rs).getBytes(ENCODING);
                } finally {
                    close(rs);
                }
            } else if (PREPARED_UPDATE.equals(_queryType)) {
                final PreparedStatement pstmt = getPreparedStatement(conn);
                setArguments(pstmt);
                pstmt.executeUpdate();
                final String sb = resultSetsToString(pstmt,false,null);
                return sb.getBytes(ENCODING);
            } else if (ROLLBACK.equals(_queryType)){
                conn.rollback();
                return ROLLBACK.getBytes(ENCODING);
            } else if (COMMIT.equals(_queryType)){
                conn.commit();
                return COMMIT.getBytes(ENCODING);
            } else if (AUTOCOMMIT_FALSE.equals(_queryType)){
                conn.setAutoCommit(false);
                return AUTOCOMMIT_FALSE.getBytes(ENCODING);
            } else if (AUTOCOMMIT_TRUE.equals(_queryType)){
                conn.setAutoCommit(true);
                return AUTOCOMMIT_TRUE.getBytes(ENCODING);
            } else { // User provided incorrect query type
                throw new UnsupportedOperationException("Unexpected query type: "+_queryType);
            }
        } finally {
            close(stmt);
        }
    }

    private String resultSetsToString(final PreparedStatement pstmt, boolean result, final int[] out) throws SQLException, UnsupportedEncodingException {
        final StringBuilder sb = new StringBuilder();
        int updateCount = 0;
        if (!result) {
            updateCount = pstmt.getUpdateCount();
        }
        do {
            if (result) {
                ResultSet rs = null;
                try {
                    rs = pstmt.getResultSet();
                    sb.append(getStringFromResultSet(rs)).append("\n"); // $NON-NLS-1$
                } finally {
                    close(rs);
                }
            } else {
                sb.append(updateCount).append(" updates.\n");
            }
            result = pstmt.getMoreResults();
            if (!result) {
                updateCount = pstmt.getUpdateCount();
            }
        } while (result || (updateCount != -1));
        if (out!=null && pstmt instanceof CallableStatement){
            final ArrayList<Object> outputValues = new ArrayList<Object>();
            final CallableStatement cs = (CallableStatement) pstmt;
            sb.append("Output variables by position:\n");
            for(int i=0; i < out.length; i++){
                if (out[i]!=java.sql.Types.NULL){
                    final Object o = cs.getObject(i+1);
                    outputValues.add(o);
                    sb.append("[");
                    sb.append(i+1);
                    sb.append("] ");
                    sb.append(o);
                    sb.append("\n");
                }
            }
            final String varnames[] = getVariableNames().split(COMMA);
            if(varnames.length > 0) {
            final JMeterVariables jmvars = getThreadContext().getVariables();
                for(int i = 0; i < varnames.length && i < outputValues.size(); i++) {
                    final String name = varnames[i].trim();
                    if (name.length()>0){ // Save the value in the variable if present
                        final Object o = outputValues.get(i);
                        jmvars.put(name, o == null ? null : o.toString());
                    }
                }
            }
        }
        return sb.toString();
    }


    private int[] setArguments(final PreparedStatement pstmt) throws SQLException, IOException {
        if (getQueryArguments().trim().length()==0) {
            return new int[]{};
        }
        final String[] arguments = CSVSaveService.csvSplitString(getQueryArguments(), COMMA_CHAR);
        final String[] argumentsTypes = getQueryArgumentsTypes().split(COMMA);
        if (arguments.length != argumentsTypes.length) {
            throw new SQLException("number of arguments ("+arguments.length+") and number of types ("+argumentsTypes.length+") are not equal");
        }
        final int[] outputs= new int[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            final String argument = arguments[i];
            String argumentType = argumentsTypes[i];
            final String[] arg = argumentType.split(" ");
            String inputOutput="";
            if (arg.length > 1) {
                argumentType = arg[1];
                inputOutput=arg[0];
            }
            final int targetSqlType = getJdbcType(argumentType);
            try {
                if (!OUT.equalsIgnoreCase(inputOutput)){
                    if (argument.equals(NULL_MARKER)){
                        pstmt.setNull(i+1, targetSqlType);
                    } else {
                        pstmt.setObject(i+1, argument, targetSqlType);
                    }
                }
                if (OUT.equalsIgnoreCase(inputOutput)||INOUT.equalsIgnoreCase(inputOutput)) {
                    final CallableStatement cs = (CallableStatement) pstmt;
                    cs.registerOutParameter(i+1, targetSqlType);
                    outputs[i]=targetSqlType;
                } else {
                    outputs[i]=java.sql.Types.NULL; // can't have an output parameter type null
                }
            } catch (final NullPointerException e) { // thrown by Derby JDBC (at least) if there are no "?" markers in statement
                throw new SQLException("Could not set argument no: "+(i+1)+" - missing parameter marker?");
            }
        }
        return outputs;
    }


    private static int getJdbcType(final String jdbcType) throws SQLException {
        Integer entry = mapJdbcNameToInt.get(jdbcType.toLowerCase(java.util.Locale.ENGLISH));
        if (entry == null) {
            try {
                entry = Integer.decode(jdbcType);
            } catch (final NumberFormatException e) {
                throw new SQLException("Invalid data type: "+jdbcType);
            }
        }
        return (entry).intValue();
    }


    private CallableStatement getCallableStatement(final Connection conn) throws SQLException {
        return (CallableStatement) getPreparedStatement(conn,true);

    }
    private PreparedStatement getPreparedStatement(final Connection conn) throws SQLException {
        return getPreparedStatement(conn,false);
    }

    private PreparedStatement getPreparedStatement(final Connection conn, final boolean callable) throws SQLException {
        Map<String, PreparedStatement> preparedStatementMap = perConnCache.get(conn);
        if (null == preparedStatementMap ) {
            @SuppressWarnings("unchecked") // LRUMap is not generic
			final
            Map<String, PreparedStatement> lruMap = new LRUMap(MAX_OPEN_PREPARED_STATEMENTS) {
                private static final long serialVersionUID = 1L;
                @Override
                protected boolean removeLRU(final LinkEntry entry) {
                    final PreparedStatement preparedStatement = (PreparedStatement)entry.getValue();
                    close(preparedStatement);
                    return true;
                }
            };
            preparedStatementMap = Collections.<String, PreparedStatement>synchronizedMap(lruMap);
            // As a connection is held by only one thread, we cannot already have a 
            // preparedStatementMap put by another thread
            perConnCache.put(conn, preparedStatementMap);
        }
        PreparedStatement pstmt = preparedStatementMap.get(getQuery());
        if (null == pstmt) {
            if (callable) {
                pstmt = conn.prepareCall(getQuery());
            } else {
                pstmt = conn.prepareStatement(getQuery());
            }
//            pstmt.setQueryTimeout(getIntegerQueryTimeout());
            // PreparedStatementMap is associated to one connection so 
            //  2 threads cannot use the same PreparedStatement map at the same time
            preparedStatementMap.put(getQuery(), pstmt);
        } else {
            final int timeoutInS = getIntegerQueryTimeout();
            if(pstmt.getQueryTimeout() != timeoutInS) {
//                pstmt.setQueryTimeout(getIntegerQueryTimeout());
            }
        }
        pstmt.clearParameters();
        return pstmt;
    }

    private static void closeAllStatements(final Collection<PreparedStatement> collection) {
        for (final PreparedStatement pstmt : collection) {
            close(pstmt);
        }
    }

    /**
     * Gets a Data object from a ResultSet.
     *
     * @param rs
     *            ResultSet passed in from a database query
     * @return a Data object
     * @throws java.sql.SQLException
     * @throws UnsupportedEncodingException
     */
    private String getStringFromResultSet(final ResultSet rs) throws SQLException, UnsupportedEncodingException {
        final ResultSetMetaData meta = rs.getMetaData();

        final StringBuilder sb = new StringBuilder();

        final int numColumns = meta.getColumnCount();
        for (int i = 1; i <= numColumns; i++) {
            sb.append(meta.getColumnLabel(i));
            if (i==numColumns){
                sb.append('\n');
            } else {
                sb.append('\t');
            }
        }
        

        final JMeterVariables jmvars = getThreadContext().getVariables();
        final String varnames[] = getVariableNames().split(COMMA);
        final String resultVariable = getResultVariable().trim();
        List<Map<String, Object> > results = null;
        if(resultVariable.length() > 0) {
            results = new ArrayList<Map<String,Object> >();
            jmvars.putObject(resultVariable, results);
        }
        int j = 0;
        while (rs.next()) {
            Map<String, Object> row = null;
            j++;
            for (int i = 1; i <= numColumns; i++) {
                Object o = rs.getObject(i);
                if(results != null) {
                    if(row == null) {
                        row = new HashMap<String, Object>(numColumns);
                        results.add(row);
                    }
                    row.put(meta.getColumnLabel(i), o);
                }
                if (o instanceof byte[]) {
                    o = new String((byte[]) o, ENCODING);
                }
                sb.append(o);
                if (i==numColumns){
                    sb.append('\n');
                } else {
                    sb.append('\t');
                }
                if (i <= varnames.length) { // i starts at 1
                    final String name = varnames[i - 1].trim();
                    if (name.length()>0){ // Save the value in the variable if present
                        jmvars.put(name+UNDERSCORE+j, o == null ? null : o.toString());
                    }
                }
            }
        }
        // Remove any additional values from previous sample
        for(int i=0; i < varnames.length; i++){
            final String name = varnames[i].trim();
            if (name.length()>0 && jmvars != null){
                final String varCount = name+"_#"; // $NON-NLS-1$
                // Get the previous count
                final String prevCount = jmvars.get(varCount);
                if (prevCount != null){
                    final int prev = Integer.parseInt(prevCount);
                    for (int n=j+1; n <= prev; n++ ){
                        jmvars.remove(name+UNDERSCORE+n);
                    }
                }
                jmvars.put(varCount, Integer.toString(j)); // save the current count
            }
        }

        return sb.toString();
    }

    public static void close(final Connection c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (final SQLException e) {
            log.warn("Error closing Connection", e);
        }
    }

    public static void close(final Statement s) {
        try {
            if (s != null) {
                s.close();
            }
        } catch (final SQLException e) {
            log.warn("Error closing Statement " + s.toString(), e);
        }
    }

    public static void close(final ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (final SQLException e) {
            log.warn("Error closing ResultSet", e);
        }
    }    
    
    /**
     * Clean cache of PreparedStatements
     */
    private static final void cleanCache() {
        for (final Map<String, PreparedStatement> element : perConnCache.values()) {
            closeAllStatements(element.values());
        }
        perConnCache.clear();
    }

}
