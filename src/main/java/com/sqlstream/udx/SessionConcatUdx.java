package com.sqlstream.udx;

import com.sqlstream.jdbc.StreamingPreparedStatement;
import com.sqlstream.jdbc.StreamingResultSet;
import com.sqlstream.jdbc.StreamingResultSet.RowEvent;
import com.sqlstream.plugin.impl.AbstractBaseUdx;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import java.sql.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class SessionConcatUdx
    extends
    AbstractBaseUdx
{
    public static final Logger tracer =
        Logger.getLogger(SessionConcatUdx.class.getName());

    private int pidx;                       // index to partition (session) id column
    private byte[] partitionBytes = null;
    private int pByteLength = 0;
    private int cidx;                       // index to input char column
    private int ridx;                       // index to result col
    private int maxlen;                     // max length of output column

    long timeout_millis = 0L;
    String fieldSeparator;
    String rowSeparator;

    StreamingPreparedStatement out;
    ResultSetMetaData inputMetadata;

    private long rtime;
    private long last_rtime = 0;

    //TLongObjectHashMap<StraggObject> straggMap;
    LinkedHashMap<Long,StraggObject> straggMap = new LinkedHashMap<>();
    ArrayList<Long> deadSessionList = new ArrayList<>();


    private SessionConcatUdx
            ( Logger tracer
            , ResultSet inputRows
            , String partitionColumn
            , String resultColumnName
            , int timeout_secs
            , String fieldSeparator
            , String rowSeparator
            , PreparedStatement results
            )
        throws SQLException
    {
        super(tracer, inputRows, results);

        timeout_millis = 1000 * timeout_secs;
        this.fieldSeparator = fieldSeparator;
        this.rowSeparator = rowSeparator;
        this.inputMetadata = inputRows.getMetaData();

        pidx = inputRows.findColumn(partitionColumn);
        if (inputRows.getMetaData().getColumnType(pidx) != Types.BIGINT &&
            inputRows.getMetaData().getColumnType(pidx) != Types.INTEGER)
        {
            throw new SQLException(
                "The partition Column "+partitionColumn+" needs to be of type INT/BIGINT");
        } 

        
        int[] colIndexes = colMap.get(resultColumnName);
        if (colIndexes == null) {
            throw new SQLException("The result column, " + resultColumnName
                + ", is missing in the RETURNS clause of the function.");
        }
        
        ridx = colMap.get(resultColumnName)[COL_IDX];
        ParameterMetaData pmd = results.getParameterMetaData();
        int rtype = pmd.getParameterType(ridx);
        maxlen = pmd.getPrecision(ridx);

        if ( rtype != Types.CHAR && rtype != Types.VARCHAR)
        {
            throw new SQLException(
                "The result Column "+resultColumnName+" needs to be of type CHAR/VARCHAR, not "+rtype);
        }


        tracer.info("Inializing SessionConcatUdx: pidx="+pidx+",ridx="+ridx+",timeout_ms="+timeout_millis+", fieldSep='"+fieldSeparator+", rowSep='"+rowSeparator+"'" );
    }

 
    /**
     * Perform a STR_AGG of all input columns except partition and rowtime across a partition/session identified by 'partitionColumn'
     * Place the result into 'resultColumnName'
     * Assume the session has ended 'timeout_secs' seconds after the last row encountered
    * -
     * @param inputRows
     * @param partitionColumn - identifies the partition / session id in the inout data
     * @param resultColumnName - identifies the result column in the output
     * @param timeout_secs   - seconds to wait before closing a session
     * @param fieldSeparator - separates fields in each rowtime
     * @param rowSeparator - separates row values
     * @param results
     * @throws SQLException
     */
    public static void stringAgg
            ( ResultSet inputRows
            , String partitionColumn
            , String resultColumnName
            , int timeout_secs
            , String fieldSeparator
            , String rowSeparator
            , PreparedStatement results
            ) throws SQLException
    {
        SessionConcatUdx udx = new SessionConcatUdx(
            tracer, inputRows, partitionColumn, resultColumnName, 
            timeout_secs, fieldSeparator, rowSeparator, results
            );
            try {
                udx.execute();
            } catch (SQLException sqle) {
                if (!results.isClosed()) {
                    throw sqle;
                }
            }
        }

    /**
     * emit the row that has been built up
     */

    private void emitRows(boolean waitForPeriod) throws SQLException {
        long timeThreshold = (waitForPeriod)?last_rtime - timeout_millis : 0;

        if (tracer.isLoggable(Level.FINE)) tracer.fine("emit rows - "+((waitForPeriod)?"normal":"closing")+" threshold time="+timeThreshold);

        // stream entries
        // filter, allow only expired sessions (unless waitForPeriod is false)
        // sort them into time order
        // emit them 
        // remove emitted sessions from the cache
        
        try {
            straggMap.values()
                   .stream()
                   //.peek(e -> tracer.info("First Peek: session:"+e.getSessionId()+"at time:"+e.getTime()))
                   .filter(e -> e.getTime() < timeThreshold)
                   //.peek(e -> tracer.info("Second Peek after filter: session:"+e.getSessionId()+"at time:"+e.getTime()))
                   .sorted((e1, e2) -> Long.compare(e2.getTime(), e1.getTime()))
                   .forEachOrdered(e -> {
                        long sessionId = e.getSessionId();
                        try {
                            if (tracer.isLoggable(Level.FINEST)) tracer.finest("time="+e.getTime()+",session="+sessionId+",val="+e.getConcatValue() );
                            out.setTimestamp(1, new Timestamp(e.getTime()));
                            out.setLong(2, sessionId);
                            String cValue = e.getConcatValue();

                            // if (cValue.length() > 1048575) {
                            //     // too large for any VARCHAR
                            //     tracer.warning("Truncating output for session_id="+sessionId+" - length > 1048575");
                            //     cvalue = cValue.substring(0,1048575);
                            // }

                            if (cValue.length() > maxlen) {
                                // too large for output VARCHAR
                                tracer.warning("Truncating output for session_id="+sessionId+" - length > "+maxlen);
                                cValue = cValue.substring(0,maxlen);

                            }
                            out.setString(3, cValue);
                            out.executeUpdate();
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                        if (tracer.isLoggable(Level.FINER)) tracer.finer("Emitted session id "+sessionId);
                        // prepare to discard used session from the original map
                        deadSessionList.add(sessionId);
            });
        } catch (RuntimeException re) {
            throw new SQLException(re);
        }

        // finally use deleted list to remove all emitted sessions from the main list
        deadSessionList.stream()
                    .forEach(i -> straggMap.remove(i));

        deadSessionList.clear();
 
    }

    /**
     * concatenate all column values in the row EXCEPT rowtime and the partition index
     */
    private String getConcatenatedValues(StreamingResultSet in, int pidx, ResultSetMetaData rsmd) throws SQLException {
        StringBuffer result = new StringBuffer(maxlen);
        int inputColumnCount = inputMetadata.getColumnCount();
        boolean first = true;

        for (int c = 2; c <= inputColumnCount; c++) {
            if (c != pidx) {
                // do not include the partition column in the concatenation
                String fValue = in.getString(c);

                if (!first) {
                    result.append(fieldSeparator);
                } else {
                    first = false;
                }


                // this should work for BIGINT, VARCHAR and CHAR; will it work for VARBINARY?
                result.append(in.getString(c));

            }
        }

        return result.toString();
    }

    private void execute() throws SQLException {
        StreamingResultSet in = (StreamingResultSet)inputRows;
        out = (StreamingPreparedStatement)results;

        ResultSetMetaData rsmd = results.getMetaData();
        int rowtimeIndex = 1;


        // TODO allow for timeout (if there is a timeout, emit row if any)

        while (!out.isClosed()) {
            RowEvent e = in.nextRowOrRowtime(1000);

            switch (e) {
            case EndOfStream:
                // emit cached rows without waiting for timeout
                emitRows(false);
                out.close();
                return;  // end of input

            case Timeout:
                continue;  

            case NewRow:
                rtime = in.getTimestampInMillis(rowtimeIndex);
                
                if (rtime > last_rtime) {
                    // rowtime has advanced, so we may have new rows to emit
                    emitRows(true);
                    last_rtime = rtime;
                }
                // get the session_id for this row
                long partitionKey = in.getLong(pidx);

                String cValue = getConcatenatedValues(in, pidx, rsmd);

                if (tracer.isLoggable(Level.FINEST)) tracer.finest("Read row at rtime="+rtime+", pv="+partitionKey+", cv="+cValue);
                StraggObject so;

                // TODO - use getRawBytes at least for strings to aggregate

                // is this a session we recognize?
                if (straggMap.containsKey(partitionKey)) {
                    if (tracer.isLoggable(Level.FINEST)) tracer.finest("key found, append");
                    so = straggMap.get(partitionKey);
                    so.append(rtime,cValue);
                } else {
                    if (tracer.isLoggable(Level.FINEST)) tracer.finest("new key");
                    so = new StraggObject(rtime, partitionKey, cValue);
                    straggMap.put(partitionKey, so);
                }

                continue;

            case NewRowtimeBound:
                Timestamp rtbound = in.getRowtimeBound();
                rtime = rtbound.getTime();
                if (tracer.isLoggable(Level.FINEST)) tracer.finest("rowtime bound at:"+rtime);

                // emit any expired sessions in rt order
                emitRows(true);

                continue;
            }
        }
    }

    /** 
     * An object to contain the concatenation (plus the sessionId and the latest rowtime)
     */
    
    public class StraggObject {

        private StringBuffer concatValue;
        private long last_rt_millis;
        private long session_id;

        public StraggObject(long ts_millis, long session_id, String initialValue) {
            concatValue = new StringBuffer(initialValue);
            last_rt_millis = ts_millis;
            this.session_id = session_id;

            if (tracer.isLoggable(Level.FINEST)) tracer.finest("Create sessionId="+session_id+", ts="+last_rt_millis+", cValue="+initialValue);
        }

        public void append(long ts_millis, String initialValue) {
            concatValue.append(rowSeparator).append(initialValue);
            last_rt_millis = ts_millis;

            if (tracer.isLoggable(Level.FINEST)) tracer.finest("Append sessionId="+session_id+", ts="+last_rt_millis+", cValue="+concatValue.toString());
        }

        public long getTime() {
            return last_rt_millis;
        }

        public long getSessionId() {
            return session_id;
        }

        public String getConcatValue() {
            return concatValue.toString();
        }

    }

}
