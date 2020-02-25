# sessionagg

This project provides the code for a SQLstream UDX which executes a kind of sessionized version of STRING_AGG.

Given a stream including a session_id (BIGINT) and a string cvalue (VARCHAR) the output will consist of the session_id and a list of all cvalue entries that were identified for that session_id.

Session values will be emitted as soon as the session times out - when (current rowtime) > (last session rowtime) + (timeout_milliseconds).

This operation is a combination of sliding and tumbling window behaviour:

* tumbling, because for every session_id, just one row is emitted (unless there is a long gap in the middle of the session)
* sliding, because rows for each session are emitted as and when each session times out

Example of use can be seen in `test/test.sql` and example of data in `test/test.csv`

## Notes

* A WARNING is logged to the trace file if the output would overflow the target column - and the data is truncated

## Limitations

* It is assumed that the session_id is a BIGINT rather than a VARCHAR. 
* The session_id must be a single column
* the cValue must be a single column (that can easily be arranged upstream of this function)
* The maximum length of the string aggregation is limited to the maximum size of a VARCHAR which is 1048575
* There is no attempt to check for very large cached sessions; if insufficient heap is allocated, out of memory errors could be reported

## Compromises

* Currently this treats the input character string as a `String` and the aggregation as a `StringBuffer`; this could cause excessive garbage collection. Next steps will include optimization.

# Change history

* initial version
* add separator parameter
* check the length of the output string against the metadata precision/length of the output column

