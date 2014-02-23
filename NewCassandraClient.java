package main.java;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class NewCassandraClient extends DB{

	private Cluster cluster;
	private Session session;
	private static final String COLUMN_FAMILY = "data";
	private static final String KEYSPACE = "usertable";
	private static final ConsistencyLevel READ_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
	private static final ConsistencyLevel UPDATE_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
	private static final ConsistencyLevel INSERT_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
	private static final ConsistencyLevel DELETE_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
	private static final int OK = 0;
	
	public NewCassandraClient(){
		this.cluster = null;
		this.session = null;
	}
	
	// For testing reasons
	public NewCassandraClient(String hosts){
		this.cluster = Cluster.builder().addContactPoints(hosts).withPort(2222).build();
		this.session = this.cluster.connect();
	}
	
	private String[] getHosts() throws DBException{
		String hosts = getProperties().getProperty("hosts");
	    if (hosts == null){
	      throw new DBException("Required property \"hosts\" missing for CassandraClient");
	    }
	    return hosts.split(",");
	}
	
	public void init() throws DBException {
		String[] hosts = this.getHosts();
		this.cluster = Cluster.builder().addContactPoints(hosts).build();
		this.session = this.cluster.connect();
	}
	
	public void cleanup() throws DBException {
		this.session.shutdown();
		this.cluster.shutdown();
	}
	
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		String query = "SELECT * FROM " + KEYSPACE + "." + COLUMN_FAMILY + " WHERE YCSB_KEY=?;";
		PreparedStatement statement = this.session.prepare(query);
		BoundStatement bStatement = new BoundStatement(statement);
		bStatement.bind(new Object[]{key});
		ResultSet queryResult = this.executePreparedStatement(bStatement, READ_CONSISTENCY_LEVEL);
		// Only one row because of primary key constraint
		for(Row row: queryResult){
			if(fields != null){
				this.copyFields(fields, row, result);
			}
			else{
				this.copyAllFields(row, result);
			}
		}
		return OK;
	}
	
	private void copyFields(Set<String> fields, Row row, HashMap<String, ByteIterator> result){
		for(String field : fields){
			String value = row.getString(field);
			ByteIterator bInterator = new StringByteIterator(value);
			result.put(field, bInterator);
		}
	}
	
	private void copyAllFields(Row row, HashMap<String, ByteIterator> result){
		for(Definition definition: row.getColumnDefinitions()){
			String field = definition.getName();
			String value = row.getString(field);
			ByteIterator bInterator = new StringByteIterator(value);
			result.put(field, bInterator);
		}
	}
	
	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
//		String query = "SELECT * FROM ?.? WHERE YCSB_KEY > ? ORDER BY YCSB_KEY ASC LIMIT ?;";
//		PreparedStatement statement = this.session.prepare(query);
//		BoundStatement bStatement = new BoundStatement(statement);
//		statement.bind(new Object[]{KEYSPACE, COLUMN_FAMILY, startkey, recordcount});
//		ResultSet queryResult = this.executePreparedStatement(bStatement, READ_CONSISTENCY_LEVEL);
//		for (Row row : queryResult) {
//			HashMap<String, ByteIterator> aResult = new HashMap<String, ByteIterator>();
//			for (String field : fields) {
//				String value = row.getString(field);
//				ByteIterator bInterator = new StringByteIterator(value);
//				aResult.put(field, bInterator);
//			}
//			result.add(aResult);
//		}
//		return OK;
		throw new UnsupportedOperationException();
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		BoundStatement statement = this.getUpdateQuery(values, key);
		this.executePreparedStatement(statement, UPDATE_CONSISTENCY_LEVEL);
		return OK;
	}

	private BoundStatement getUpdateQuery(Map<String, ByteIterator> values, String key){
		int amountOfFields = values.size();
		String query = "UPDATE " + KEYSPACE + "." + COLUMN_FAMILY + " SET " + 
			this.getQuestionmarkListForUpdate(amountOfFields, values.keySet()) + " WHERE YCSB_KEY=?;";
		PreparedStatement statement = this.session.prepare(query);
		BoundStatement bStatement = new BoundStatement(statement);
		Object[] bindings = new Object[amountOfFields + 1];
		int counter = 0;
		for(String field : values.keySet()){
			 bindings[counter++] = values.get(field).toString(); 
		}
		bindings[bindings.length-1] = key;
		bStatement.bind(bindings);
		return bStatement;
	}
	
	private String getQuestionmarkListForUpdate(int amount, Set<String> fields){
		String result = "";
		boolean first = true;
		for(String field : fields){
			if(first){
				result += (field + "=?");
				first = false;
			}
			else{
				result += ("," + field + "=?");
			}
		}
		return result;
	}
	
	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		BoundStatement statement = this.getInsertQuery(values, key);
		this.executePreparedStatement(statement, INSERT_CONSISTENCY_LEVEL);
		return OK;
	}

	private BoundStatement getInsertQuery(Map<String, ByteIterator> values, String key){
		values.put("YCSB_KEY", new StringByteIterator(key));
		String questionmarkList = this.getQuestionmarkList(values.size());
		String fieldList = this.getListOfFields(values.keySet());
		String query = "INSERT INTO " + KEYSPACE + "." + COLUMN_FAMILY + 
						" (" + fieldList + ") VALUES (" + questionmarkList + ");";
		Object[] bindings = new Object[values.size()];
		int counter = 0;
		for(String field : values.keySet()){
			bindings[counter++] = values.get(field).toString();
		}
		PreparedStatement statement = this.session.prepare(query);
		BoundStatement bStatement = new BoundStatement(statement);
		bStatement.bind(bindings);
		return bStatement;
	}
	
	private String getListOfFields(Set<String> fields){
		String result = "";
		for(String field: fields){
			if(result.equals(""))
				result += field;
			else
				result += ("," + field);
		}
		return result;
	}
	
	private String getQuestionmarkList(int amount){
		String result = "?";
		for(int i=0; i<amount-1; i++){
			result += ",?";
		}
		return result;
	}
	
	@Override
	public int delete(String table, String key) {
		String query = "DELETE FROM " + KEYSPACE + "." + COLUMN_FAMILY + " WHERE YCSB_KEY=?;";
		PreparedStatement statement = this.session.prepare(query);
		BoundStatement bStatement = new BoundStatement(statement);
		bStatement.bind(new Object[]{key});
		this.executePreparedStatement(bStatement, DELETE_CONSISTENCY_LEVEL);
		return OK; 
	}
	
	private ResultSet executePreparedStatement(BoundStatement statement, ConsistencyLevel consistencylevel){
		try{
			statement.setConsistencyLevel(consistencylevel);
			return this.session.execute(statement);
		} catch(NoHostAvailableException exc){
			throw new RuntimeException("No host in cluster available");
		} catch(QueryExecutionException exc){
			throw new RuntimeException("Cannot offer req. consistency level");
		} catch(QueryValidationException exc){
			throw new RuntimeException("Invalid syntex of query: " + statement.toString());
		}
	}
	
	public static void main(String[] args) throws DBException {
		NewCassandraClient client = new NewCassandraClient("127.0.0.1");
		client.delete(null, "zzz");
		client.cleanup();
	}
	
}