package se.bth.libsla.db;

import java.lang.annotation.*;
import java.lang.reflect.Constructor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Representation of a database table.
 * Quite inspired by BasicObject by Eric Druid [https://github.com/edruid/BasicObject]
 * 
 * @author David Sveningsson <david.sveningsson@bth.se>
 */
public abstract class DBObject {
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface Column {
		String value();
	}
	
	static public class Field {
		public String name;
		public Class<?> data_type;
		java.lang.reflect.Field field;

		@SuppressWarnings("hiding")
		public Field(String name, Class<?> data_type, java.lang.reflect.Field field) {
			this.name = name;
			this.data_type = data_type;
			this.field = field;
		}
	}
	
	/* If true, it means that the object has a corresponding row in the
	 * database, otherwise a new row must be inserted if object saved. */
	private boolean _exists = false;
	
	/**
	 * Initializes database queries. Must be called once.
	 * @param db 
	 * @param table Name of the database table with the data.
	 * @param custom_fields Additional fields to pull.
	 * @return A DescriptionTableQuery state object, pass this to other methods.
	 * @throws SQLException
	 */
	protected static <T> DBObjectState initialize(Class<T> cls, DataLayer db, String table) throws Exception {
		DBObjectState query = new DBObjectState();
		
		query.db = db;
		query.table = table;
		query.fields = get_fields(cls, db, table);
		query.ctor = cls.getConstructors();
			
		/* used by all */
		query.all = db.prepareStatement(
			"SELECT " +
				column_query_from_array(query.fields) +
			"FROM " +
			"	`" + table + "` ");
		
		/* used by from_id_int */
		query.by_id = db.prepareStatement(
			"SELECT " +
			column_query_from_array(query.fields) +
			"FROM " +
			"	`" + table + "` " +
			"WHERE " +
			"	`id` = ? " +
			"LIMIT 1");
		
		/* used by persist_int */
		/* TODO use primary key in WHERE clause, not hardcoded column */
		query.persist = db.prepareStatement(
			"UPDATE `" + table + "` SET " +
			column_update_from_array(query.fields) + 
			"WHERE " +
			"	`id` = ?");
		
		return query;
	}
	
	private static <T> List<Field> get_fields(Class<?> cls, DataLayer db, String table) throws SQLException {
		List<String> available_columns = get_columns(db, table);
		List<Field> fields = new ArrayList<Field>();
		
		for ( java.lang.reflect.Field field : cls.getDeclaredFields() ){
			Column column = field.getAnnotation(Column.class);
			
			if ( column == null ){
				continue;
			}
			
			if ( !available_columns.contains(column.value()) ){
				throw new RuntimeException(String.format(
					"Class %s.%s refers to `%s`.`%s` which is not available",
					cls.getName(), field.getName(), table, column.value()
				));
			}
			
			field.setAccessible(true);
			
			fields.add(new Field(column.value(), field.getType(), field));
		}
		
		return fields;
	}
	
	@SuppressWarnings("unused")
	private static List<String> get_primary_key(DataLayer db, String table) throws SQLException{
		/* Based on BasicObject by edruid */
		PreparedStatement query = db.prepareStatement(
			"SELECT " +
			"	`COLUMN_NAME` " +
			"FROM " +
			"	`information_schema`.`key_column_usage` JOIN " +
			"	`information_schema`.`table_constraints` USING (`CONSTRAINT_NAME`, `CONSTRAINT_SCHEMA`, `TABLE_NAME`) " + 
			"WHERE " +
			"	`table_constraints`.`CONSTRAINT_TYPE` = 'PRIMARY KEY' AND " +
			"	`table_constraints`.`CONSTRAINT_SCHEMA` = ? AND " +
			"	`table_constraints`.`TABLE_NAME` = ?"
		);
		
		query.setString(1, db.dbname());
		query.setString(2, table);
		query.execute();
		ResultSet rs = query.getResultSet();
		
		List<String> result = new ArrayList<String>();
		while ( rs.next() ){
			result.add( rs.getString(1) );
		}
		
		return result; 
	}
	
	private static List<String> get_columns(DataLayer db, String table) throws SQLException {
		/* Based on BasicObject by edruid */
		PreparedStatement query = db.prepareStatement(
			"SELECT `COLUMN_NAME`\n" +
			"FROM `information_schema`.`COLUMNS`\n" +
			"WHERE\n" + 
			" `TABLE_SCHEMA` = ? AND\n" +
			" `table_name` = ?"
		);
		
		query.setString(1, db.dbname());
		query.setString(2, table);
		query.execute();
		ResultSet rs = query.getResultSet();
		
		List<String> result = new ArrayList<String>();
		while ( rs.next() ){
			result.add( rs.getString(1) );
		}
		
		return result; 
	}
	
	private static String column_query_from_array(List<Field> fields){
		List<String> tmp = new ArrayList<String>(fields.size());
		for ( Field f : fields ){
			tmp.add(String.format("\t`%s`", f.name));
		}
		
		return array_join(tmp, ",\n") + "\n"; /* append a space after so it doesn't choke
		                               * on the next SQL line, eg: "`foo`FROM"
		                               */
	}
	
	private static String column_update_from_array(List<Field> fields){
		List<String> tmp = new ArrayList<String>(fields.size());
		for ( Field f : fields ){
			/* TODO should blacklist based on primary key, not hardcoded column */
			if ( f.name.equals("id") ){
				continue;
			}
			
			tmp.add(String.format("\t`%s` = ?", f.name));
		}
		
		return array_join(tmp, ",\n") + "\n"; /* append a space after so it doesn't choke
		                               * on the next SQL line, eg: "`foo`FROM"
		                               */
	}
	
	private static List<String> string_map_format(Collection<String> array, String fmt){
		List<String> tmp = new ArrayList<String>(array.size());
		for ( String str : array ){
			tmp.add(String.format(fmt, str));
		}
		return tmp;
	}
	
	/**
	 * Join an array of strings with a delimiter.
	 * E.g join(new String[]{"foo", "bar", "baz"]}, "|") becomes "foo|bar|baz"
	 * 
	 * @param s Input strings
	 * @param delimiter What string to put inbetween elements
	 * @return
	 */
	private static String array_join(Collection<String> s, String delimiter){
		if ( s.isEmpty() ) return "";

	    Iterator<String> iter = s.iterator();
	    StringBuilder buffer = new StringBuilder(iter.next());
	    while (iter.hasNext()){
	    	while (iter.hasNext()) buffer.append(delimiter).append(iter.next());
	    }
	    
        return buffer.toString();
	}
	
	@SuppressWarnings("unchecked")
	private static <T extends DBObject> T instantiate(DBObjectState query, ResultSet rs, Object[] args) throws Exception {
		Constructor<?> ctor = null;
		
		/* try to find a constructor matching args */
		outer:
		for ( Constructor<?> candidate : query.ctor ){
			Class<?>[] p = candidate.getParameterTypes();
			
			/* match length of expected parameters and passed args */
			if ( p.length != args.length ){
				continue;
			}
			
			/* match class types */
			for ( int i = 0; i < p.length; i++ ){
				try {
					p[i].cast(args[i]);
				} catch ( ClassCastException e ){
					continue outer;
				}
			}
			
			ctor = candidate;
			break;
		}
		
		/* no matching constructor found */
		if ( ctor == null ){
			throw new NoSuchMethodException("Could not find a matching constructor");
		}
		
		/* create a new instance of T */
		T item = (T) ctor.newInstance(args);
		
		/* fill in all fields from the database query */
		for ( Field field : query.fields ){
			Object value = rs.getObject(field.name);
			field.field.set(item, value);
		}
		
		/* mark it as existing (in db) */
		item._exists = true;
		
		/* done */
		return item;
	}
	
	/**
	 * Same as from_id_int(query, id, new Object[0])
	 */
	protected static <T extends DBObject> T from_id_int(DBObjectState query, int id) {
		return from_id_int(query, id, new Object[0]);
	}
	
	/**
	 * Get row from ID.
	 * 
	 * Sample usage:
	 * Metric x = from_id_int(Metric.class, query, id);
	 * 
	 * @param <T> Type of the subclass
	 * @param cls Class-object of the subclass
	 * @param query State object from initialize_queries
	 * @param id ID of the row to request.
	 * @return new instance of T, or null if not found.
	 */
	protected static <T extends DBObject> T from_id_int(DBObjectState query, int id, Object[] args) {
		try {
			/* execute query */
			query.by_id.setInt(1, id);
			query.by_id.execute();
			ResultSet rs = query.by_id.getResultSet();
			
			/* no matching row */
			if ( !rs.next() ){
				return null;
			}
			
			return instantiate(query, rs, args);
		} catch ( Exception e ){
			e.printStackTrace();
			return null; 
		}
	}
	
	/**
	 * Same as all(self, new Object[0]);
	 */
	protected static <T extends DBObject> List<T> all(DBObjectState self){
		return all(self, new Object[0]);
	}
	
	protected static <T extends DBObject> List<T> all(DBObjectState self, Object[] args){
		/* allocate list */
		List<T> result = new ArrayList<T>();
		
		try {
			/* execute query */
			self.all.execute();
			ResultSet rs = self.all.getResultSet();
			
			while ( rs.next() ){
				T item = instantiate(self, rs, args);
				result.add(item);
			}
		} catch ( Exception e ){
			e.printStackTrace(); 
		}
		
		return result;
	}
	
	/**
	 * Same as selection(self, criteria, new Object[0]);
	 */
	protected static <T extends DBObject> List<T> selection(DBObjectState self, Map<String, Object> criteria){
		return selection(self, criteria, new Object[0]);
	}
	
	/**
	 * Get all rows matching criteria.
	 * 
	 * Sample usage:
	 * Map<String, Object> criteria = new Hashtable<String, Object>();
	 * criteria.put("domain_id", new Integer(domain.id()));
	 * List<Metric> x = all(criteria);
	 * 
	 * Criteria
	 * --------
	 * 
	 * By default all entries in criteria is used in the WHERE clause. Special
	 * keywords can be used to further refine the query. Keywords always begin
	 * with the @-sign.
	 * 
	 * `@limit` INTEGER - Appends a LIMIT clause 
	 * 
	 * @param <T> Type of the subclass.
	 * @param self State object from initialize_queries.
	 * @param criteria Map of which columns to match with.
	 * @return List of T with all matching rows, on errors list will be incomplete.
	 */
	protected static <T extends DBObject> List<T> selection(DBObjectState self, Map<String, Object> criteria, Object[] args){
		List<T> result = new ArrayList<T>();
		
		StringBuilder sql = new StringBuilder("SELECT\n" +
			column_query_from_array(self.fields) +
			"FROM\n" +
			String.format("\t`%s`\n", self.table));
		
		Map<String, Object> where = new Hashtable<String, Object>();
		String limit = null;
		
		for ( Map.Entry<String, Object> entry : criteria.entrySet() ){
			String key = entry.getKey();
			Object value = entry.getValue();
			
			if ( key.startsWith("@") ){
				if (key.equals("@limit") ){
					if ( limit != null ){
						throw new RuntimeException("Got multiple limit-keywords");
					}
					
					if ( !(value instanceof Integer) ){
						throw new RuntimeException("Limit expected numerical value, got " + value.getClass().getName());
					}
					
					limit = "LIMIT " + value.toString();
				} else {
					throw new RuntimeException("Unknown keyword " + key + " passed as criteria");
				}
				
				continue;
			}
			
			where.put(entry.getKey(), entry.getValue());
		}
		
		
		List<String> col = string_map_format(where.keySet(), "\t`%s` = ?");
		String where_sql = array_join(col, " AND \n");
		
		/* append WHERE */
		sql.append("WHERE\n");
		sql.append(where_sql);
		sql.append("\n");
		
		/* append LIMIT */
		if ( limit != null ){
			sql.append(limit);
			sql.append("\n");
		}
		
		try {
			PreparedStatement query = self.db.prepareStatement(sql.toString());
			
			int i = 1;
			for ( Object value: where.values() ){
				query.setObject(i++, value);
			}
			
			query.execute();
			ResultSet rs = query.getResultSet();
			
			while ( rs.next() ){
				T item = instantiate(self, rs, args);
				result.add(item);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("\nWhen executing selection query:\n" + sql);
			System.err.println("Args was:\n" + Arrays.toString(args) + "\n");
			System.err.println("Available constructors:\n" + Arrays.toString(self.ctor) + "\n");
		}
		
		return result;
	}
	
	public DBObject() {
		super();
	}
	
	public abstract int id();

	public void remove() {
		// TODO Auto-generated method stub
	}

	public boolean persist_int(DBObjectState self) {
		StringBuilder sql = new StringBuilder();

		if ( _exists ){
			sql.append("UPDATE ");
		} else {
			sql.append("INSERT INTO ");
		}
		
		sql.append("`" + self.table + "` SET\n");
		sql.append(column_update_from_array(self.fields));
		
		if ( _exists ){
			sql.append("WHERE\n");
			sql.append("\t`id` = ?\n");
		}

		try {
			PreparedStatement query = self.db.prepareStatement(sql.toString());
			
			int i = 1;
			for ( Field f : self.fields ){
				/* TODO should blacklist based on primary key, not hardcoded column */
				if ( f.name.equals("id") ){
					continue;
				}
				
				System.out.println("setting " + f.name + " to " + f.field.get(this));
				query.setObject(i++, f.field.get(this));
			}
			
			if ( _exists ){
				query.setInt(i, id());
			}
		
			query.execute();
		
			return true;
		} catch ( Exception e ){
			e.printStackTrace();
			System.err.println("\nWhen executing selection query:\n" + sql);
			return false;
		}
	}

}