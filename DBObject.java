package se.bth.libsla.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.annotation.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Representation of a database table.
 * Quite inspired by BasicObject by Eric Druid [https://github.com/edruid/BasicObject]
 * 
 * Usage:
 * 
 * 1. Create a new class extending DBObject
 * 2. Add some fields to it and mark them using @Column
 * 3. Add a static method to initialize the class, have it call
 *    DBObject.initialize and store the state object it returns.
 * 4. Using from_id() or selection() you can query the database table, and
 *    persist() to store object. New instances are INSERT'ed and existing are
 *    UPDATE'd. 
 * 
 * @author David Sveningsson <david.sveningsson@bth.se>
 */
public abstract class DBObject {
	
	/**
	 * Mark a field as a representation of a table column.
	 * E.g.
	 *     "private @Column(id) int id;"
	 * will put the value of table.id into the variable id.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	protected @interface Column {
		String value();
	}
	
	/**
	 * Mark field as a reference to another DBObject class using @Column as key.
	 * E.g.
	 *     "private @Column(foo_id) @References(Foo.class) Foo foo;"
	 * will create an instance of the class Foo based on the table column
	 * foo_id. Foo must be a subclass of DBObject and have a matching
	 * constructor to select based on foo_id.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	protected @interface References {
		Class<? extends DBObject> value();
	}
	
	/**
	 * Mark field as a reference to another class using Serializable as a middleman.
	 * Table column must have the datatype BLOB.
	 * E.g.
	 * 		"private @Column(data) @Serializes(Foo.class) Foo inst;"
	 * will create an instance of Foo by deserialization of data.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	protected @interface Serializes {
		Class<? extends Serializable> value();
	}
	
	static public class ColumnData {
		/* Field refers to the class members */
		public Field field;
		public String field_name;
		public Class<?> field_datatype;
		
		/* Column refers to the database table */
		public String column_name;
		public String column_datatype;
		public boolean column_nullable;
		public boolean column_primary;
		
		public Class<? extends DBObject> reference;
		public Class<? extends Serializable> serializes;
		
		@SuppressWarnings("hiding")
		public ColumnData(DataLayer db, String table, Field field, Column column) throws SQLException{
			this.field_name = field.getName();
			this.field_datatype = field.getType();
			
			this.column_name = column.value();
			
			this.reference = null;
			this.field = field;
			
			PreparedStatement query = db.prepareStatement(
				"SELECT" +
				"	`DATA_TYPE`,\n" +
				"	`IS_NULLABLE`,\n" +
				"	`COLUMN_KEY`\n" +
				"FROM" +
				"	`information_schema`.`COLUMNS`\n" +
				"WHERE\n" + 
				"	`TABLE_SCHEMA` = ? AND\n" +
				"	`TABLE_NAME` = ? AND\n" +
				"	`COLUMN_NAME` = ?\n" +
				"LIMIT 1"
			);
				
			query.setString(1, db.dbname());
			query.setString(2, table);
			query.setString(3, column_name);
			query.execute();
			ResultSet rs = query.getResultSet();
			
			if ( !rs.next() ){
				throw new SQLException(String.format(
					"`%s`.`%s` does not exist",
					table, column
				));
			}
			
			/**
			 * TIMESTAMP and NULL.
			 * Timestamps handles null values different than other datatypes.
			 * 
			 * If column is declared "NOT NULL", NULL is still permitted and
			 * means the same as CURRENT_TIMESTAMP: "[..] and assigning NULL
			 * assigns the current timestamp" (MySQL Reference 5.0)
			 */
			
			column_datatype = rs.getString(1);
			column_nullable = rs.getBoolean(2) || column_datatype.equals("timestamp"); /* se note above */
			column_primary  = rs.getString(3).equals("PRI");
		}
	}
	
	/* If true, it means that the object has a corresponding row in the
	 * database, otherwise a new row must be inserted if object saved. */
	private boolean _exists = false;
	
	/**
	 * Instantiate using primary key.
	 */
	protected DBObject(DBObjectState self, int id){
		refresh(self, id);
	}
	
	/**
	 * Initializes database queries. Must be called once for each subclass.
	 * @param db 
	 * @param table Name of the database table with the data.
	 * @param custom_fields Additional fields to pull.
	 * @return A DescriptionTableQuery state object, pass this to other methods.
	 * @throws SQLException
	 */
	protected static <T extends DBObject> DBObjectState initialize(Class<T> cls, DataLayer db, String table) throws Exception {
		DBObjectState query = new DBObjectState();
		
		query.db = db;
		query.table = table;
		query.cls = cls;
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
		
		return query;
	}
	
	/**
	 * Lists all fields in the subclass, marked with the @Column annotation.
	 */
	private static <T> List<ColumnData> get_fields(Class<?> cls, DataLayer db, String table) throws SQLException {
		List<String> available_columns = get_columns(db, table);
		List<ColumnData> fields = new ArrayList<ColumnData>();
		
		for ( Field field : cls.getDeclaredFields() ){
			Column column = field.getAnnotation(Column.class);
			
			/* Field did not declare the @Column annotation, skip */
			if ( column == null ){
				continue;
			}
			
			/* Ensure that the database table holds a column with the referenced name */
			if ( !available_columns.contains(column.value()) ){
				throw new RuntimeException(String.format(
					"Class %s.%s refers to `%s`.`%s` which is not available",
					cls.getName(), field.getName(), table, column.value()
				));
			}
			
			/* since this class writes data to the fields, it must be accessible */
			field.setAccessible(true);
			
			/* Create and fill field wrapper */
			ColumnData data = new ColumnData(db, table, field, column);
			
			/* check if a reference (to another DBObject class) was requested */
			References ref = field.getAnnotation(References.class);
			
			if ( ref != null ){
				data.reference = ref.value();
			}
			
			/* check if a serialization was requested */
			Serializes serializes = field.getAnnotation(Serializes.class);
			
			if ( serializes != null ){
				if ( !data.column_datatype.equals("blob") ){
					throw new RuntimeException(String.format(
						"Class %s.%s (serialized) refers to `%s`.`%s` which has datatype `%s', but is required to be `%s' when serializing.",
						cls.getName(), field.getName(), table, column.value(), data.column_datatype, "blob"
					));
				}
				
				data.serializes = serializes.value();
			}
			
			/* store */
			fields.add(data);
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
	
	/**
	 * Get a list of all columns in the selected table.
	 */
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
	
	private static String column_query_from_array(List<ColumnData> fields){
		List<String> tmp = new ArrayList<String>(fields.size());
		for ( ColumnData f : fields ){
			tmp.add(String.format("\t`%s`", f.column_name));
		}
		
		return array_join(tmp, ",\n") + "\n"; /* append a space after so it doesn't choke
		                               * on the next SQL line, eg: "`foo`FROM"
		                               */
	}
	
	private static String column_update_from_array(List<ColumnData> fields){
		List<String> tmp = new ArrayList<String>(fields.size());
		for ( ColumnData f : fields ){
			/* column is primary_key, ignore */
			if ( f.column_primary ){
				continue;
			}
			
			tmp.add(String.format("\t`%s` = ?", f.column_name));
		}
		
		return array_join(tmp, ",\n") + "\n"; /* append a space after so it doesn't choke
		                               * on the next SQL line, eg: "`foo`FROM"
		                               */
	}
	
	@SuppressWarnings("unused")
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
	
	/**
	 * TODO This *should* be private, but as a hack I made it protected so it
	 * would be possible to write a custom query which wasn't possible to write
	 * using the selection api.
	 * 
	 *  @note DO NOT USE OUTSIDE THIS CLASS! CONSIDER IT PRIVATE!
	 */
	@SuppressWarnings("unchecked")
	protected static <T extends DBObject> T instantiate(DBObjectState query, ResultSet rs, Object[] args) throws Exception {
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
			StringBuilder prototype = new StringBuilder();
			prototype.append(query.cls.getName());
			prototype.append('(');
			for ( Object a : args ){
				prototype.append(a.getClass().getName());
				prototype.append(", ");
			}
			prototype.append(')');
			throw new NoSuchMethodException(String.format(
				"Could not find a constructor matching '%s'",
				prototype.toString()
			));
		}
		
		/* create a new instance of T */
		T item = (T) ctor.newInstance(args);
		
		/* fill in all fields from the database query */
		item.update_fields(query, rs);
		
		/* mark it as existing (in db) */
		item._exists = true;
		
		/* done */
		return item;
	}
	
	private void update_fields(DBObjectState self, ResultSet rs) throws Exception {
		for ( ColumnData field : self.fields ){
			Object value = rs.getObject(field.column_name);
			
			if ( field.reference != null ){ /* reference column */
				try {
					Class<? extends DBObject> ref_cls = field.reference;
					Constructor<? extends DBObject> ctor = ref_cls.getConstructor(value.getClass());

					value = ctor.newInstance(value);
				} catch ( Exception e ){
					e.printStackTrace();
					value = null;
				}
			}
			
			if ( field.serializes != null ){ /* serialized column */
				try {
					ByteArrayInputStream bis = new ByteArrayInputStream((byte[]) value);
					ObjectInputStream in = new ObjectInputStream(bis);
					value = in.readObject();
				} catch ( Exception e ){
					e.printStackTrace();
					value = null;
				}
			}
			
			field.field.set(this, value);
		}
	}
	
	/**
	 * Refresh all values (based on primary key) with values from database.
	 */
	public void refresh(DBObjectState self){
		refresh(self, id()); /* TODO hardcoded primary key */
	}
	
	/* TODO hardcoded primary key */
	private void refresh(DBObjectState self, int id){ 
		try {
			/* execute query */
			self.by_id.setInt(1, id);
			self.by_id.execute();
			ResultSet rs = self.by_id.getResultSet();
			
			/* no matching row */
			if ( !rs.next() ){
				throw new RuntimeException("Invalid object referece, primary key not found in database");
			}
			
			update_fields(self, rs);
		} catch ( Exception e ){
			e.printStackTrace();
		}
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
	protected static <T extends DBObject> List<T> selection(DBObjectState self, TupleList criteria){
		return selection(self, criteria, new Object[0]);
	}
	
	/**
	 * Get all rows matching criteria.
	 * 
	 * Sample usage:
	 * TupleList criteria = new TupleList();
	 * criteria.add("domain_id", new Integer(domain.id()));
	 * List<Metric> x = selection(criteria);
	 * 
	 * Criteria
	 * --------
	 * 
	 * By default all entries in criteria is used in the WHERE clause. Special
	 * keywords can be used to further refine the query. Keywords always begin
	 * with the @-sign. By default criteria is glued with AND, use keywords to
	 * change this behaviour.
	 * 
	 * `@or` CRITERIA - Match subcriteria using OR, e.g. WHERE (foo=1 OR bar=2)
	 * `@and` CRITERIA - Match subcriteria using AND, e.g. WHERE (foo=1 AND bar=2)
	 * `@limit` INTEGER - Appends a LIMIT clause 
	 * 
	 * Values
	 * ------
	 * 
	 * Values are by default matched by the database, with the following exceptions (strings):
	 * 
	 * `@null` - Will match against null, e.g. WHERE foo IS NULL
	 * `@not_null` - Will match against not null, e.g. WHERE foo IS NOT NULL
	 * 
	 * @param <T> Type of the subclass.
	 * @param self State object from initialize_queries.
	 * @param criteria Map of which columns to match with.
	 * @return List of T with all matching rows, on errors list will be incomplete.
	 */
	protected static <T extends DBObject> List<T> selection(DBObjectState self, TupleList criteria, Object[] args){
		List<T> result = new ArrayList<T>();
		
		StringBuilder sql = new StringBuilder("SELECT\n" +
			column_query_from_array(self.fields) +
			"FROM\n" +
			String.format("\t`%s`\n", self.table));
		
		String limit[] = {null};
		StringBuilder where = new StringBuilder();
		List<Object> values = selection_build_where(where, criteria, limit, "AND");
		
		/* only add WHERE-clauses if criteria was specified */
		if ( !where.toString().equals("\n") ){
			sql.append("WHERE\n");
			sql.append(where.toString());
		}
		
		/* append LIMIT */
		if ( limit[0] != null ){
			sql.append(limit[0]);
			sql.append("\n");
		}
		
		try {
			PreparedStatement query = self.db.prepareStatement(sql.toString());
			
			int i = 1;
			for ( Object value: values ){
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
	
	private static List<Object> selection_build_where(StringBuilder dst, TupleList criteria, String[] limit, String glue){
		List<String> clause = new ArrayList<String>();
		List<Object> values = new ArrayList<Object>();
		
		for ( Map.Entry<String, Object> entry : criteria.entrySet() ){
			String key = entry.getKey();
			Object value = entry.getValue();
			
			if ( key.startsWith("@") ){
				if ( key.equals("@or") ){
					TupleList inner = (TupleList)value;
					
					StringBuilder sub_clause = new StringBuilder("(\n");
					List<Object> sub_values = selection_build_where(sub_clause, inner, null, "OR");
					sub_clause.append(")");
					
					clause.add(sub_clause.toString());
					values.addAll(sub_values);
				} else if ( key.equals("@and") ){
					TupleList inner = (TupleList)value;
					
					StringBuilder sub_clause = new StringBuilder("(\n");
					List<Object> sub_values = selection_build_where(sub_clause, inner, null, "AND");
					sub_clause.append(")");
					
					clause.add(sub_clause.toString());
					values.addAll(sub_values);
				} else if ( key.equals("@limit") ){
					if ( limit[0] != null ){
						throw new RuntimeException("Got multiple limit-keywords");
					}
					
					if ( !(value instanceof Integer) ){
						throw new RuntimeException("Limit expected numerical value, got " + value.getClass().getName());
					}
					
					limit[0] = "LIMIT " + value.toString();
				} else {
					throw new RuntimeException("Unknown keyword " + key + " passed as criteria");
				}
				
				continue;
			}
			
			if ( value == null || value.equals("@null") ){
				clause.add(String.format("\t`%s` IS NULL", key));
			} else if ( value.equals("@not_null") ){
				clause.add(String.format("\t`%s` IS NOT NULL", key));
			} else {
				clause.add(String.format("\t`%s` = ?", key));
				values.add(value);
			}
		}
		
		
		//parts.addAll(string_map_format(columns.keySet(), "\t`%s` = ?"));
		String where_sql = array_join(clause, " " + glue + " \n");
		
		/* append WHERE */
		dst.append(where_sql);
		dst.append("\n");
		
		return values;
		
	}
	
	public DBObject() {
		super();
	}
	
	public abstract int id();
	public int primary_key(){ return id(); }

	public void remove() {
		// TODO Auto-generated method stub
	}
	
	private Object value_from_column(ColumnData column) throws Exception {
		Object value = null;
		
		try {
			value = column.field.get(this);
			
			/* return null without checking references if the value is unset */
			if ( value == null ){
				return null;
			}
			
			/* for references the primary key is stored */
			if ( column.reference != null ){
				value = ((DBObject)value).primary_key();
			}
			
			/* serialize object if serialization is requested */
			if ( column.serializes != null ){
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream out = new ObjectOutputStream(bos);
				out.writeObject(value);
				out.close();
				
				value = bos.toByteArray();
			}
		} catch ( Exception ei ){
			System.err.println("Exception raised when reading column " + column.column_name + ":");
			throw ei;
		}
		
		return value;
	}

	/**
	 * Store this object in the table. If it is a new object it is INSERT'ed
	 * and if it was queried from the database it will be UPDATE'd.
	 * @param self State object.
	 * @return Whenever successful or not.
	 */
	protected boolean persist(DBObjectState self) {
		PreparedStatement query = null;
		String sql = null;
		
		try {
			sql = persist_query_store(self);
			query = self.db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			int i = 1;
			
			/* Fill all column values */
			for ( ColumnData f : self.fields ){
				/* column is primary_key, ignore */
				if ( f.column_primary ){
					continue;
				}
				
				Object value = value_from_column(f);
				
				if ( value == null && !f.column_nullable ){
					throw new SQLException(String.format(
						"Field %s.%s cannot be null, `%s`.`%s` declared 'NOT NULL'",
						self.cls.getName(), f.field_name, self.table, f.column_name
					));
				}
				
				query.setObject(i++, value);
			}
			
			/* if the object exists add the primary key to the WHERE clause */
			if ( _exists ){
				query.setInt(i++, primary_key());
			}
			
			query.execute();
			
			/* update fields in object if a new object was created */
			if ( !_exists ){
				ResultSet rs = query.getGeneratedKeys();
				rs.next();
				
				/* TODO again, match primary key, not hardcoded */
				refresh(self, rs.getInt(1));
			}
			
			/* mark as existing, since it is definitely saved now */
			_exists = true;
		
			return true;
		} catch ( Exception e ){
			e.printStackTrace();
			System.err.println("\nWhen executing selection query:\n" + sql);
			
			return false;
		}
	}
	
	/**
	 * Create the query used by persist()
	 */
	private String persist_query_store(DBObjectState self){
		StringBuilder dst = new StringBuilder();
		
		if ( _exists ){
			dst.append("UPDATE ");
		} else {
			dst.append("INSERT INTO ");
		}
		
		dst.append("`" + self.table + "` SET\n");
		dst.append(column_update_from_array(self.fields));
		
		if ( _exists ){
			dst.append("WHERE\n");
			dst.append("\t`id` = ?;\n");
		} else {
			dst.append(";\n");
		}
		
		return dst.toString();
	}
}