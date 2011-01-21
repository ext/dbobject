Simple SQL ORM for Java
=======================

Not the most efficient as it makes more queries than needed, quite easy to
optimize if needed. I initially write it as a simple abstraction for a single
table, but started reusing it more and more so I decided to upload it here.

Sample
------

	public class Party extends DBObject implements se.bth.libsla.Party {
	        private static DBObjectState query;
	       
	       	private @Column("id") int _id;
		private @Column("sla_id")    @References(SLA.class)  SLA _sla;
		private @Column("role_id")   @References(Role.class) Role _role;
		private @Column("peer_data") @Serializes(Peer.class) Peer _peer;
		private @Column("signed") int _signed;
	
		static public void initialize_queries(DataLayer db) throws Exception {
		       query = initialize(Party.class, db, "party");
		}

		/* ... */
	}

Documentation
-------------

1. First create your own class, represented by a table in the database.
2. Add a static initialization function and call it before creating any
   instances, preferably at the same place where the database connection is
   made.
3. For each column you want represented add the @Column annotation where the
   value is the column name.
   - If you have foreign keys you can add @References to create an instance of
     another class.
   - For serialized data use @Serializes
   - SQL enumeration can either be a String, Integer or int.
4. To store changes use the inherited persist() method. If the object was loaded
   from database it will be updated, otherwise it will be inserted.
5. To query rows use the selection() method with criteria.
