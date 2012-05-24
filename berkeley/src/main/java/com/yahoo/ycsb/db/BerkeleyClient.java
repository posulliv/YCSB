package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Enumeration;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Pattern;
import java.io.File;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.Durability;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Berkeley DB implementation.
 */
public class BerkeleyClient extends DB
{
    public static final String VERBOSE = "berkeleydb.verbose";
    public static final String VERBOSE_DEFAULT = "false";

    public static final String SIMULATE_DELAY = "berkeleydb.simulatedelay";
    public static final String SIMULATE_DELAY_DEFAULT = "0";

    public static final String DEFER_WRITES = "berkeleydb.deferwrites";
    public static final String DEFER_WRITES_DEFAULT = "false";

    public static final String SYNC_POLICY = "berkeleydb.sync";
    public static final String SYNC_POLICY_DEFAULT = "NO_SYNC";

    Environment env = null;
    Database db = null;
    Transaction txn = null;
    Cursor cursor = null;

    Random random;
    boolean verbose;
    String syncPolicy;
    boolean defer_writes;
    int todelay;

    public BerkeleyClient()
    {
        random = new Random();
        todelay = 0;
        verbose = false;
        defer_writes = false;
    }


    void delay()
    {
        if (todelay > 0)
        {
            try
            {
                Thread.sleep((long)random.nextInt(todelay));
            }
            catch (InterruptedException e)
            {
                //do nothing
            }
        }
    }

    /**
     * Initialize any state for this DB.
     */
    public void init()
    {
        Properties props = getProperties();
        verbose = Boolean.parseBoolean(props.getProperty(VERBOSE, VERBOSE_DEFAULT));
        todelay = Integer.parseInt(props.getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));
        defer_writes = Boolean.parseBoolean(props.getProperty(DEFER_WRITES, DEFER_WRITES_DEFAULT));
        syncPolicy = props.getProperty(SYNC_POLICY, SYNC_POLICY_DEFAULT);

        if (verbose)
        {
            System.out.println("***************** properties *****************");
            Properties p=getProperties();
            if (p!=null)
            {
                for (Enumeration e=p.propertyNames(); e.hasMoreElements(); )
                {
                    String k=(String)e.nextElement();
                    System.out.println("\""+k+"\"=\""+p.getProperty(k)+"\"");
                }
            }
            System.out.println("**********************************************");
        }

        try 
        {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setCachePercent(80);
            envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000000000");
            /* make configurable */
            if (true) 
            {
                envConfig.setTransactional(true);
            }
            env = new Environment(new File("/tmp/berkeley"), envConfig);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            /* make configurable */
            if (true) 
            {
                dbConfig.setTransactional(true);
            }
            if (defer_writes)
            {
                dbConfig.setDeferredWrite(true);
            }
            db = env.openDatabase(txn,
                                  "benchDatabase",
                                  dbConfig);
            cursor = db.openCursor(null, null);
        }
        catch (DatabaseException dbe)
        {
            return;
        }
    }

    public void cleanup()
    {
        try
        {
            if (cursor != null)
            {
                cursor.close();
            }
            if (db != null)
            {
                db.getEnvironment().sync();
                db.close();
            }
            if (env != null)
            {
                env.close();
            }
        }
        catch (DatabaseException dbe)
        {
            return;
        }
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {
        delay();

        if (verbose)
        {
            System.out.print("READ " + table + " " + key + " [ ");
            if (fields != null)
            {
                for (String f : fields)
                {
                    System.out.print(f + " ");
                }
            }
            else
            {
                System.out.print("<all fields>");
            }
            System.out.println("]");
        }

        try
        {
            Durability dur = createDurMode();
            TransactionConfig tc = new TransactionConfig();
            tc.setDurability(dur);
            txn = env.beginTransaction(null, tc); 
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theValue = new DatabaseEntry();
            /* retrieve the data */
            if (db.get(txn, theKey, theValue, LockMode.DEFAULT) == OperationStatus.SUCCESS)
            {
                /* re-create string from value */
                byte[] retData = theValue.getData();
                String foundData = new String(retData, "UTF-8");
                result.put("data", new StringByteIterator(foundData));
                txn.commit();
            }
            else
            {
                txn.commit();
                /* the data was not found */
                return 1;
            }
        }
        catch (Exception e)
        {
            if (txn != null)
            {
                txn.abort();
                txn = null;
            }
            return 1;
        }

        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result)
    {
        delay();

        if (verbose)
        {
            System.out.print("SCAN " + table + " " + startkey + " " + recordcount + " [ ");
            if (fields != null)
            {
                for (String f : fields)
                {
                    System.out.print(f + " ");
                }
            }
            else
            {
                System.out.print("<all fields>");
            }
            System.out.println("]");
        }

        Durability dur = null;
        try {
            dur = createDurMode();
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        TransactionConfig tc = new TransactionConfig();
        tc.setDurability(dur);
        txn = env.beginTransaction(null, tc); 
        Cursor localCursor = null;

        try
        {
            localCursor = db.openCursor(txn, null);
            DatabaseEntry theKey = new DatabaseEntry(startkey.getBytes("UTF-8"));
            DatabaseEntry theValue = new DatabaseEntry();
            int counter = 0; /* keeps track of how many records we have scanned */
            while (localCursor.getNext(theKey, theValue, LockMode.DEFAULT) == OperationStatus.SUCCESS &&
                   counter <= recordcount)
            {
                /* re-create string from value */
                byte[] retData = theValue.getData();
                String foundData = new String(retData, "UTF-8");
                HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
                String key = "data" + counter;
                tuple.put(key, new StringByteIterator(foundData));
                result.add(tuple);
                counter++;
            }
            localCursor.close();
            txn.commit();
        }
        catch (Exception e)
        {
            if (localCursor != null)
            {
                localCursor.close(); /* must close cursor */
            }
            if (txn != null)
            {
                txn.abort();
                txn = null;
            }
            return 1;
        }

        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String,ByteIterator> values)
    {
        delay();

        if (verbose)
        {
            System.out.print("UPDATE " + table + " " + key + " [ ");
            if (values != null)
            {
                for (String k : values.keySet())
                {
                    System.out.print(k + "=" + values.get(k) + " ");
                }
            }
            System.out.println("]");
        }

        try
        {
            Durability dur = createDurMode();
            TransactionConfig tc = new TransactionConfig();
            tc.setDurability(dur);
            txn = env.beginTransaction(null, tc); 
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            /* construct the value for this entry in BerkeleyDB */
            String hash_map_string = values.toString();
            DatabaseEntry theValue = new DatabaseEntry(hash_map_string.getBytes("UTF-8"));
            try {
                /* actually insert the data */
                db.put(txn, theKey, theValue);
                txn.commit();
            } catch (Exception e) {
                if (txn != null) {
                    txn.abort();
                    txn = null;
                }
                return 1;
            }
        }
        catch (Exception e)
        {
            return 1;
        }

        return 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
        delay();

        if (verbose)
        {
            System.out.print("INSERT " + table + " " + key + " [ ");
            if (values != null)
            {
                for (String k : values.keySet())
                {
                    System.out.print(k + "=" + values.get(k) + " ");
                }
            }
            System.out.println("]");
        }

        try
        {
            Durability dur = createDurMode();
            TransactionConfig tc = new TransactionConfig();
            tc.setDurability(dur);
            txn = env.beginTransaction(null, tc); 
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            /* construct the value for this entry in BerkeleyDB */
            String hash_map_string = values.toString();
            DatabaseEntry theValue = new DatabaseEntry(hash_map_string.getBytes("UTF-8"));
            try {
                /* actually insert the data */
                db.put(txn, theKey, theValue);
                txn.commit();
            } catch (Exception e) {
                if (txn != null) {
                    txn.abort();
                    txn = null;
                }
                e.printStackTrace();
                return 1;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return 1;
        }

        return 0;
    }


    @Override
    public int delete(String table, String key)
    {
        delay();

        if (verbose)
        {
            System.out.println("DELETE " + table + " " + key);
        }

        try
        {
            Durability dur = createDurMode();
            TransactionConfig tc = new TransactionConfig();
            tc.setDurability(dur);
            txn = env.beginTransaction(null, tc); 
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            try {
                /* actually remove the data */
                db.delete(txn, theKey);
                txn.commit();
            } catch (Exception e) {
                if (txn != null) {
                    txn.abort();
                    txn = null;
                }
                return 1;
            }
        }
        catch (Exception e)
        {
            return 1;
        }

        return 0;
    }

    private Durability createDurMode() throws Exception
    {
        Durability dur = null;
        if (syncPolicy.equals("NO_SYNC"))
        {
            dur = new Durability(Durability.SyncPolicy.NO_SYNC, null, null);
        } 
        else if (syncPolicy.equals("SYNC"))
        {
            dur = new Durability(Durability.SyncPolicy.SYNC, null, null);
        } 
        else if (syncPolicy.equals("WRITE_NO_SYNC"))
        {
            dur = new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, null, null);
        }
        else
        {
            throw new Exception("Invalid Sync mode specified.");
        }
        return dur;
    }

}
