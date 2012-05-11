package com.yahoo.ycsb.db;

import java.lang.String;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Enumeration;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Pattern;
import java.io.File;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;


/**
 * Persistit DB implementation.
 */
public class PersistitClient extends DB
{

    public static final String VERBOSE = "persistitdb.verbose";
    public static final String VERBOSE_DEFAULT = "false";

    public static final String SIMULATE_DELAY = "persistitdb.simulatedelay";
    public static final String SIMULATE_DELAY_DEFAULT = "0";

    private Persistit db;

    private Exchange exchange;

    Random random;
    boolean verbose;
    int todelay;

    public PersistitClient()
    {
        random = new Random();
        todelay = 0;
        verbose = false;
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
    public void init() throws DBException
    {
        Properties props = getProperties();
        verbose = Boolean.parseBoolean(props.getProperty(VERBOSE, VERBOSE_DEFAULT));
        todelay = Integer.parseInt(props.getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));

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


        db = new Persistit();
        
        try
        {
            db.initialize(props);
            exchange = new Exchange(db, "benchmark", "tree1", true);
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            return;
        }
    }

    public void cleanup()
    {
        if (db != null)
        {
            try 
            {
                db.flush();
                db.close();
            }
            catch (final Exception e)
            {
                return;
            }
            db = null;
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
            exchange.clear().to(key);
            /* retrieve the data */
            exchange.fetch();
            String foundData = exchange.getValue().getString();
            result.put("data", new StringByteIterator(foundData));
        }
        catch (final Exception e)
        {
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

        try
        {
            exchange.clear().to(startkey);
            int counter = 0; /* keeps track of how many records we have scanned */
            while (exchange.next() && counter <= recordcount)
            {
                /* retrieve the data */
                String foundData = exchange.getValue().getString();
                HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
                String key = "data" + counter;
                tuple.put(key, new StringByteIterator(foundData));
                result.add(tuple);
                counter++;
            }
        }
        catch (final Exception e)
        {
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
            exchange.clear().to(key);
            /* construct the value for this entry in persistit */
            String hash_map_string = values.toString();
            exchange.getValue().clear();
            exchange.getValue().putUTF(hash_map_string);
            /* actually store the data */
            exchange.store();
        }
        catch (final Exception e)
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
            exchange.clear().to(key);
            /* construct the value for this entry in persistit */
            String hash_map_string = values.toString();
            exchange.getValue().clear();
            exchange.getValue().putUTF(hash_map_string);
            /* actually store the data */
            exchange.store();
        }
        catch (final Exception e)
        {
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
            exchange.clear().to(key);
            exchange.remove();
        }
        catch (final Exception e)
        {
            return 1;
        }

        return 0;
    }

}
