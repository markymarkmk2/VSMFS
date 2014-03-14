/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm;

import de.dimm.vsm.net.RemoteCallFactory;
import de.dimm.vsm.net.interfaces.StoragePoolHandlerInterface;
import de.dimm.vsm.net.interfaces.FSENodeInterface;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

class StoragePoolHandlerApiEntry
{
    StoragePoolHandlerInterface api;
    RemoteCallFactory factory;
}
class FSENodeInterfaceApiEntry
{
    FSENodeInterface api;
    RemoteCallFactory factory;
}


class ServerTicket
{
    InetAddress adress;
    int port;
    StoragePoolHandlerApiEntry poolhandlerEntry;
    FSENodeInterfaceApiEntry nodeInterfaceEntry;

    public ServerTicket( InetAddress adress, int port )
    {
        this.adress = adress;
        this.port = port;
        this.poolhandlerEntry = null;
        this.nodeInterfaceEntry = null;
    }


    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof ServerTicket)
        {
            ServerTicket t = (ServerTicket) obj;
            if (t.adress.equals(adress) && t.port == port)
                return true;

            return false;
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode()
    {
        int hash = adress.hashCode();
        return hash;
    }
}
/**
 *
 * @author Administrator
 */
public class FSServerConnector
{
    public static final String path = "fs";
    public static final String keyStore = "J:\\Develop\\VSM\\Eval\\ProjectEval\\vsmkeystore2.jks";
    public static final String keyPwd = "123456";
    private static int connTimeout = 5000;
    private static int txTimeout = 2*60*1000;

    List<ServerTicket> serverList;

    public static void setConnTimeout( int connTimeout )
    {
        FSServerConnector.connTimeout = connTimeout;
    }

    public static void setTxTimeout( int txTimeout )
    {
        FSServerConnector.txTimeout = txTimeout;
    }

    
    public FSServerConnector()
    {
        serverList = new ArrayList<ServerTicket>();
    }

    public StoragePoolHandlerInterface connect( InetAddress adress, int port, boolean ssl, boolean tcp )
    {
        ServerTicket tk = new ServerTicket(adress, port);
        int idx = serverList.indexOf(tk);
        try
        {
            if (idx >= 0)
            {
                tk = serverList.remove(idx);
                tk.nodeInterfaceEntry.factory.close();
                tk.poolhandlerEntry.factory.close();

                tk.poolhandlerEntry = generate_poolhandler_api(adress, port, ssl, path, keyStore, keyPwd, tcp);
                tk.nodeInterfaceEntry = generate_nodehandler_api(adress, port, ssl, path, keyStore, keyPwd, tcp);
                serverList.add(tk);
                return tk.poolhandlerEntry.api;
            }
            else
            {
                tk.poolhandlerEntry = generate_poolhandler_api(adress, port, ssl, path, keyStore, keyPwd, tcp);
                tk.nodeInterfaceEntry = generate_nodehandler_api(adress, port, ssl, path, keyStore, keyPwd, tcp);
                
                serverList.add(tk);
                return tk.poolhandlerEntry.api;
            }
        }
        catch (Exception e)
        {
            System.out.println("Connect to Server " + adress.toString() + " failed: " + e.toString());
        }
        return null;
    }
    
    public StoragePoolHandlerInterface getStoragePoolHandlerApi( InetAddress adress, int port, boolean ssl, boolean tcp )
    {
        ServerTicket tk = new ServerTicket(adress, port);
        int idx = serverList.indexOf(tk);
        if (idx >= 0)
            return serverList.get(idx).poolhandlerEntry.api;

        return connect( adress, port, ssl, tcp );
    }
    public FSENodeInterface getFSENodeHandlerApi( InetAddress adress, int port, boolean ssl, boolean tcp )
    {
        ServerTicket tk = new ServerTicket(adress, port);
        int idx = serverList.indexOf(tk);
        if (idx >= 0)
            return serverList.get(idx).nodeInterfaceEntry.api;
        connect( adress, port, ssl, tcp );

        tk = new ServerTicket(adress, port);
        idx = serverList.indexOf(tk);
        if (idx >= 0)
            return serverList.get(idx).nodeInterfaceEntry.api;

        return null;
    }

    public void invalidateConnect(InetAddress adress, int port )
    {
        ServerTicket tk = new ServerTicket(adress, port);
        int idx = serverList.indexOf(tk);
        if (idx >= 0)
        {
            tk = serverList.remove(idx);
            //tk.api.close();
        }
    }


    private static StoragePoolHandlerApiEntry generate_poolhandler_api( InetAddress adress, int port, boolean ssl, String path, String keystore, String keypwd, boolean tcp )
    {
        System.setProperty("javax.net.ssl.trustStore", keystore);

        StoragePoolHandlerApiEntry api = new StoragePoolHandlerApiEntry();
        try
        {
            api.factory = new RemoteCallFactory(adress, port, path, ssl, tcp, connTimeout, txTimeout);

            api.api = (StoragePoolHandlerInterface) api.factory.create(StoragePoolHandlerInterface.class);
        }
        catch (MalformedURLException malformedURLException)
        {
            System.out.println("Err: " + malformedURLException.getMessage());
        }
        return api;
    }

    private static FSENodeInterfaceApiEntry generate_nodehandler_api( InetAddress adress, int port, boolean ssl, String path, String keystore, String keypwd, boolean tcp )
    {
        FSENodeInterfaceApiEntry api = new FSENodeInterfaceApiEntry();
        System.setProperty("javax.net.ssl.trustStore", keystore);

        try
        {
            api.factory = new RemoteCallFactory(adress, port, path, ssl, tcp, connTimeout, txTimeout);

            api.api = (FSENodeInterface) api.factory.create(FSENodeInterface.class);
        }
        catch (MalformedURLException malformedURLException)
        {
            System.out.println("Err: " + malformedURLException.getMessage());
        }
        return api;
    }


    public void disconnect( InetAddress adr, int port )
    {
        ServerTicket tk = new ServerTicket(adr, port);
        if (tk != null)
        {
            try
            {
                tk.nodeInterfaceEntry.factory.close();
                tk.poolhandlerEntry.factory.close();
            }
            catch (IOException iOException)
            {
            }
        }
    }

}
