/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.fsutils.IVirtualFSFile;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.fsutils.VirtualLocalFSFile;
import de.dimm.vsm.net.RemoteFSElem;

/**
 *
 * @author Administrator
 */
public interface IVfsBuffer
{
    public String getBufferPath();   
    public boolean isFsBufferFree();    
    public boolean isBufferReady();     
    public void init();
    public void removeEntry( IVirtualFSFile ret );
    public IVirtualFSFile createFile( RemoteFSElem fseNode );
    public void shutdown();
    public void idle();
    public void flush();
    public boolean close( VirtualLocalFSFile aThis );

    public void setRemoteStoragePoolHandler( RemoteStoragePoolHandler sp_handler );
   
    public IVirtualFSFile createDelegate(RemoteFSElem fseNode);
    public void checkForFlush( String path );

    
}
