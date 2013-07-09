/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.vfs.IVfsEventProcessor;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public class VirtualFSENode extends FSENode
{

    IVfsEventProcessor processor;

    public VirtualFSENode( RemoteFSElem node, RemoteStoragePoolHandler remoteFSApi, IVfsEventProcessor processor)
    {
        super(node, remoteFSApi);
        this.processor = processor;
    }
    
    @Override
    public FileHandle open_file_handle( boolean create ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        l("open_file_handle");
        if (isStreamPath())
            return null;

        VirtualRemoteFileHandle handle = new VirtualRemoteFileHandle( new VirtualFSFile(processor, fseNode),  fseNode);
        
        VirtualFsFilemanager.addFile(fseNode.getPath(), handle.getFile());
        
        return handle;
    }    
        
}
