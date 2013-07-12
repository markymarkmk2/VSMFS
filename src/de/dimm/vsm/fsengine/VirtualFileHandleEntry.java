/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsutils.FSENode;
import de.dimm.vsm.fsutils.IVirtualFSFile;
import de.dimm.vsm.fsutils.VirtualRemoteFileHandle;
import java.io.IOException;

/**
 *
 * @author Administrator
 */
public class VirtualFileHandleEntry extends FileHandleEntry
{
    public VirtualFileHandleEntry( VirtualRemoteFileHandle fh,FSENode node, long handle )
    {
        super(fh, node, true);
        this.handle = handle;
    }


    public IVirtualFSFile getFile()
    {
        return ((VirtualRemoteFileHandle)fh).getFile();
    }

    @Override
    public long getHandle()
    {
        return handle;
    }

    @Override
    public void close() throws IOException
    {
        fh.close();
    }            
}