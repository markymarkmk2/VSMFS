/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsutils.FSENode;
import de.dimm.vsm.net.interfaces.FileHandle;
import java.io.IOException;

/**
 *
 * @author Administrator
 */
public class FileHandleEntry extends HandleEntry
{
    FileHandle fh;
     boolean forWrite;
    

    public FileHandleEntry( FileHandle fh, FSENode node, boolean forWrite )
    {
        super(node);
        this.fh = fh;
        this.forWrite = forWrite;
    }


    public FileHandle getFh()
    {
        return fh;
    }

    @Override
    public long getHandle()
    {
        return node.getNode().getFileHandle();
    }

    @Override
    public void close() throws IOException
    {
        fh.close();
    }

    public boolean isForWrite()
    {
        return forWrite;
    }
    
    
    

    
}