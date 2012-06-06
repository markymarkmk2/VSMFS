/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsutils.FSENode;
import de.dimm.vsm.net.interfaces.FileHandle;

/**
 *
 * @author Administrator
 */
public class FileHandleEntry
{
    FileHandle fh;
    FSENode node;

    public FileHandleEntry( FileHandle fh, FSENode node )
    {
        this.fh = fh;
        this.node = node;
    }
    @Override
    public String toString()
    {
        return node.get_path();
    }

    public FileHandle getFh()
    {
        return fh;
    }

    public FSENode getNode()
    {
        return node;
    }
    
}