/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsutils.FSENode;

/**
 *
 * @author Administrator
 */
public class DirHandleEntry
{
    FSENode node;
    long handle;

    public DirHandleEntry(  FSENode node )
    {
        this.node = node;
    }

    @Override
    public String toString()
    {
        return node.get_path();
    }

    public FSENode getNode()
    {
        return node;
    }

    public long getHandle()
    {
        return handle;
    }

}
