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
public class DirHandleEntry extends HandleEntry
{

    public DirHandleEntry(  FSENode node, long handleNo )
    {
        super(node);
        this.handle = handleNo;
    }


    @Override
    public long getHandle()
    {
        return handle;
    }

    @Override
    public void close()
    {
        
    }
    

}
