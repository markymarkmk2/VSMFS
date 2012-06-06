package de.dimm.vsm.fsutils;

import de.dimm.vsm.fuse.FuseVSMFS;
import de.dimm.vsm.dokan.DokanVSMFS;

public class ShutdownHook extends Thread
{

    protected String mountPoint;
    boolean is_fuse;

    public ShutdownHook( String mountPoint, boolean is_fuse )
    {
        this.mountPoint = mountPoint;
        this.is_fuse = is_fuse;
    }

    @Override
    public void run()
    {                
        if (!is_fuse)
        {
            DokanVSMFS.unmount(mountPoint);
        }
        else
        {
            FuseVSMFS.unmount(mountPoint);
        }
    }

    public String getMountPoint()
    {
        return mountPoint;
    }
    
}
