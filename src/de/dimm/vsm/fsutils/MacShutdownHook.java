package de.dimm.vsm.fsutils;

import de.dimm.vsm.fuse.MacFuseVSMFS;
import java.io.IOException;

public class MacShutdownHook extends ShutdownHook
{


    public MacShutdownHook( String mountPoint )
    {
        super( mountPoint, false);
        
    }

    @Override
    public void run()
    {                
        
        try {
            MacFuseVSMFS.unmount(mountPoint);
        } catch (IOException | InterruptedException iOException) {
        }
        
    }
    
}
