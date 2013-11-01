package de.dimm.vsm.fsutils;

import de.dimm.vsm.fuse.FuseVSMFS;
import de.dimm.vsm.dokan.DokanVSMFS;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.fsutils.utils.OSValidator;
import de.dimm.vsm.fuse.MacFuseVSMFS;
import de.dimm.vsm.fuse.VfsMacFuseVSMFS;
import de.dimm.vsm.net.StoragePoolWrapper;
import de.dimm.vsm.vfs.IBufferedEventProcessor;
import java.net.InetAddress;

import java.util.Properties;

import org.apache.log4j.Logger;


public class MountVSMFS
{
    static boolean allowWrite = true;
    static RemoteStoragePoolHandler sp_handler;
    
    public static IVSMFS mount_vsmfs(InetAddress host, int port, StoragePoolWrapper pool, IBufferedEventProcessor processor, String drive, boolean use_fuse, boolean rdwr)
    {
        // -v VSMFS -m R
        checkJavaVersion();

        // BEIM MAC: sysctl -w macfuse.tunables.admin_group=12  (12 == everyone)
        // ODER sysctl -w vfs.generic.fuse4x.tunables.admin_group=80
        try
        {
            String[] fuse_args  = { "-s", "-r", "-o", "volname=VSMFileSystem"};
            if (OSValidator.isMac())
            {
                if (allowWrite) 
                {
                    String[] _fuse_args = { "-s", "-o", "allow_other", "-o", "volname=VSMFileSystem" };
                    fuse_args = _fuse_args;
                }
                else
                {
                    String[] _fuse_args = { "-s", "-r", "-o", "allow_other", "-o", "volname=VSMFileSystem" };
                    fuse_args = _fuse_args;
                }
            }
//            String[] fuse_args = { "-s", "-o", "allow_other" , "-o", "volname=VSMFileSystem", "-o", "debug",};
//            String[] fuse_args = { "-s", "-o", "volname=VSMFileSystem", "-o", "debug", "-o", "allow_root"};
//            String[] fuse_args = {"-d","-s", "-o", "allow_other"};
            
            for (int i = 0; i < fuse_args.length; i++)
            {
                System.out.println(fuse_args[i]);
            }

            Logger log = VSMFSLogger.getLog();

            if (sp_handler != null)
                sp_handler.disconnect();

            sp_handler = new RemoteStoragePoolHandler( pool/*, timestamp, subPath, user*/ );
            sp_handler.setBuffered(true);
            sp_handler.setWriteBufferBlockSize(10*1024*1024);
            // TODO: from Server
            sp_handler.setReadBufferBlockSize(1024*1024);
            sp_handler.connect(host, port);
            
            processor.setRemoteStoragePoolHandler(sp_handler);


            final IVSMFS filesystem;
            // FUSE ?
            if (use_fuse)
            {
                if (OSValidator.isMac())
                    filesystem = new VfsMacFuseVSMFS(sp_handler, drive, log, fuse_args, rdwr );
//                    filesystem = new MacFuseVSMFS(sp_handler, drive, log, fuse_args, rdwr );
                else
                    filesystem = new FuseVSMFS(sp_handler, drive, log, fuse_args );
            }
            else
            {                
                filesystem = new DokanVSMFS(sp_handler, processor, drive, log);
            }

            Thread thr = new Thread(new Runnable() {

                @Override
                public void run()
                {
                    filesystem.mount();
                }
            }, "MountVSMFS");
            
            thr.start();          

            return filesystem;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }        
        return null;
    }


    public static void checkJavaVersion()
    {
        Properties sProp = java.lang.System.getProperties();
        String sVersion = sProp.getProperty("java.version");
        sVersion = sVersion.substring(0, 3);
        Float f = Float.valueOf(sVersion);
        if (f.floatValue() < (float) 1.6)
        {
            System.out.println("Java version must be 1.6 or newer");
            System.exit(-1);
        }
    }
}
